# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # ERD Generator
# MAGIC
# MAGIC **Draws an entity-relationship diagram straight from a pipeline YAML config.**
# MAGIC
# MAGIC Point it at any config in this repo, run it, and you get a Mermaid diagram
# MAGIC plus a CSV relationship list. Because it reads the config the pipeline
# MAGIC actually runs on, the diagram can't drift the way a hand-drawn one does -
# MAGIC re-run it after a config change and the picture is current again.
# MAGIC
# MAGIC ## Quick start
# MAGIC
# MAGIC Set `config_path`, press Run All. Three common examples:
# MAGIC
# MAGIC | I want to see... | config_path | entity_filter |
# MAGIC | --- | --- | --- |
# MAGIC | The Salesforce star schema | `.../gold/config/gold_nexus_config.yaml` | _(blank)_ |
# MAGIC | How accounts and contacts are modelled in the vault | `.../silver/config/vault_config_nexus.yaml` | `account\|contact` |
# MAGIC | Just the vault skeleton, no satellite noise | `.../silver/config/vault_config_nexus.yaml` | `_hub_\|_link_` |
# MAGIC
# MAGIC > **Start narrow.** A whole vault is ~78 boxes and renders as a hairball.
# MAGIC > `entity_filter` is the difference between a diagram people read and one
# MAGIC > they squint at.
# MAGIC
# MAGIC ## What it understands
# MAGIC
# MAGIC The config type is detected automatically from its top-level keys - you
# MAGIC don't have to tell it which layer you're pointing at.
# MAGIC
# MAGIC | Layer | Recognised by | Boxes on the diagram | Lines between them |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Bronze (`extract`) | `objects:` | one per source object | none by default - bronze tables have no declared relationships |
# MAGIC | Silver vault | `hubs:` | hubs, links, satellites | hub → satellite, hub → link |
# MAGIC | Silver business sat | `satellites:` | vault satellite → business satellite | derives |
# MAGIC | Gold | `tables:` | dimensions and facts | fact → dimension, via `foreign_keys` |
# MAGIC
# MAGIC ## Parameters
# MAGIC
# MAGIC | Parameter | Default | What it does |
# MAGIC | --- | --- | --- |
# MAGIC | `config_path` | _(required)_ | The YAML config to diagram |
# MAGIC | `output_dir` | _(blank)_ | Where to save `.mmd` and `.csv`. Blank = print only |
# MAGIC | `entity_filter` | _(blank)_ | Regex. Keeps only matching boxes. Lines are kept only when both ends survive |
# MAGIC | `max_attributes` | `12` | Columns shown per box. Extras collapse to one summary row. `0` shows all |
# MAGIC | `vault_config_path` | _(blank)_ | **Gold only.** Lets fact → dimension lines resolve exactly instead of by naming convention |
# MAGIC | `infer_relationships` | `false` | **Bronze only.** Guesses links from `<Object>Id` column names. Drawn dotted, because they are guesses |
# MAGIC
# MAGIC ## Where the output goes
# MAGIC
# MAGIC * **`<config>.mmd`** - paste into <https://mermaid.live>, a Markdown file,
# MAGIC   Confluence, or Miro's Mermaid plugin
# MAGIC * **`<config>_edges.csv`** - `from,to,label,cardinality`, for Miro's CSV
# MAGIC   import or any graph tool
# MAGIC * The diagram also renders inline at the bottom of this notebook
# MAGIC
# MAGIC ## Also runs outside Databricks
# MAGIC
# MAGIC ```bash
# MAGIC python3 generate_erd.py <config_path> [output_dir]
# MAGIC ENTITY_FILTER="account|contact" python3 generate_erd.py <config_path> ./diagrams
# MAGIC ```
# MAGIC
# MAGIC ## One thing to be clear about when sharing
# MAGIC
# MAGIC This diagrams the **config**, not the deployed tables. That is the point -
# MAGIC it shows the intended design and cannot go stale. It is not a substitute
# MAGIC for checking what is actually in the catalog.

# COMMAND ----------

# DBTITLE 1,Parameters
import csv
import os
import re
import sys

import yaml

# Works two ways: as a Databricks notebook (widgets) or a plain script
# (arguments + environment variables). The try/except picks whichever applies.
try:
    dbutils.widgets.text("config_path", "", "1. Config Path")
    dbutils.widgets.text("output_dir", "", "2. Output Dir (blank = print only)")
    dbutils.widgets.text("entity_filter", "", "3. Entity Filter (regex, blank = all)")
    dbutils.widgets.text("max_attributes", "12", "4. Max Columns Per Box (0 = all)")
    dbutils.widgets.text("vault_config_path", "", "5. Vault Config (gold only)")
    dbutils.widgets.dropdown(
        "infer_relationships", "false", ["true", "false"], "6. Infer FKs (bronze only)"
    )

    CONFIG_PATH = dbutils.widgets.get("config_path")
    OUTPUT_DIR = dbutils.widgets.get("output_dir")
    ENTITY_FILTER = dbutils.widgets.get("entity_filter")
    MAX_ATTRS = int(dbutils.widgets.get("max_attributes") or 12)
    VAULT_CONFIG_PATH = dbutils.widgets.get("vault_config_path")
    INFER = dbutils.widgets.get("infer_relationships") == "true"
except NameError:
    CONFIG_PATH = sys.argv[1] if len(sys.argv) > 1 else ""
    OUTPUT_DIR = sys.argv[2] if len(sys.argv) > 2 else ""
    ENTITY_FILTER = os.environ.get("ENTITY_FILTER", "")
    MAX_ATTRS = int(os.environ.get("MAX_ATTRS", "12"))
    VAULT_CONFIG_PATH = os.environ.get("VAULT_CONFIG", "")
    INFER = os.environ.get("INFER", "") == "true"

if not CONFIG_PATH:
    raise ValueError("config_path is required.")

# COMMAND ----------

# DBTITLE 1,Step 1 - small helpers
# Mermaid is picky about characters, so everything that becomes a box name or a
# column name gets cleaned first. These four helpers are the whole of it.


def load_yaml(path):
    """Read a YAML config. Databricks paths may start with dbfs:."""
    resolved_path = path.replace("dbfs:", "/dbfs")
    with open(resolved_path, "r") as handle:
        return yaml.safe_load(handle)


def detect_kind(cfg):
    """Identify the layer from the config's top-level keys.

    Each config shape has one key the others don't, so this is unambiguous.
    """
    if "objects" in cfg:
        return "extract"
    if "hubs" in cfg:
        return "vault"
    if "tables" in cfg:
        return "gold"
    if "satellites" in cfg:
        return "bsat"
    raise ValueError("Unrecognised config - no objects/hubs/tables/satellites key.")


def safe_name(value):
    """Mermaid box and column names allow word characters only."""
    return re.sub(r"\W", "_", str(value or "unknown"))


def safe_type(value):
    """decimal(18,2) -> decimal. Mermaid rejects brackets and commas in types."""
    text = str(value or "string").split("(")[0]
    return re.sub(r"\W", "_", text) or "string"


# COMMAND ----------

# DBTITLE 1,Step 2 - the diagram itself


class Erd:
    """Collects boxes and lines, then renders them as Mermaid or CSV.

    Deliberately dumb: the builders below decide *what* goes on the diagram,
    this class only knows how to write it out.
    """

    def __init__(self, title):
        self.title = title
        self.entities = {}   # box name -> [(type, column, "PK"/"FK"/"UK"/""), ...]
        self.edges = []      # [(left box, cardinality, right box, line label), ...]

    def entity(self, name, attributes):
        """Add a box (or replace one of the same name)."""
        self.entities[safe_name(name)] = attributes

    def edge(self, left, right, label, cardinality="||--o{"):
        """Add a line. Default cardinality is one-to-many, left to right."""
        self.edges.append((safe_name(left), cardinality, safe_name(right), label))

    def to_mermaid(self):
        """Render as a Mermaid erDiagram."""
        out = [f"%% {self.title}", "erDiagram"]

        for name, attrs in self.entities.items():
            shown = attrs if MAX_ATTRS == 0 else attrs[:MAX_ATTRS]
            out.append(f"    {name} {{")
            for dtype, col, marker in shown:
                line = f"        {safe_type(dtype)} {safe_name(col)}"
                if marker:
                    line += f" {marker}"
                out.append(line)

            # Rather than 87 rows of column names, say how many were hidden
            hidden = len(attrs) - len(shown)
            if hidden > 0:
                out.append(f"        string _and_{hidden}_more_columns")
            out.append("    }")

        for left, card, right, label in self.edges:
            out.append(f'    {left} {card} {right} : "{label}"')

        return "\n".join(out)

    def to_rows(self):
        """Render the lines as CSV rows, for Miro import."""
        return [
            {"from": left, "to": right, "label": label, "cardinality": card}
            for left, card, right, label in self.edges
        ]


# COMMAND ----------

# DBTITLE 1,Step 3 - one builder per layer
# Each builder reads its config shape and decides what to draw. They all return
# an Erd, so everything downstream is layer-agnostic.


def build_extract(cfg):
    """BRONZE - one box per source object.

    Bronze tables land as-is from the source, so there are no declared
    relationships. Turn on infer_relationships to guess them from column
    names (AccountId on the Case object -> a line from Account to Case).
    """
    erd = Erd(f"Bronze extract - {cfg.get('source_system')} / {cfg.get('brand')}")

    object_to_table = {}
    for obj in cfg["objects"]:
        if not obj.get("is_active", True):
            continue
        object_to_table[obj["source_object"]] = obj["target_table"]

        attributes = []
        for col in obj.get("columns") or []:
            marker = "PK" if col.get("is_primary_key") else ""
            attributes.append((col.get("data_type"), col["source_column"], marker))
        erd.entity(obj["target_table"], attributes)

    if INFER:
        for obj in cfg["objects"]:
            if not obj.get("is_active", True):
                continue
            for col in obj.get("columns") or []:
                name = col["source_column"]
                # "AccountId" -> is "Account" also an object we extract?
                if not name.endswith("Id") or col.get("is_primary_key"):
                    continue
                target = object_to_table.get(name[:-2])
                if target and target != obj["target_table"]:
                    # Dotted line, because this is a guess and not from config
                    erd.edge(
                        target, obj["target_table"], f"{name} (inferred)", "||..o{"
                    )
    return erd


def build_vault(cfg):
    """SILVER VAULT - hubs, links and satellites.

    Data Vault in one line each:
      hub       = a business entity, identified by its business key
      link      = a relationship between hubs
      satellite = the descriptive columns hanging off a hub or a link
    """
    erd = Erd(f"Silver vault - {cfg.get('source_system')} / {cfg.get('brand')}")

    # Built as we go, so links and satellites can find the hub they belong to
    hash_key_to_hub = {}

    for hub in cfg.get("hubs") or []:
        if not hub.get("is_active", True):
            continue
        hash_key_to_hub[hub["hash_key_name"]] = hub["table_name"]
        erd.entity(
            hub["table_name"],
            [
                ("string", hub["hash_key_name"], "PK"),
                ("string", hub["bk_column_name"], "UK"),
            ],
        )

    for link in cfg.get("links") or []:
        if not link.get("is_active", True):
            continue

        attributes = [("string", "LinkHashKey", "PK")]
        for ref in link.get("hub_references") or []:
            attributes.append(("string", ref["column_name"], "FK"))
        erd.entity(link["table_name"], attributes)

        # A link joins two or more hubs - draw one line per hub it references
        for ref in link.get("hub_references") or []:
            hub = hash_key_to_hub.get(ref["column_name"])
            if hub:
                erd.edge(
                    hub,
                    link["table_name"],
                    f"pattern {link.get('relationship_pattern', 3)}",
                )

    for sat in cfg.get("satellites") or []:
        if not sat.get("is_active", True):
            continue

        parent = sat["parent_key"]
        attributes = [("string", parent, "FK")]
        for target_column in (sat.get("attributes") or {}):
            attributes.append(("string", target_column, ""))
        erd.entity(sat["table_name"], attributes)

        # Most satellites hang off a hub. Link satellites hang off the link
        # built from the same bronze table, so look that up instead.
        owner = hash_key_to_hub.get(parent)
        if owner is None and sat.get("is_link_satellite"):
            owner = next(
                (
                    link["table_name"]
                    for link in cfg.get("links") or []
                    if link["source_table"] == sat["source_table"]
                ),
                None,
            )
        if owner:
            erd.edge(owner, sat["table_name"], "describes")

    return erd


def build_bsat(cfg):
    """SILVER BUSINESS SATELLITES - which vault satellite each one is built from.

    A business satellite reads one vault satellite and applies transforms
    (trim, normalise, sentinel defaults). One line per pair.
    """
    erd = Erd(f"Business satellites - {cfg.get('source_system')} / {cfg.get('brand')}")

    for sat in cfg.get("satellites") or []:
        if not sat.get("is_active", True):
            continue

        attributes = [("string", sat["parent_key_column"], "FK")]
        for attr in sat.get("attributes") or []:
            if attr.get("is_active", True):
                attributes.append(("string", attr["target_column"], ""))

        erd.entity(sat["table_name"], attributes)
        erd.entity(sat["source_table"], [("string", sat["parent_key_column"], "PK")])
        erd.edge(sat["source_table"], sat["table_name"], "derives")

    return erd


def build_gold(cfg, vault_cfg=None):
    """GOLD - the star schema: facts surrounded by the dimensions they join to.

    Gold declares foreign keys as "this fact reaches that hub through this
    link". To draw fact -> dimension we need to know which dimension is built
    on that hub, which is what the two lookups below work out.
    """
    erd = Erd(f"Gold - {cfg.get('source_system')} / {cfg.get('brand')}")

    # Exact route: hash key -> hub, but only the vault config knows this
    hash_key_to_hub = {}
    if vault_cfg:
        hash_key_to_hub = {
            hub["hash_key_name"]: hub["table_name"] for hub in vault_cfg.get("hubs") or []
        }

    # hub -> the dimension built on it
    hub_to_dim = {}
    for table in cfg["tables"]:
        if table.get("table_type") == "dim" and table.get("hub"):
            hub_to_dim.setdefault(table["hub"], table["table_name"])

    for table in cfg["tables"]:
        if not table.get("is_active", True):
            continue

        attributes = [("string", table["key_column"], "PK")]
        for fk in table.get("foreign_keys") or []:
            attributes.append(("string", fk["fk_column"], "FK"))
        for sat in table.get("satellites") or []:
            for col in sat.get("columns") or []:
                attributes.append(("string", col["source"], ""))
        erd.entity(table["table_name"], attributes)

    for table in cfg["tables"]:
        if not table.get("is_active", True):
            continue
        for fk in table.get("foreign_keys") or []:
            hub = hash_key_to_hub.get(fk["hub_key"])
            if hub is None:
                # No vault config supplied - fall back to the naming convention,
                # e.g. HubAccountHashKey -> a hub name ending in "accounts"
                stem = re.sub(r"^Hub|HashKey$", "", fk["hub_key"]).lower()
                hub = next((h for h in hub_to_dim if h.endswith(stem + "s")), None)

            dim = hub_to_dim.get(hub)
            if dim and dim != table["table_name"]:
                erd.edge(dim, table["table_name"], fk["fk_column"])

    return erd


# COMMAND ----------

# DBTITLE 1,Step 4 - build it
config = load_yaml(CONFIG_PATH)
kind = detect_kind(config)

if kind == "extract":
    erd = build_extract(config)
elif kind == "vault":
    erd = build_vault(config)
elif kind == "bsat":
    erd = build_bsat(config)
else:
    vault = load_yaml(VAULT_CONFIG_PATH) if VAULT_CONFIG_PATH else None
    if vault is None:
        print("NOTE: no vault_config_path given - FK->dim resolution uses naming rules.")
    erd = build_gold(config, vault)

# Narrow the picture. Lines are kept only when BOTH ends survive the filter,
# so the diagram never references a box that isn't drawn.
if ENTITY_FILTER:
    pattern = re.compile(ENTITY_FILTER, re.IGNORECASE)
    kept = {name for name in erd.entities if pattern.search(name)}
    dropped = len(erd.entities) - len(kept)
    erd.entities = {n: a for n, a in erd.entities.items() if n in kept}
    erd.edges = [e for e in erd.edges if e[0] in kept and e[2] in kept]
    print(f"Filter '{ENTITY_FILTER}' kept {len(kept)} entities, dropped {dropped}")

mermaid = erd.to_mermaid()
rows = erd.to_rows()

print(f"Config type   : {kind}")
print(f"Entities      : {len(erd.entities)}")
print(f"Relationships : {len(rows)}")

# COMMAND ----------

# DBTITLE 1,Step 5 - save the files
if OUTPUT_DIR:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stem = os.path.splitext(os.path.basename(CONFIG_PATH))[0]

    mmd_path = os.path.join(OUTPUT_DIR, f"{stem}.mmd")
    with open(mmd_path, "w") as handle:
        handle.write(mermaid + "\n")
    print(f"Wrote {mmd_path}")

    csv_path = os.path.join(OUTPUT_DIR, f"{stem}_edges.csv")
    with open(csv_path, "w", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=["from", "to", "label", "cardinality"]
        )
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {csv_path}")
else:
    print("(no output_dir set - printing only)")

# COMMAND ----------

# DBTITLE 1,Step 6 - the Mermaid source
# Copy this into https://mermaid.live if you want to tweak the layout by hand.
print(mermaid)

# COMMAND ----------

# DBTITLE 1,Step 7 - rendered diagram
try:
    displayHTML(
        """
        <div style="
            background: #fff;
            border: 1px solid #e5e7eb;
            border-radius: 12px;
            padding: 20px;
            overflow: auto;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.06);
        ">
          <div style="font: 600 16px/1.4 system-ui; margin-bottom: 12px; color: #111827;">
            ERD Preview
          </div>
          <div class="mermaid">%s</div>
        </div>
        <script type="module">
          import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
          mermaid.initialize({
            startOnLoad: true,
            er: { useMaxWidth: false }
          });
        </script>
        """
        % mermaid
    )
except NameError:
    pass
