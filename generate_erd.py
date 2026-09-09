# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # ERD Generator - Mermaid diagrams from YAML config
# MAGIC
# MAGIC Point it at any config in this repo and it produces a Mermaid `erDiagram`
# MAGIC plus a CSV edge list. Config-driven, so the diagram never drifts from the
# MAGIC pipeline the way a hand-drawn one does.
# MAGIC
# MAGIC | Config type | Detected by | Entities | Relationships |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `extract` (bronze) | top-level `objects:` | one per source object | none, unless `infer_relationships` is on |
# MAGIC | `vault` (silver) | top-level `hubs:` | hubs, links, satellites | hub->sat, hub->link |
# MAGIC | `bsat` (silver) | top-level `satellites:` | vault sat -> business sat | derives |
# MAGIC | `gold` | top-level `tables:` | dims and facts | fact -> dim via `foreign_keys` |
# MAGIC
# MAGIC | Parameter | Default | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `config_path` | _(required)_ | Path to the YAML config |
# MAGIC | `output_dir` | _(blank)_ | Where to write `.mmd` / `.csv`. Blank = print only |
# MAGIC | `max_attributes` | `12` | Columns shown per entity. `0` = all |
# MAGIC | `vault_config_path` | _(blank)_ | Gold only - resolves FK hub keys to dim names exactly |
# MAGIC | `infer_relationships` | `false` | Bronze only - guess FKs from `<Object>Id` column names |
# MAGIC | `entity_filter` | _(blank)_ | Regex - keep only matching entities. A full vault is 78 entities and renders as a hairball; scope it before presenting |
# MAGIC
# MAGIC ### Viewing the output
# MAGIC * **Mermaid** - paste the `.mmd` into <https://mermaid.live>, a Markdown
# MAGIC   file, or Confluence
# MAGIC * **Miro** - import the `.csv` edge list, or use Miro's Mermaid plugin and
# MAGIC   paste the `.mmd` directly
# MAGIC
# MAGIC Runs in Databricks or as a plain script:
# MAGIC `python generate_erd.py <config_path> [output_dir]`

# COMMAND ----------

# DBTITLE 1,Parameters
import csv
import os
import re
import sys

import yaml

try:
    dbutils.widgets.text("config_path", "", "Config Path")
    dbutils.widgets.text("output_dir", "", "Output Dir (blank = print only)")
    dbutils.widgets.text("max_attributes", "12", "Max Attributes Per Entity (0 = all)")
    dbutils.widgets.text("vault_config_path", "", "Vault Config (gold only, optional)")
    dbutils.widgets.dropdown(
        "infer_relationships", "false", ["true", "false"], "Infer FKs (bronze only)"
    )
    dbutils.widgets.text("entity_filter", "", "Entity Filter (regex, blank = all)")

    CONFIG_PATH = dbutils.widgets.get("config_path")
    OUTPUT_DIR = dbutils.widgets.get("output_dir")
    MAX_ATTRS = int(dbutils.widgets.get("max_attributes") or 12)
    VAULT_CONFIG_PATH = dbutils.widgets.get("vault_config_path")
    INFER = dbutils.widgets.get("infer_relationships") == "true"
    ENTITY_FILTER = dbutils.widgets.get("entity_filter")
except NameError:  # plain python
    CONFIG_PATH = sys.argv[1] if len(sys.argv) > 1 else ""
    OUTPUT_DIR = sys.argv[2] if len(sys.argv) > 2 else ""
    MAX_ATTRS = int(os.environ.get("MAX_ATTRS", "12"))
    VAULT_CONFIG_PATH = os.environ.get("VAULT_CONFIG", "")
    INFER = os.environ.get("INFER", "") == "true"
    ENTITY_FILTER = os.environ.get("ENTITY_FILTER", "")

if not CONFIG_PATH:
    raise ValueError("config_path is required.")

# COMMAND ----------

# DBTITLE 1,Helpers


def load_yaml(path):
    """Read a YAML config, tolerating dbfs: prefixes."""
    return yaml.safe_load(open(path.replace("dbfs:", "/dbfs"), "r"))


def detect_kind(cfg):
    """Work out which config shape this is."""
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
    """Mermaid entity/attribute names allow word characters only."""
    return re.sub(r"\W", "_", str(value or "unknown"))


def safe_type(value):
    """decimal(18,2) -> decimal. Mermaid rejects brackets and commas."""
    text = str(value or "string").split("(")[0]
    return re.sub(r"\W", "_", text) or "string"


class Erd:
    """Accumulates entities and relationships, renders Mermaid + CSV."""

    def __init__(self, title):
        self.title = title
        self.entities = {}   # name -> list of (type, column, key_marker)
        self.edges = []      # (left, cardinality, right, label)

    def entity(self, name, attributes):
        self.entities[safe_name(name)] = attributes

    def edge(self, left, right, label, cardinality="||--o{"):
        self.edges.append((safe_name(left), cardinality, safe_name(right), label))

    def to_mermaid(self):
        out = [f"%% {self.title}", "erDiagram"]
        for name, attrs in self.entities.items():
            shown = attrs if MAX_ATTRS == 0 else attrs[:MAX_ATTRS]
            out.append(f"    {name} {{")
            for dtype, col, marker in shown:
                line = f"        {safe_type(dtype)} {safe_name(col)}"
                if marker:
                    line += f" {marker}"
                out.append(line)
            hidden = len(attrs) - len(shown)
            if hidden > 0:
                out.append(f"        string _and_{hidden}_more_columns")
            out.append("    }")
        for left, card, right, label in self.edges:
            out.append(f'    {left} {card} {right} : "{label}"')
        return "\n".join(out)

    def to_rows(self):
        return [
            {"from": l, "to": r, "label": lb, "cardinality": c}
            for l, c, r, lb in self.edges
        ]


# COMMAND ----------

# DBTITLE 1,Builders - one per config shape


def build_extract(cfg):
    """Bronze: one entity per source object, optionally with inferred FKs."""
    erd = Erd(f"Bronze extract - {cfg.get('source_system')} / {cfg.get('brand')}")
    by_object = {}
    for obj in cfg["objects"]:
        if not obj.get("is_active", True):
            continue
        table = obj["target_table"]
        by_object[obj["source_object"]] = table
        attrs = []
        for col in obj.get("columns") or []:
            marker = "PK" if col.get("is_primary_key") else ""
            attrs.append((col.get("data_type"), col["source_column"], marker))
        erd.entity(table, attrs)

    if INFER:
        # <Object>Id on table A, where <Object> is another extracted object
        for obj in cfg["objects"]:
            if not obj.get("is_active", True):
                continue
            for col in obj.get("columns") or []:
                name = col["source_column"]
                if not name.endswith("Id") or col.get("is_primary_key"):
                    continue
                target = by_object.get(name[:-2])
                if target and target != obj["target_table"]:
                    erd.edge(target, obj["target_table"], f"{name} (inferred)", "||..o{")
    return erd


def build_vault(cfg):
    """Silver vault: hubs, links and satellites with their real relationships."""
    erd = Erd(f"Silver vault - {cfg.get('source_system')} / {cfg.get('brand')}")
    hash_to_hub = {}

    for hub in cfg.get("hubs") or []:
        if not hub.get("is_active", True):
            continue
        hash_to_hub[hub["hash_key_name"]] = hub["table_name"]
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
        attrs = [("string", "LinkHashKey", "PK")]
        for ref in link.get("hub_references") or []:
            attrs.append(("string", ref["column_name"], "FK"))
        erd.entity(link["table_name"], attrs)
        for ref in link.get("hub_references") or []:
            hub = hash_to_hub.get(ref["column_name"])
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
        attrs = [("string", parent, "FK")]
        for target, source in (sat.get("attributes") or {}).items():
            attrs.append(("string", target, ""))
        erd.entity(sat["table_name"], attrs)
        owner = hash_to_hub.get(parent)
        if owner is None and sat.get("is_link_satellite"):
            # link satellites hang off the link built from the same source table
            owner = next(
                (
                    l["table_name"]
                    for l in cfg.get("links") or []
                    if l["source_table"] == sat["source_table"]
                ),
                None,
            )
        if owner:
            erd.edge(owner, sat["table_name"], "describes")
    return erd


def build_bsat(cfg):
    """Business satellites: which vault satellite each one derives from."""
    erd = Erd(f"Business satellites - {cfg.get('source_system')} / {cfg.get('brand')}")
    for sat in cfg.get("satellites") or []:
        if not sat.get("is_active", True):
            continue
        attrs = [("string", sat["parent_key_column"], "FK")]
        for attr in sat.get("attributes") or []:
            if attr.get("is_active", True):
                attrs.append(("string", attr["target_column"], ""))
        erd.entity(sat["table_name"], attrs)
        erd.entity(sat["source_table"], [("string", sat["parent_key_column"], "PK")])
        erd.edge(sat["source_table"], sat["table_name"], "derives")
    return erd


def build_gold(cfg, vault_cfg=None):
    """Gold star schema: facts joined to dims through the vault links."""
    erd = Erd(f"Gold - {cfg.get('source_system')} / {cfg.get('brand')}")

    hash_to_hub = {}
    if vault_cfg:
        hash_to_hub = {
            h["hash_key_name"]: h["table_name"] for h in vault_cfg.get("hubs") or []
        }

    hub_to_dim = {}
    for table in cfg["tables"]:
        if table.get("table_type") == "dim" and table.get("hub"):
            hub_to_dim.setdefault(table["hub"], table["table_name"])

    for table in cfg["tables"]:
        if not table.get("is_active", True):
            continue
        attrs = [("string", table["key_column"], "PK")]
        for fk in table.get("foreign_keys") or []:
            attrs.append(("string", fk["fk_column"], "FK"))
        for sat in table.get("satellites") or []:
            for col in sat.get("columns") or []:
                attrs.append(("string", col["source"], ""))
        erd.entity(table["table_name"], attrs)

    for table in cfg["tables"]:
        if not table.get("is_active", True):
            continue
        for fk in table.get("foreign_keys") or []:
            hub = hash_to_hub.get(fk["hub_key"])
            if hub is None:
                # fall back to the naming convention: HubAccountHashKey -> accounts
                stem = re.sub(r"^Hub|HashKey$", "", fk["hub_key"]).lower()
                hub = next((h for h in hub_to_dim if h.endswith(stem + "s")), None)
            dim = hub_to_dim.get(hub)
            if dim and dim != table["table_name"]:
                erd.edge(dim, table["table_name"], fk["fk_column"], "||--o{")
    return erd


# COMMAND ----------

# DBTITLE 1,Generate
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

if ENTITY_FILTER:
    pattern = re.compile(ENTITY_FILTER, re.IGNORECASE)
    kept = {n for n in erd.entities if pattern.search(n)}
    dropped = len(erd.entities) - len(kept)
    erd.entities = {n: a for n, a in erd.entities.items() if n in kept}
    # keep only edges whose BOTH ends survived, so no dangling references
    erd.edges = [e for e in erd.edges if e[0] in kept and e[2] in kept]
    print(f"Filter '{ENTITY_FILTER}' kept {len(kept)} entities, dropped {dropped}")

mermaid = erd.to_mermaid()
rows = erd.to_rows()

print(f"Config type : {kind}")
print(f"Entities    : {len(erd.entities)}")
print(f"Relationships: {len(rows)}")

# COMMAND ----------

# DBTITLE 1,Write outputs
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
    print("(no output_dir - printing only)")

# COMMAND ----------

# DBTITLE 1,Preview
print(mermaid)

# COMMAND ----------

# DBTITLE 1,Render inline (Databricks only)
try:
    displayHTML(
        """
        <div class="mermaid">%s</div>
        <script type="module">
          import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
          mermaid.initialize({ startOnLoad: true, er: { useMaxWidth: false } });
        </script>
        """
        % mermaid
    )
except NameError:
    pass
