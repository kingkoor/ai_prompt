# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "workspace-base-environments/defaultenvironment-cggrgsujo0"
# environment_version = "5"
# ///
# DBTITLE 1,Account matching overview
# MAGIC %md
# MAGIC ## account_matching_salesforce
# MAGIC
# MAGIC Resolves a de-duplicated parent account per Salesforce account and writes
# MAGIC the result to a current-state table that a business satellite reads.
# MAGIC
# MAGIC | Parameter | Default | Description |
# MAGIC | --- | --- | --- |
# MAGIC | `config_path` | _(required)_ | Workspace path to the vault config YAML (tenant + schema settings) |
# MAGIC | `catalog_name` | `bbdatawarehouse_dev` | Unity Catalog target catalog (overrides YAML) |
# MAGIC
# MAGIC ### What it does
# MAGIC * Normalises name and address using the shared `cleansing` functions
# MAGIC * Builds `AccountDedupeKey` from the normalised values
# MAGIC * Detects circular `ParentId` references (A -> B -> A)
# MAGIC * Scores each account and picks one golden record per dedupe group
# MAGIC * Emits `AccountEDWParentId`: the real Salesforce parent when it is valid
# MAGIC   and not part of a loop, otherwise the winning account of the group
# MAGIC
# MAGIC ### Position in the pipeline
# MAGIC Runs **after** `load_vault` and **before** `load_business_satellites`.
# MAGIC Reads current-state vault rows (`ETL_EndDate IS NULL`) and overwrites its
# MAGIC output table each run.
# MAGIC
# MAGIC Replaces the Fabric notebooks `account_matching_salesforce.py` +
# MAGIC the account-matching join in `salesforce_accounts.py`.

# COMMAND ----------

# DBTITLE 1,Parameters and framework imports
import os
import sys
import importlib

import yaml
from pyspark.sql import Window
from pyspark.sql import functions as f

dbutils.widgets.text("config_path", "", "Vault Config Path")
dbutils.widgets.text("catalog_name", "bbdatawarehouse_dev", "Catalog Name")

config_path = dbutils.widgets.get("config_path")
catalog_name = dbutils.widgets.get("catalog_name")

if not config_path:
    raise ValueError("config_path is required.")

# Add libraries to path (same pattern as the shared framework notebooks)
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_dir = f"/Workspace{os.path.dirname(notebook_path)}"
repo_root = os.path.dirname(os.path.dirname(os.path.dirname(notebook_dir)))
libraries_path = os.path.join(repo_root, "Libraries")
framework_path = os.path.join(libraries_path, "framework")

sys.path.insert(0, libraries_path)
sys.path.insert(0, framework_path)

import cleansing
importlib.reload(cleansing)
from cleansing import (
    normalize_city,
    normalize_company_name,
    normalize_country,
    normalize_postal,
    normalize_state_province,
    normalize_street_address,
)

with open(config_path.replace("dbfs:", "/dbfs"), "r") as handle:
    cfg = yaml.safe_load(handle)

CATALOG = catalog_name or cfg["target_catalog"]
SILVER = cfg["silver_schema"]
BRAND = cfg["brand"]
REGION = cfg["region"]

print(f"Catalog: {CATALOG}   Schema: {SILVER}   Brand: {BRAND}   Region: {REGION}")

# COMMAND ----------

# DBTITLE 1,Source tables and columns - VERIFY THESE FIRST
# MAGIC %md
# MAGIC Every table and column name this notebook depends on is listed in the
# MAGIC next cell. If a name is wrong the run fails immediately with
# MAGIC `AnalysisException: Column ... cannot be resolved` - correct it here,
# MAGIC not further down.

# COMMAND ----------

# --- source tables -------------------------------------------------------
T_HUB_ACCOUNT = f"{CATALOG}.{SILVER}.salesforce_hub_accounts"
T_SAT_ACCOUNT = f"{CATALOG}.{SILVER}.salesforce_sat_accounts_bluebeam_us"
T_SAT_ACCOUNT_RESTRICTED = f"{CATALOG}.{SILVER}.salesforce_sat_accounts_restricted_bluebeam_us"
T_LINK_ASSET_ACCOUNT = f"{CATALOG}.{SILVER}.salesforce_link_asset_account"
T_SAT_ASSET = f"{CATALOG}.{SILVER}.salesforce_sat_assets_bluebeam_us"

# --- target table --------------------------------------------------------
T_TARGET = f"{CATALOG}.{SILVER}.salesforce_account_matching_bluebeam_us"

# --- hub columns ---------------------------------------------------------
C_HUB_KEY = "HubAccountHashKey"
C_ACCOUNT_BK = "AccountBk"                    # the Salesforce Id

# --- general account satellite columns -----------------------------------
C_PARENT_ID = "AccountParentId"
C_CREATED_DATE = "AccountCreatedDate"
C_EXTERNAL_ID = "AccountExternalId"
C_ORG_ADMIN = "AccountOrgAdminProEnabled"
C_CSM_OWNER_NAME = "AccountCSMOwnerName"

# --- restricted (PII) account satellite columns --------------------------
# NOTE: these hold RAW values. The "Normalized" suffix describes intent, not
# state - vault satellites are pure renames. Normalisation happens below.
C_NAME = "AccountNameNormalized"
C_STREET = "AccountBillingStreetNormalized"
C_CITY = "AccountBillingCityNormalized"
C_STATE = "AccountBillingStateNormalized"
C_POSTAL = "AccountBillingPostalCodeNormalized"
C_COUNTRY = "AccountBillingCountryNormalized"

# --- asset columns (reached through the link) ----------------------------
C_HUB_ASSET_KEY = "HubAssetHashKey"
C_ASSET_LIFECYCLE_END = "AssetLifecycleEndDate"

# --- scoring weights -----------------------------------------------------
W_NEW_ORG_ADMIN = 13
W_EXTERNAL_ID = 11
W_MANAGED_OWNER = 5
W_ACTIVE_ASSET = 5

TENANT_COLUMNS = ["ETL_SourceSystem", "ETL_Brand", "ETL_Region"]

# COMMAND ----------

# DBTITLE 1,Read current-state vault rows


def current_rows(table):
    """Current (open) versions for this tenant only."""
    df = spark.table(table)
    if "ETL_EndDate" in df.columns:
        df = df.filter(f.col("ETL_EndDate").isNull())
    return df.filter(
        (f.col("ETL_Brand") == BRAND) & (f.col("ETL_Region") == REGION)
    )


hub = spark.table(T_HUB_ACCOUNT).select(C_HUB_KEY, C_ACCOUNT_BK)

sat = current_rows(T_SAT_ACCOUNT).select(
    C_HUB_KEY,
    C_PARENT_ID,
    C_CREATED_DATE,
    C_EXTERNAL_ID,
    C_ORG_ADMIN,
    C_CSM_OWNER_NAME,
    "ETL_RecordStatus",
    *TENANT_COLUMNS,
)

restricted = current_rows(T_SAT_ACCOUNT_RESTRICTED).select(
    C_HUB_KEY, C_NAME, C_STREET, C_CITY, C_STATE, C_POSTAL, C_COUNTRY
)

base = (
    sat.join(hub, on=C_HUB_KEY, how="inner")
    .join(restricted, on=C_HUB_KEY, how="left")
)

print(f"Accounts in scope: {base.count()}")

# COMMAND ----------

# DBTITLE 1,Normalise and build the dedupe key
# Country first - normalize_state_province needs an ISO code to decide
# whether the state value is meaningful for that country.
base = base.withColumn("_country_n", normalize_country(f.col(C_COUNTRY)))

base = (
    base.withColumn("_name_n", normalize_company_name(f.col(C_NAME)))
    .withColumn("_street_n", normalize_street_address(f.col(C_STREET)))
    .withColumn("_city_n", normalize_city(f.col(C_CITY)))
    .withColumn("_postal_n", normalize_postal(f.col(C_POSTAL)))
    .withColumn(
        "_state_n", normalize_state_province(f.col(C_STATE), f.col("_country_n"))
    )
)

base = base.withColumn(
    "AccountDedupeKey",
    f.concat_ws(
        "|",
        f.coalesce(f.col("_name_n"), f.lit("")),
        f.coalesce(f.col("_street_n"), f.lit("")),
        f.coalesce(f.col("_city_n"), f.lit("")),
        f.coalesce(f.col("_postal_n"), f.lit("")),
        f.coalesce(f.col("_state_n"), f.lit("")),
        f.coalesce(f.col("_country_n"), f.lit("")),
    ),
)

# COMMAND ----------

# DBTITLE 1,Detect circular ParentId references
has_parent = f.col(C_PARENT_ID).isNotNull() & (f.trim(f.col(C_PARENT_ID)) != "")

parent_lookup = base.select(
    f.col(C_ACCOUNT_BK).alias("_parent_bk"),
    f.col(C_PARENT_ID).alias("_grandparent_id"),
)

base = base.join(
    parent_lookup, f.col(C_PARENT_ID) == f.col("_parent_bk"), "left"
).withColumn(
    "_is_loop",
    f.coalesce(f.col("_grandparent_id") == f.col(C_ACCOUNT_BK), f.lit(False)),
)

print(f"Circular references: {base.filter(f.col('_is_loop')).count()}")

# COMMAND ----------

# DBTITLE 1,Score each account
active_assets = (
    spark.table(T_LINK_ASSET_ACCOUNT)
    .select(C_HUB_ASSET_KEY, C_HUB_KEY)
    .join(
        current_rows(T_SAT_ASSET).select(C_HUB_ASSET_KEY, C_ASSET_LIFECYCLE_END),
        on=C_HUB_ASSET_KEY,
        how="inner",
    )
    .groupBy(C_HUB_KEY)
    .agg(
        f.sum(
            f.when(f.col(C_ASSET_LIFECYCLE_END).isNull(), f.lit(1)).otherwise(f.lit(0))
        ).alias("_active_asset_count")
    )
)

base = base.join(active_assets, on=C_HUB_KEY, how="left")

is_ext_id_valid = f.col(C_EXTERNAL_ID).isNotNull() & (
    f.trim(f.col(C_EXTERNAL_ID)) != ""
)
is_new_org_admin = is_ext_id_valid & (f.col(C_ORG_ADMIN) == True)  # noqa: E712
is_managed_owner = (
    f.col(C_CSM_OWNER_NAME).isNotNull()
    & ~f.lower(f.col(C_CSM_OWNER_NAME)).contains("unmanaged")
    & ~f.lower(f.col(C_CSM_OWNER_NAME)).contains("integration")
)

base = base.withColumn(
    "_score",
    f.when(is_new_org_admin, f.lit(W_NEW_ORG_ADMIN)).otherwise(f.lit(0))
    + f.when(is_ext_id_valid, f.lit(W_EXTERNAL_ID)).otherwise(f.lit(0))
    + f.when(is_managed_owner, f.lit(W_MANAGED_OWNER)).otherwise(f.lit(0))
    + f.when(
        f.coalesce(f.col("_active_asset_count"), f.lit(0)) >= 1,
        f.lit(W_ACTIVE_ASSET),
    ).otherwise(f.lit(0)),
)

# COMMAND ----------

# DBTITLE 1,Pick the golden record per dedupe group
group_window = Window.partitionBy("AccountDedupeKey").orderBy(
    f.col("_score").desc(),                     # 1. highest score
    f.col(C_CREATED_DATE).asc_nulls_last(),     # 2. oldest record
    f.col(C_ACCOUNT_BK).asc(),                  # 3. deterministic tie-break
)

# Group size is kept as an output column: a dedupe key shared by a very large
# number of accounts means the key is degenerate (e.g. blank name and address
# all collapsing to "|||||") rather than a genuine set of duplicates.
group_all = Window.partitionBy("AccountDedupeKey")

base = base.withColumn("_rank", f.row_number().over(group_window)).withColumn(
    "AccountGroupSize", f.count(f.lit(1)).over(group_all)
)

winners = base.filter(f.col("_rank") == 1).select(
    "AccountDedupeKey", f.col(C_ACCOUNT_BK).alias("_winner_bk")
)

base = base.join(winners, on="AccountDedupeKey", how="left")

# COMMAND ----------

# DBTITLE 1,Resolve the parent and label the match tier
keep_source_parent = has_parent & ~f.col("_is_loop")

result = (
    base.withColumn(
        "AccountEDWParentId",
        f.when(keep_source_parent, f.col(C_PARENT_ID)).otherwise(f.col("_winner_bk")),
    )
    .withColumn(
        "AccountMatchTier",
        f.when(keep_source_parent, f.lit("Salesforce Parent Preserved"))
        .when(has_parent, f.lit("Loop Broken (Winner Fallback)"))
        .otherwise(f.lit("No Parent (Winner Fallback)")),
    )
    .select(
        C_HUB_KEY,
        "AccountEDWParentId",
        "AccountMatchTier",
        # --- diagnostics: matching table only, not promoted to the bsat ---
        "AccountDedupeKey",
        f.col("_score").alias("AccountScore"),
        f.col("AccountGroupSize"),
        (f.col("_rank") == 1).alias("AccountIsGroupWinner"),
        f.col("_winner_bk").alias("AccountWinnerBk"),
        f.coalesce(f.col("_active_asset_count"), f.lit(0)).alias(
            "AccountActiveAssetCount"
        ),
        f.col("_is_loop").alias("AccountHasParentLoop"),
        # ------------------------------------------------------------------
        f.lit(None).cast("timestamp").alias("ETL_EndDate"),
        "ETL_RecordStatus",
        *TENANT_COLUMNS,
    )
)

display(result.groupBy("AccountMatchTier").count())

# COMMAND ----------

# DBTITLE 1,Write current-state output
(
    result.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(T_TARGET)
)

print(f"Wrote {result.count()} rows to {T_TARGET}")

# COMMAND ----------

# DBTITLE 1,Match quality checks
# MAGIC %md
# MAGIC Read these before trusting the output.
# MAGIC
# MAGIC * **Degenerate keys** - a dedupe key made only of separators (`|||||`)
# MAGIC   means blank name *and* blank address. Every such account collapses
# MAGIC   into one group and is parented to whichever one wins. If the count
# MAGIC   here is not small, the grouping is wrong, not the data.
# MAGIC * **Largest groups** - genuine duplicate sets are small. A group of
# MAGIC   hundreds is a normalisation problem.
# MAGIC * **Score spread** - if almost every account scores 0 the tie-break
# MAGIC   falls through to oldest-then-alphabetical, which is arbitrary.

# COMMAND ----------

written = spark.table(T_TARGET)

print("--- degenerate dedupe keys (blank name AND address) ---")
degenerate = written.filter(f.regexp_replace(f.col("AccountDedupeKey"), r"\|", "") == "")
print(f"accounts with an empty dedupe key: {degenerate.count()}")

print("\n--- largest dedupe groups ---")
display(
    written.select("AccountDedupeKey", "AccountGroupSize")
    .distinct()
    .orderBy(f.col("AccountGroupSize").desc())
    .limit(20)
)

print("\n--- group size distribution ---")
display(
    written.groupBy("AccountGroupSize")
    .count()
    .orderBy(f.col("AccountGroupSize").asc())
    .limit(20)
)

print("\n--- score distribution ---")
display(written.groupBy("AccountScore").count().orderBy(f.col("AccountScore").desc()))

     
