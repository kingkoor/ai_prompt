# Databricks notebook source
# DODE-2030 post-deployment: rename the key column on the restricted gold dims
# from ...Key to ...RestrictedKey. Run once per environment, before the gold jobs.
# Safe to re-run: tables that don't exist or are already renamed are skipped.
# Fact tables need nothing here; their new ...RestrictedKey FK columns are added
# by the gold loader (mergeSchema).

dbutils.widgets.text("catalog_name", "bbdatawarehouse_dev", "Catalog Name")
catalog = dbutils.widgets.get("catalog_name")

# table -> (old key column, new key column)
RENAMES = {
    "salesforce_dim_accounts_restricted": ("AccountKey", "AccountRestrictedKey"),
    "salesforce_dim_contacts_restricted": ("ContactKey", "ContactRestrictedKey"),
    "salesforce_dim_users_restricted":    ("UserKey", "UserRestrictedKey"),
    "salesforce_dim_cases_restricted":    ("CaseKey", "CaseRestrictedKey"),
    "salesforce_dim_leads_restricted":    ("LeadKey", "LeadRestrictedKey"),
    "salesforce_dim_orders_restricted":   ("OrderKey", "OrderRestrictedKey"),
    "chargebee_dim_customers_restricted": ("CustomerKey", "CustomerRestrictedKey"),
    "recurly_dim_accounts_restricted":    ("AccountKey", "AccountRestrictedKey"),
    "skilljar_dim_students_restricted":   ("StudentKey", "StudentRestrictedKey"),
}

for table, (old_col, new_col) in RENAMES.items():
    full_name = f"{catalog}.gold.{table}"
    if not spark.catalog.tableExists(full_name):
        print(f"SKIP  {full_name}: table does not exist")
        continue

    columns = [f.name for f in spark.table(full_name).schema.fields]
    if new_col in columns:
        print(f"SKIP  {full_name}: {new_col} already exists")
        continue
    if old_col not in columns:
        raise ValueError(f"{full_name}: neither {old_col} nor {new_col} found")

    set_properties_sql = f"""
    ALTER TABLE {full_name} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
    """
    spark.sql(set_properties_sql)

    alter_sql = f"""
    ALTER TABLE {full_name}
    RENAME COLUMN {old_col} TO {new_col}
    """
    spark.sql(alter_sql)
    print(f"DONE  {full_name}: {old_col} -> {new_col}")

