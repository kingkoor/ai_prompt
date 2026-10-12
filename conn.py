# Databricks notebook source
# DODE-2058 post-deployment: enable Delta column mapping by name on every
# EXISTING table in the given schemas. New tables already get it from the framework.
# Run once per environment. Safe to re-run: tables that already have it are skipped.
# Start with dry_run = true to see the list, then run again with dry_run = false.

dbutils.widgets.text("catalog_name", "bbdatawarehouse_dev", "Catalog Name")
dbutils.widgets.text("schemas", "bronze,silver,gold", "Schemas (comma-separated)")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run")

catalog = dbutils.widgets.get("catalog_name")
schemas = [s.strip() for s in dbutils.widgets.get("schemas").split(",") if s.strip()]
dry_run = dbutils.widgets.get("dry_run") == "true"

tables = spark.sql(f"""
    SELECT table_schema, table_name
    FROM {catalog}.information_schema.tables
    WHERE table_schema IN ({", ".join(f"'{s}'" for s in schemas)})
      AND table_type IN ('MANAGED', 'EXTERNAL')
      AND data_source_format = 'DELTA'
    ORDER BY table_schema, table_name
""").collect()

done, skipped, failed = [], [], []

for row in tables:
    full_name = f"{catalog}.{row.table_schema}.{row.table_name}"
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {full_name}").collect()}
    if props.get("delta.columnMapping.mode") == "name":
        skipped.append(full_name)
        continue

    if dry_run:
        print(f"WOULD SET  {full_name}")
        done.append(full_name)
        continue

    try:
        set_properties_sql = f"""
        ALTER TABLE {full_name} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
        """
        spark.sql(set_properties_sql)
        print(f"DONE  {full_name}")
        done.append(full_name)
    except Exception as e:
        print(f"FAILED  {full_name}: {e}")
        failed.append(full_name)

print(f"\n{'Dry run - ' if dry_run else ''}{len(done)} to set/set, {len(skipped)} already set, {len(failed)} failed")
if failed:
    raise RuntimeError(f"Column mapping failed for {len(failed)} table(s): {failed}")
