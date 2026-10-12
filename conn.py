## DODE-2058: Enable Delta column mapping by name on framework-created tables

### Summary
All tables created by the framework now get `delta.columnMapping.mode = 'name'`. Without it, Delta does not allow `RENAME COLUMN` / `DROP COLUMN`, which blocks schema fixes such as the DODE-2030 key renames and column-name corrections. With column mapping by name, a rename or drop only changes metadata: no data is rewritten, and column names, loads and queries are unchanged.

- **New tables:** the property is set at creation, either in the `CREATE TABLE` DDL `TBLPROPERTIES` or with an `ALTER TABLE ... SET TBLPROPERTIES` right after the first `saveAsTable`.
- **Existing tables:** a one-time post-deployment script sets it.

### Files changed
| Layer | File | Change |
|---|---|---|
| Business vault | `Libraries/framework/business_satellite_metadata/loader.py` | 6 DDLs: bsats, business links, business link effsat, bhub, same-as links, bhub effsat |
| Gold | `Libraries/framework/gold_metadata/loader.py` | ALTER after first write in `_write_full_refresh` |
| Vault | `Libraries/framework/datavault_util/ddl_generator.py` | 4 DDLs: hubs, links, satellites, effsats |
| Bronze | `Libraries/framework/extract_metadata/loader.py` | ALTER after first write / reconcile_ids overwrite |
| Gold shared dims | `Shared/notebooks/gold/dim_dates.py`, `dim_currencies.py`, `dim_plan_rates.py`, `dim_addresses_runner.py` | ALTER after first write / `TBLPROPERTIES` in DDL |
| Deployment | `Deployment/dode_2058_column_mapping/enable_column_mapping.py` | **New.** One-time script: sets column mapping on all existing tables |

Includes the new business hub tables from DODE-2165.

### Post-deployment (once per environment)
Run `Deployment/dode_2058_column_mapping/enable_column_mapping.py` after the deploy and before the pipelines.

| Parameter | Value |
|---|---|
| `catalog_name` | target catalog (e.g. `bbdatawarehouse_dev`) |
| `schemas` | `bronze,silver,gold` (default) |
| `dry_run` | `true` first to review the list, then `false` to apply |

- Finds every Delta table in those schemas and sets the property on any that don't have it.
- Safe to re-run: tables that already have it are skipped. Fails the run if any table fails.
- One-time per environment. After that, the framework sets it on every new table.

### Notes
- Setting the property upgrades the Delta protocol (reader v2 / writer v5). This is supported on Databricks/UC and can't be undone; nothing in our stack reads these tables outside Databricks.
- Bronze: the ALTER runs after every overwrite (first create and `reconcile_ids`), so rebuilt tables keep the property.
- `dim_plan_rates`: the ALTER runs only on first create. Later runs use `MERGE`, which keeps the existing table and its properties.

### Testing
- [ ] All changed Python files compile
- [ ] New table created in dev (bsat + gold): `SHOW TBLPROPERTIES` shows `delta.columnMapping.mode = name`
- [ ] `ALTER TABLE ... RENAME COLUMN` succeeds on that table (renamed back after)
- [ ] Row counts match the previous run
- [ ] Post-deployment script in dev: `dry_run=true` lists the tables; a re-run shows `0 to set, N already set, 0 failed`
