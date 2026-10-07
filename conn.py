Here's a PR description you can paste into GitHub:

```markdown
## DODE-2030 — Restricted keys in gold

### Summary
Restricted gold dims now use a `...RestrictedKey` key column, and every fact that references a restricted hub carries a matching `...RestrictedKey` FK next to the existing `...Key` FK. The column-level masking policy matches on `Restricted` in the column name, so restricted keys are masked the same way restricted values are. Existing `...Key` columns are unchanged, so current joins and reports keep working.

YAML config changes only. No framework or job changes.

### Files changed
| Config | Restricted dim(s) — key renamed | FK copies added |
|---|---|---|
| `Chargebee/gold/config/gold_config_chargebee.yaml` | `chargebee_dim_customers_restricted` | 2 |
| `Salesforce/gold/config/gold_config_sitedocs.yaml` | `salesforce_dim_accounts_restricted` | 2 |
| `Salesforce/gold/config/gold_config_gocanvas.yaml` | accounts, contacts, users `_restricted` | 10 |
| `Salesforce/gold/config/gold_config_nexus.yaml` | accounts, contacts, users, cases, leads, orders `_restricted` | 31 (includes 4 own-key copies) |
| `Recurly/gold/config/gold_config_gocanvas.yaml` | `recurly_dim_accounts_restricted` | 2 |
| `Skilljar/gold/config/gold_config_skilljar.yaml` | `skilljar_dim_students_restricted` | 4 |

Nexus is applied on top of DODE-2165 (conformed product dimension). Products are not restricted, so `AssetProductKey` has no restricted copy.

Agreed. If no access has been granted on these tables yet, a drop has nothing to lose, so the record and re-apply steps don't apply. Replace the **Deployment steps** section of the PR description with this:

```markdown
### Deployment steps
The gold loader keeps existing columns, so the restricted dims must be dropped for the new key column to take effect. Fact tables do **not** need dropping; the new FK columns are added automatically (`mergeSchema`).


**1. Drop these 9 tables** (gold schema):
- `salesforce_dim_accounts_restricted` (shared: Nexus, GoCanvas, SiteDocs)
- `salesforce_dim_contacts_restricted` (shared: Nexus, GoCanvas)
- `salesforce_dim_users_restricted` (shared: Nexus, GoCanvas)
- `salesforce_dim_cases_restricted`
- `salesforce_dim_leads_restricted`
- `salesforce_dim_orders_restricted`
- `chargebee_dim_customers_restricted`
- `recurly_dim_accounts_restricted`
- `skilljar_dim_students_restricted`

**2. Reload gold:** Salesforce (Nexus, GoCanvas, SiteDocs — all three, because the tables are shared), Chargebee, Recurly, Skilljar.
```
