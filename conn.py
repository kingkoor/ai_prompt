# Chargebee (SiteDocs) — Design Document & Presentation Content

Content source: the Chargebee configs and README in the repo (bronze, silver vault, business satellites, gold).
Use Part 1 for the Word design document and Part 2 for the PowerPoint. Part 3 is a short prep sheet for the Q&A.
Numbers (6 objects, 6 hubs, 4 links, 10 satellites, 7 bsats, 5 gold tables) come from the configs. Re-check them against the office copy before presenting.

---

# PART 1 — DESIGN DOCUMENT

## 1. Purpose and scope

**Purpose.** This document explains how Chargebee billing data for the SiteDocs brand flows through the lakehouse (bronze → silver Data Vault → business vault → gold), why each structure exists, and proposes one design improvement.

**Business context.** Chargebee is SiteDocs' subscription billing system. It holds who the customers are, what plan they're on, their subscriptions, and what they were invoiced. Reporting needs: MRR/ARR, subscription status and movement, invoicing, and linking Chargebee customers to their Salesforce accounts.

**In scope.** 6 Chargebee objects: customers, plans, subscriptions, invoices, addons, coupons. Brand SiteDocs, region US.

**Out of scope.**
- Nested arrays (invoice line items, taxes, discounts, subscription addons) — they would need their own child tables.
- Sensitive payment data (SPI) — never extracted (see section 7).
- The Salesforce ↔ Chargebee relationship inside the vault (see section 8 and the improvement proposal).

## 2. Architecture overview

```
Chargebee REST API v2
   │  fetch_chargebee_raw.py  (API → raw JSON staging, 7-day retention)
   ▼
BRONZE   6 tables  chargebee_<object>_sitedocs_us        (typed, flattened, incremental on updated_at)
   │  bronze_to_silver_vault.py  (vault_config_chargebee.yaml)
   ▼
SILVER — RAW VAULT   6 hubs · 4 links · 1 effectivity satellite · 10 satellites (3 restricted)
   │  silver_to_business_satellite.py  (business_satellite_config_chargebee.yaml)
   ▼
SILVER — BUSINESS VAULT   7 business satellites (money, dates, currency, address keys)
   │  dim_addresses_runner.py  → shared gold dim_addresses
   │  silver_to_gold.py        (gold_config_chargebee.yaml)
   ▼
GOLD   chargebee_dim_customers, chargebee_dim_customers_restricted, chargebee_dim_plans,
       chargebee_fact_subscriptions, chargebee_fact_invoices
       + shared dims: dim_dates, dim_currencies, dim_addresses
```

**Key principle — everything is configuration.** No Chargebee-specific SQL is hand-written for silver or gold. Each layer is a YAML config read by the shared framework (`Libraries/framework`). Adding a column or table means editing YAML, not code.

**Jobs**
| Job | Tasks (in order) |
|---|---|
| `chargebee_bronze_extract` | generate_inputs → fetch_raw → extract_objects (per object) → purge_staging |
| `chargebee_silver_vault` | load_vault → load_business_satellites |
| `chargebee_gold` | dim_address_runner → load_gold |

## 3. Bronze layer — land the data as-is, safely

**What:** one table per object, `chargebee_<object>_sitedocs_us`. Incremental daily loads on `updated_at`.

**Why it's built this way**
- **Raw but typed.** Bronze keeps source values unchanged (no business logic), so anything downstream can be rebuilt from it.
- **Dates stay as epoch integers (`bigint`).** The Chargebee API returns every date as Unix epoch seconds. Casting them to timestamp in bronze would silently produce NULLs, so conversion is deferred to the business vault.
- **Money stays in integer cents.** Converted to dollars later, once, in one place.
- **No JSON columns.** Nested fields are flattened only when something needs them (today: billing/shipping address parts for the address key).
- **Data-governance driven column list.** Columns follow the DG classification spreadsheet: SPI is never extracted; PI/PII is extracted only where needed and routed to restricted satellites.

## 4. Silver — Raw Data Vault

### 4.1 Hubs (6) — "what are the business things?"
One hub per business entity, keyed on Chargebee's `id`:
`chargebee_hub_customers`, `_subscriptions`, `_plans`, `_invoices`, `_addons`, `_coupons`.

**Why:** a hub holds only the business key and its hash key (`HubCustomerHashKey` = MD5 of `id` + source system + brand + region). It never changes, so it's the stable anchor everything else joins to. Including brand/region in the hash keeps tenants separate even if two brands had the same id.

### 4.2 Links (4) — "how are the things related?"
| Link | Relationship | Pattern | Why this pattern |
|---|---|---|---|
| `link_subscription_customer` | subscription → customer | 3 (immutable) | A subscription belongs to one customer for life |
| `link_subscription_plan` | subscription → plan | **1 (driving key, with effectivity satellite)** | The plan **changes** on upgrade/downgrade — this is what drives MRR movement, so history must be tracked |
| `link_invoice_subscription` | invoice → subscription | 3 | Never changes |
| `link_invoice_customer` | invoice → customer | 3 | Never changes |

**Why links are separate tables:** relationships can change independently of the entities. Keeping them separate means a plan change doesn't rewrite the subscription, and many-to-many relationships are possible without redesign.

**Effectivity satellite** (`chargebee_effsat_chargebee_link_subscription_plan_sitedocs`): records when each subscription-plan pairing became active or inactive. Gold uses it to pick the *current* plan.

### 4.3 Satellites (10) — "what do we know about each thing, over time?"
- One normal satellite per hub (customers, subscriptions, plans, invoices, addons, coupons).
- Three **restricted** satellites (customers, subscriptions, invoices) hold PI/PII: company, email, phone, addresses.

**Why:** satellites keep full history — a new row is added only when an attribute changes (detected by a hash diff). Splitting restricted data into its own satellite means access can be granted or denied at table level.

## 5. Silver — Business Vault (7 business satellites)

Business rules are applied **once** here, so every consumer gets the same answer.

| Rule | Transform | Example |
|---|---|---|
| Money cents → dollars | `cents_to_dollars` (DECIMAL 18,2) | `mrr` 12900 → `MrrUsd` 129.00 |
| Epoch → date key | `epoch_to_date_key` | joins to `dim_dates` |
| Epoch → timestamp | `epoch_to_timestamp` | dims carry readable timestamps |
| Currency code → key | `currency_to_key` | joins to `dim_currencies` |
| Address → conformed key | `address_to_key` + normalize street/city/state/postal/country | joins to shared `dim_addresses` |
| No NULLs | `default_values` / `default_values_measures` | text → 'Not Available', numbers → -4, booleans → false, dates → Unknown row |

- 4 non-restricted bsats: money, dates, currency (customers, subscriptions, plans, invoices).
- 3 restricted bsats: address keys and normalized address fields (customers, subscriptions, invoices).

**Why a separate business layer:** the raw vault stays an exact, auditable copy of the source. Rules can change and be re-run without touching history.

## 6. Gold — Star schema for reporting

| Table | Type | Grain | Main content |
|---|---|---|---|
| `chargebee_dim_customers` | Dim | 1 row per customer | AutoCollection, CreatedAt, Deleted, SalesforceAccountId, MrrUsd |
| `chargebee_dim_customers_restricted` | Dim (restricted) | 1 row per customer | Company, Email |
| `chargebee_dim_plans` | Dim | 1 row per plan | Name, Period, PeriodUnit, CurrencyCode, Status, PriceUsd |
| `chargebee_fact_subscriptions` | Fact | 1 row per subscription | Status, MrrUsd, PlanAmountUsd, date keys, CurrencyKey, ShippingAddressRestrictedKey; FKs CustomerKey, CustomerRestrictedKey, PlanKey (current plan via effsat) |
| `chargebee_fact_invoices` | Fact | 1 row per invoice | Status, TotalUsd, AmountPaidUsd, AmountDueUsd, date keys, CurrencyKey, Billing/ShippingAddressRestrictedKey; FKs CustomerKey, CustomerRestrictedKey, SubscriptionKey |

**Design rules applied**
- **Dims carry descriptive values; facts carry keys and measures** (house pattern, same as Zuora/GoCanvas and Nexus).
- **Restricted values in their own table** (`_restricted` dim). **Restricted keys** stay in the main tables but have `Restricted` in the column name, so the column-masking policy hides them (DODE-2030). `CustomerRestrictedKey` holds the same hash as `CustomerKey`; only the name differs.
- **No NULLs in gold**; missing FKs point to the Unknown sentinel row.
- **Shared, multi-tenant gold:** writes replace only this tenant's partition (`ETL_SourceSystem, ETL_Brand, ETL_Region`), so other brands are untouched.
- **Conformed dimensions:** `dim_dates`, `dim_currencies`, `dim_addresses` are shared with Salesforce, NetSuite, etc.

## 7. Data governance and security

- **SPI never extracted:** invoice `notes.note` (payment-method text, can include card info), `tax_origin.registration_number`.
- **No card or tax detail:** card_status, taxability, payment source ids not extracted.
- **PI/PII → restricted satellites → restricted gold table / Restricted-named keys.**
- **Masking:** column-level policy on `*Restricted*` columns; row-level security separates brands.
- **Schema changes:** all tables use Delta column mapping by name (DODE-2058), so columns can be renamed or dropped safely without reloading data.

## 8. Known limitations

1. **No Salesforce ↔ Chargebee link in the vault.** The framework can't create a link between hubs from two source systems. Today the join happens above gold on a plain column (`SalesforceAccountId`, from Chargebee custom field `cf_sf_accountid`). It works, but it's a text match with no history, no data-quality tracking, and every report has to re-implement it.
2. **Addons and coupons are loaded to silver but not modelled in gold** (no reporting requirement yet).
3. **Arrays not extracted** (line items, taxes, discounts) — invoice-line-level analysis isn't possible yet.

## 9. Proposed improvement — Conformed Customer/Account business hub

**Proposal.** Add a conformed **business hub** for account/customer, `bhub_account`, using the same pattern DODE-2165 just introduced for products (`bhub_product`).

**How it would work**
- `bhub_account` business key = the Salesforce Account Id.
  - Salesforce account → BK = its own Id.
  - Chargebee customer → BK = `cf_sf_accountid` (falls back to its own id when empty, so unmatched customers are still kept).
- The framework auto-creates **same-as links**: `link_sameas_bhub_account_salesforce` and `link_sameas_bhub_account_chargebee`, with an effectivity satellite on the Chargebee side because `cf_sf_accountid` can be edited.
- Gold facts get a conformed `AccountKey` through a 2-hop FK (`via:`), exactly like `AssetProductKey` in Nexus.

**Why it matters**
- One governed, reusable answer to "which Salesforce account is this Chargebee customer?" instead of every report joining on text.
- History: if someone re-points a Chargebee customer to a different SF account, the effectivity satellite shows when.
- Data quality becomes measurable: count of Chargebee customers with no/invalid SF account.
- Enables cross-system metrics, e.g. ARR per Salesforce account for SiteDocs.

**Trade-offs / ripple effects**
| Pro | Con / risk |
|---|---|
| Reuses an existing, reviewed framework pattern (no new code) | Depends on `cf_sf_accountid` quality — bad values create wrong matches |
| Removes duplicated join logic in reports | Adds tables (bhub, 2 same-as links, effsat) and a conformed dim to maintain |
| History + DQ metrics come for free | The bhub job must run after both SF and CB silver jobs (orchestration dependency) |
| Same approach can later add NetSuite customers | Needs agreement on the "golden" key (SF Account Id) with the data modeler |

**Next steps:** profile `cf_sf_accountid` (fill rate, how many match a real SF account), agree the key with the lead data modeler, then prototype in dev.

## 10. Operations and testing

- **Row-count parity:** `chargebee_row_count_check.py` compares bronze vs satellite vs bsat counts.
- **Idempotent loads:** hubs/links insert only new keys; satellites insert only changed rows (hash diff); gold overwrites only its partition.
- **Post-deployment scripts** for schema changes (DODE-2030 key renames, DODE-2058 column mapping) are one-time, re-runnable, dry-run first.

## 11. Glossary
- **Hub** — table of unique business keys for one entity.
- **Link** — table of relationships between hubs.
- **Satellite** — history of descriptive attributes for a hub or link.
- **Effectivity satellite** — history of when a relationship was active.
- **Business satellite (bsat)** — satellite with business rules applied.
- **Business hub (bhub)** — conformed hub that unifies the same entity across source systems.
- **Medallion** — bronze (raw) / silver (integrated) / gold (reporting).
- **Sentinel** — fixed "Unknown"/"Not Available" row/value used instead of NULL.

---

# PART 2 — POWERPOINT CONTENT (12 slides, ~30–40 minutes)

**Slide 1 — Title**
- Chargebee (SiteDocs): Data Vault & Medallion Design Walkthrough
- Name, team, date

**Slide 2 — Why Chargebee / what we need from it**
- SiteDocs subscription billing system
- Reporting needs: MRR/ARR, subscription movement, invoicing, link to Salesforce accounts
- Scope: 6 objects — customers, plans, subscriptions, invoices, addons, coupons
- *Speaker note:* set the business context before any tables.

**Slide 3 — End-to-end flow (diagram)**
- API → Bronze → Raw Vault → Business Vault → Gold (+ shared dims)
- 3 jobs: bronze extract, silver vault, gold
- *Visual:* left-to-right boxes from section 2.
- *Speaker note:* "Everything after the API call is YAML config on a shared framework."

**Slide 4 — Bronze: land it safely**
- 6 tables, daily incremental on `updated_at`
- Dates kept as epoch, money kept as cents — converted once, later
- No JSON, no SPI, DG-driven column list
- *Speaker note:* explain the epoch→NULL trap that made us keep bigint.

**Slide 5 — Hubs: the business things**
- 6 hubs keyed on Chargebee `id`
- Hash key = MD5(id + source system + brand + region)
- *Speaker note:* "Hubs never change — they're the anchor."

**Slide 6 — Links: how things relate**
- 4 links; table from section 4.2
- Highlight subscription→plan = pattern 1 + effectivity satellite
- *Speaker note:* "Plan change = MRR movement, so we need its history."

**Slide 7 — Satellites: history of attributes**
- 10 satellites; 3 restricted (company, email, phone, address)
- New row only when something changes (hash diff)
- *Visual:* customer hub with its normal + restricted satellite.

**Slide 8 — Business vault: rules applied once**
- cents→dollars, epoch→date key/timestamp, currency key, address key, no NULLs
- *Visual:* small before/after example (12900 → 129.00; 1788402637 → 20260903).

**Slide 9 — Gold star schema (diagram)**
- 2 facts (subscriptions, invoices), 3 dims + shared dates/currencies/addresses
- *Visual:* star diagram, facts in the centre.

**Slide 10 — Security by design**
- SPI never extracted
- Restricted values → restricted dim; restricted keys → `...RestrictedKey` masked by policy
- Row-level security by brand; column mapping enables safe schema changes

**Slide 11 — Proposed improvement: conformed account business hub**
- Problem: SF↔CB join is a text match done in every report
- Proposal: `bhub_account` + same-as links + effsat, same pattern as DODE-2165 products
- Benefits vs trade-offs (2-column table from section 9)
- *Speaker note:* "This reuses a framework pattern the team just reviewed — no new code."

**Slide 12 — Summary & next steps**
- What exists today works end to end and is fully config-driven
- Known gaps: SF link, addons/coupons not in gold, no line items
- Next: profile `cf_sf_accountid`, agree key with data modeler, prototype in dev
- Questions?

---

# PART 3 — Q&A PREP (likely questions)

- **Why Data Vault instead of loading straight to a star schema?** History and auditability; new sources/relationships added without redesign; business rules can be re-run without losing raw history.
- **Why is the plan link pattern 1 but the others pattern 3?** Only the subscription's plan can change; the other relationships are fixed once created.
- **Why convert money/dates in the business vault, not gold?** One place, one rule, every consumer gets the same answer; gold stays a simple selection layer.
- **Why are `CustomerKey` and `CustomerRestrictedKey` the same value?** Same hub hash; the name lets the masking policy hide the restricted one while keeping normal joins open.
- **What happens if Chargebee adds a field?** Add it to the extract YAML and the satellite YAML; the framework adds the column automatically (schema evolution).
- **How do we know loads are correct?** Row-count parity notebook; hash-diff loads; idempotent re-runs.

