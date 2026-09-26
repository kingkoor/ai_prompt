**Don't run reconcile.** Incremental is the right mode. You changed only the bsat (plus `normalize.py`), and the bsat loader rebuilds from all current vault rows on every run. That means every GoCanvas contact and user got the new columns in this incremental run, not just the recently changed ones.

Reconcile is for bronze/vault problems, and we changed neither. A silver reconcile run on its own is also risky: it tombstones every record that hasn't changed in bronze in the last 7 days.

## What to check (dev)

Run these one at a time with `USE CATALOG bbdatawarehouse_dev;` first.

**1. Keys are filled and there are no duplicate current rows**
```sql
SELECT count(*)                                         AS current_rows,
       count(DISTINCT HubContactHashKey)                AS contacts,
       count(ContactMailingAddressKey)                  AS mailing_keys,
       count(ContactOtherAddressKey)                    AS other_keys,
       count_if(ContactMailingAddressKey = md5('Unknown')) AS mailing_unknown,
       count_if(ContactMailingStreet2Normalized = 'Not Available') AS street2_ok
FROM silver.salesforce_bsat_contacts_restricted_gocanvas_us
WHERE ETL_EndDate IS NULL;
```
Expect:
- `current_rows = contacts`: one current row per contact.
- `mailing_keys`, `other_keys` and `street2_ok` all equal `current_rows`.
- `mailing_unknown` to be large, since about 309K contacts have no address. That's normal.

**2. States are now codes**
```sql
SELECT ContactMailingCountryNormalized, ContactMailingStateNormalized, count(*) AS n
FROM silver.salesforce_bsat_contacts_restricted_gocanvas_us
WHERE ETL_EndDate IS NULL
GROUP BY ALL ORDER BY n DESC LIMIT 25;
```
Expect `US | TX`, `US | CA`, `US | FL`, `CA | ON`, `AU | NSW`, not "Texas" or blanks. Blank state with a blank country is fine.

**3. Users, same checks**
```sql
SELECT count(*) AS current_rows, count(DISTINCT HubUserHashKey) AS users,
       count(UserAddressKey) AS keys, count_if(UserAddressKey = md5('Unknown')) AS unknown
FROM silver.salesforce_bsat_users_restricted_gocanvas_us
WHERE ETL_EndDate IS NULL;
```

**4. One-time version bump (just to understand the numbers)**
```sql
SELECT count(*) AS all_rows, count_if(ETL_EndDate IS NULL) AS current_rows
FROM silver.salesforce_bsat_contacts_restricted_gocanvas_us;
```
`all_rows` is about double the old count. That's expected: every contact got a new version because of the new columns, and it happens only this once.

## If something looks wrong

| You see | Likely cause |
|---|---|
| Keys NULL | The loader.py `f.lit("")` fix isn't typed, or the job used an old library build |
| States still "TEXAS" or blank for US | The `normalize.py` change isn't picked up. Check that the job's framework library was rebuilt/redeployed |
| `current_rows > contacts` | Duplicate current versions. Send me a photo |
| Job error on `literal` | `literal` isn't registered in work `transformations.py` (the earlier spot-check) |

Once 1–3 look right, run the gold job for GoCanvas with `dim_addresses_config_path` set to the GoCanvas config. Don't run it at the same time as Nexus.
