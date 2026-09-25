USE CATALOG bbdatawarehouse_dev;

WITH s AS (
  SELECT 'nexus account billing' AS src, AccountBillingStateNormalized AS st, AccountBillingCountryNormalized AS ctry FROM silver.salesforce_sat_accounts_restricted_bluebeam_us
  UNION ALL SELECT 'nexus account shipping', AccountShippingStateNormalized, AccountShippingCountryNormalized FROM silver.salesforce_sat_accounts_restricted_bluebeam_us
  UNION ALL SELECT 'nexus order billing', OrderBillingStateNormalized, OrderBillingCountryNormalized FROM silver.salesforce_sat_orders_restricted_bluebeam_us
  UNION ALL SELECT 'nexus contact mailing', ContactMailingStateCode, ContactMailingCountryCode FROM silver.salesforce_sat_contacts_restricted_bluebeam_us
  UNION ALL SELECT 'chargebee customer', CustomerBillingAddressStateCode, CustomerBillingAddressCountry FROM silver.chargebee_sat_customers_restricted_sitedocs_us
  UNION ALL SELECT 'chargebee subscription', SubscriptionShippingAddressStateCode, SubscriptionShippingAddressCountry FROM silver.chargebee_sat_subscriptions_restricted_sitedocs_us
  UNION ALL SELECT 'chargebee invoice billing', InvoiceBillingAddressStateCode, InvoiceBillingAddressCountry FROM silver.chargebee_sat_invoices_restricted_sitedocs_us
  UNION ALL SELECT 'netsuite address', AddressState, AddressCountry FROM silver.netsuite_sat_addresses_restricted_bluebeam_us
)
SELECT src,
  CASE WHEN st IS NULL OR trim(st) = '' THEN 'blank'
       WHEN upper(trim(st)) RLIKE '^[A-Z]{2,3}$' THEN 'code' ELSE 'name' END AS state_kind,
  CASE WHEN ctry IS NULL OR trim(ctry) = '' THEN 'blank'
       WHEN upper(trim(ctry)) RLIKE '^[A-Z]{2}$' THEN 'code' ELSE 'name' END AS country_kind,
  count(*) AS n
FROM s
GROUP BY ALL
ORDER BY src, n DESC;

SELECT src, st, ctry, count(*) AS n
FROM s
WHERE trim(st) <> '' AND NOT upper(trim(st)) RLIKE '^[A-Z]{2,3}$'
GROUP BY ALL
ORDER BY n DESC
LIMIT 40;


GoCanvas Salesforce has no state or country code fields (picklists are off). States are stored as full names ("Texas", "Ontario"), so GoCanvas states come out blank and its AddressKeys won't match Nexus or Chargebee for the same address.
I'd like to change normalize_state_province to convert the country to a code first and map full US, Canadian and Australian state names to codes. Values that are already codes pass through, so Nexus and Chargebee keys don't change.
Does NetSuite send state names or codes? If it sends names, its keys would change.
