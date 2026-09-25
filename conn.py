SELECT 'contact mailing' AS src, ContactMailingCountry AS country, ContactMailingState AS state, count(*) AS n
FROM bbdatawarehouse_dev.silver.salesforce_sat_contacts_restricted_gocanvas_us GROUP BY ALL
UNION ALL
SELECT 'user', UserCountry, UserState, count(*)
FROM bbdatawarehouse_dev.silver.salesforce_sat_users_restricted_gocanvas_us GROUP BY ALL
ORDER BY n DESC LIMIT 60;
