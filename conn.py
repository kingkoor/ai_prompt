SELECT count(*) total,
       count_if(ContactLastActivityDateKey IS NULL) lastactivity_null,
       count_if(ContactCreatedDateKey IS NULL)      created_null,
       count_if(ContactLastModifiedDateKey IS NULL) modified_null,
       count_if(ContactSystemModstampKey IS NULL)   modstamp_null
FROM bbdatawarehouse_prod.gold.salesforce_fact_contacts
WHERE ETL_Brand = 'GoCanvas';

DESCRIBE TABLE bbdatawarehouse_prod.gold.salesforce_fact_contacts;
