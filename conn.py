from sql_connectors import get_salesforce_object

meta = get_salesforce_object("Contact", environment="prod_gocanvas")
check = ["State__c", "Person_State__c", "Person_Country__c", "Infer_Country__c",
         "mkto71_Inferred_Country__c", "mkto71_Inferred_State_Region__c"]
for f in meta["fields"]:
    if f["name"] in check:
        print(f["name"], "| formula:", f["calculatedFormula"], "| help:", f["inlineHelpText"])

from sql_connectors import get_salesforce_object

meta = get_salesforce_object("Contact", environment="prod_gocanvas")
check = ["State__c", "Person_State__c", "Person_Country__c", "Infer_Country__c",
         "mkto71_Inferred_Country__c", "mkto71_Inferred_State_Region__c"]
for f in meta["fields"]:
    if f["name"] in check:
        print(f["name"], "| formula:", f["calculatedFormula"], "| help:", f["inlineHelpText"])
