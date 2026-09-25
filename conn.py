from sql_connectors import get_salesforce_object

for obj in ["Contact", "User"]:
    meta = get_salesforce_object(obj, environment="prod_gocanvas")
    for f in meta["fields"]:
        if "state" in f["name"].lower() or "country" in f["name"].lower():
            print(obj, f["name"], "|", f["label"], "|", f["type"])

I checked the GoCanvas org with get_salesforce_object. Contact and User have no StateCode or CountryCode fields; Nexus has them. It looks like State/Country picklists aren't turned on in GoCanvas, so there's nothing to extract.
Turning them on is a GoCanvas Salesforce admin change that converts all existing addresses. Should we ask the GoCanvas admin, or go with the function change for now?

from sql_connectors import get_salesforce_object

for obj in ["Contact", "User"]:
    meta = get_salesforce_object(obj, environment="prod_gocanvas")
    for f in meta["fields"]:
        text = (f["name"] + " " + f["label"]).lower()
        if "state" in text or "country" in text or "province" in text:
            print(obj, f["name"], "|", f["label"], "|", f["type"])
