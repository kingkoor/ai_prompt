from sql_connectors import get_salesforce_object

for env in ["prod_nexus", "prod_gocanvas"]:
    for obj in ["Contact", "User"]:
        meta = get_salesforce_object(obj, environment=env)
        names = [f["name"] for f in meta["fields"]]
        print(env, obj, [n for n in names if n.endswith("StateCode") or n.endswith("CountryCode")])
