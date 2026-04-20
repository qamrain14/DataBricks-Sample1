import json
data = json.load(open("silver_ev2.json"))
for e in data:
    uid = e.get("origin", {}).get("update_id", "")
    if uid and uid.startswith("ae184edb"):
        print(json.dumps(e, indent=2)[:500])
        print("---")
