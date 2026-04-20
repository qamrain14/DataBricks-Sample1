import json
data = json.load(open("silver_ev2.json"))
# Show all unique update_ids and their final status
updates = {}
for e in data:
    uid = e.get("origin", {}).get("update_id", "")
    if uid:
        if uid not in updates:
            updates[uid] = []
        updates[uid].append((e.get("level",""), e.get("message","")[:200]))

for uid, evts in updates.items():
    print(f"\n=== Update {uid[:12]} ===")
    for lvl, msg in evts[:5]:
        print(f"  {lvl}: {msg}")
