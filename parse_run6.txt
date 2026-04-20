import json
d = json.load(open("run6_status.json"))
tasks = d.get("tasks", [])
for t in tasks:
    name = t.get("task_key", "")
    state = t.get("state", {})
    rs = state.get("result_state", "?")
    ls = state.get("life_cycle_state", "?")
    print(f"{name}: {ls} / {rs}")
