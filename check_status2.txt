import json
with open("run_status.json") as f:
    data = json.load(f)
state = data.get("state", {})
print(f"Run state: {state.get('life_cycle_state')} / {state.get('result_state', 'N/A')}")
tasks = data.get("tasks", [])
summary = {}
for t in tasks:
    ts = t.get("state", {})
    lcs = ts.get("life_cycle_state", "UNKNOWN")
    rs = ts.get("result_state", "")
    status = rs if rs else lcs
    summary[status] = summary.get(status, 0) + 1
    if rs and rs != "SUCCEEDED":
        print(f"  FAILED: {t['task_key']} -> {rs}")
print(f"Task summary: {summary}")
print(f"Total tasks: {len(tasks)}")
