import subprocess, json, sys

RUN_ID = "266642807125415"
cmd = f'cmd /c "set PATH=%LOCALAPPDATA%\\Microsoft\\WinGet\\Links;%PATH% && databricks jobs get-run {RUN_ID} --profile cli-auth 2>&1"'
result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
raw = result.stdout.strip()

try:
    data = json.loads(raw)
except:
    print("RAW OUTPUT:")
    print(raw[:2000])
    sys.exit(1)

state = data.get("state", {})
life_cycle = state.get("life_cycle_state", "UNKNOWN")
result_state = state.get("result_state", "")
print(f"Run {RUN_ID}: lifecycle={life_cycle}, result={result_state}")

tasks = data.get("tasks", [])
summary = {"SUCCEEDED": 0, "FAILED": 0, "RUNNING": 0, "PENDING": 0, "OTHER": 0}
failed_tasks = []
for t in tasks:
    ts = t.get("state", {})
    tl = ts.get("life_cycle_state", "UNKNOWN")
    tr = ts.get("result_state", "")
    name = t.get("task_key", "?")
    if tr == "SUCCESS":
        summary["SUCCEEDED"] += 1
    elif tr == "FAILED":
        summary["FAILED"] += 1
        failed_tasks.append(name)
    elif tl in ("RUNNING",):
        summary["RUNNING"] += 1
    elif tl in ("PENDING", "BLOCKED", "WAITING_FOR_RETRY"):
        summary["PENDING"] += 1
    else:
        summary["OTHER"] += 1

print(f"Tasks: {len(tasks)} total")
for k, v in summary.items():
    if v > 0:
        print(f"  {k}: {v}")
if failed_tasks:
    print(f"Failed: {', '.join(failed_tasks)}")
