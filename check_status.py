import json, subprocess, sys
run_id = sys.argv[1] if len(sys.argv) > 1 else "42250884504268"
cmd = f'cmd /c "set PATH=%LOCALAPPDATA%\\Microsoft\\WinGet\\Links;%PATH% && databricks jobs get-run {run_id} --profile cli-auth 2>&1"'
result = subprocess.run(cmd, capture_output=True, text=True, shell=True, cwd=r"c:\Users\mohdq\src\DataBricks-Sample1")
try:
    data = json.loads(result.stdout)
except:
    print("ERROR parsing JSON:", result.stdout[:500])
    sys.exit(1)
print(f"Job state: {data['state']['life_cycle_state']} {data['state'].get('result_state','')}")
for t in data.get("tasks", []):
    s = t["state"]
    lcs = s["life_cycle_state"]
    rs = s.get("result_state", "")
    msg = s.get("state_message", "")[:60]
    print(f"  {t['task_key']:40s} {lcs:12s} {rs:10s} {msg}")
