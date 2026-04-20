import json
d = json.load(open('run_status5.json'))
tasks = d.get('tasks', [])
for t in tasks:
    s = t.get('state', {})
    lc = s.get('life_cycle_state', '?')
    rs = s.get('result_state', '?')
    print(f"{t['task_key']}: {lc} / {rs}")
print()
print("Overall:", d.get('state', {}).get('life_cycle_state', '?'), '/', d.get('state', {}).get('result_state', '?'))
