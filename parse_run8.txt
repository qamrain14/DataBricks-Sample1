import json, sys
try:
    with open('run8.json') as f:
        data = json.load(f)
    state = data.get('state', {})
    print(f"Life cycle: {state.get('life_cycle_state')}")
    print(f"Result: {state.get('result_state')}")
    print(f"State message: {state.get('state_message','')[:200]}")
    tasks = data.get('tasks', [])
    print(f"\nTasks ({len(tasks)}):")
    for t in tasks:
        ts = t.get('state', {})
        print(f"  {t.get('task_key','?'):40s} {ts.get('life_cycle_state','?'):15s} {ts.get('result_state','')}")
except Exception as e:
    print(f"Error: {e}")
    with open('run8.json') as f:
        print(f.read()[:500])
