import json
d = json.load(open('run_status5.json'))
for t in d.get('tasks', []):
    if t['task_key'] == 'run_gold_pipeline':
        s = t.get('state', {})
        print("State:", s)
        print()
        # Get pipeline task details
        pt = t.get('pipeline_task', {})
        print("Pipeline task:", pt)
        print()
        # Check for error info
        for k in ['state', 'run_page_url', 'attempt_number']:
            if k in t:
                print(f"{k}: {t[k]}")
        break
