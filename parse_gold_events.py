import json
with open('gold_events.json') as f:
    data = json.load(f)
events = data if isinstance(data, list) else data.get('events', data.get('value', []))
# Show ERROR and FLOW_PROGRESS events
for e in events[:50]:
    etype = e.get('event_type','')
    level = e.get('level','')
    ts = e.get('timestamp','')
    msg = e.get('message','')
    details = e.get('details', {})
    error = e.get('error', {})
    if level in ('ERROR','WARN') or 'error' in etype.lower() or 'fail' in msg.lower():
        print(f"[{ts}] {level} {etype}: {msg[:300]}")
        if error:
            print(f"  error: {json.dumps(error)[:300]}")
        if details:
            print(f"  details: {json.dumps(details)[:300]}")
        print()
