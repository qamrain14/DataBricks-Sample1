import json, re

# Load CSV headers
headers = json.load(open('csv_headers.json'))

# Read silver pipeline
with open('databricks/silver/02_silver_pipeline.py', 'r') as f:
    content = f.read()

# Find all expect_or_drop constraints
expects = re.findall(r'expect_or_drop\("([^"]+)",\s*"([^"]+)"\)', content)

# Find which table each expect belongs to
lines = content.split('\n')
for i, line in enumerate(lines):
    if 'expect_or_drop' in line:
        # Find the table name above
        for j in range(i-1, max(i-10, 0), -1):
            m = re.search(r'name="silver_(\w+)"', lines[j])
            if m:
                table_name = m.group(1)
                # Extract column from expect
                em = re.search(r'expect_or_drop\("[^"]+",\s*"(\w+)\s+IS NOT NULL"\)', line)
                if em:
                    col = em.group(1)
                    csv_cols = headers.get(table_name, [])
                    if csv_cols and col not in csv_cols:
                        print(f"MISMATCH: silver_{table_name} expects '{col}' but CSV has: {csv_cols[:8]}")
                    else:
                        print(f"OK: silver_{table_name} column '{col}' exists")
                break
