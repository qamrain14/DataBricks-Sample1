import ast

with open('databricks/gold/03_gold_pipeline.py', 'r') as f:
    content = f.read()

lines = content.split('\n')
cells = []
current = []
start = 1

for i, line in enumerate(lines, 1):
    stripped = line.strip()
    if stripped == '# COMMAND ----------':
        if current:
            cells.append((start, current))
        current = []
        start = i + 1
    elif stripped == '# Databricks notebook source':
        start = i + 1
    elif line.startswith('# MAGIC'):
        pass  # skip magic lines entirely
    else:
        current.append(line)

if current:
    cells.append((start, current))

print(f"Total cells: {len(cells)}")

errors = 0
for idx, (fline, clines) in enumerate(cells):
    code = '\n'.join(clines).strip()
    if not code:
        continue
    try:
        ast.parse(code)
    except SyntaxError as e:
        errors += 1
        print(f"SYNTAX ERROR in cell {idx+1} (starts file line {fline}):")
        print(f"  Line {e.lineno}: {e.msg}")
        if e.text:
            print(f"  Text: {e.text.rstrip()}")

if errors == 0:
    print("No syntax errors found!")
