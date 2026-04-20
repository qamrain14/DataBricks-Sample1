import ast, re

with open('databricks/gold/03_gold_pipeline.py', 'r') as f:
    content = f.read()

# Split into cells
lines = content.split('\n')
cells = []
current_cell = []
cell_start_line = 1

for i, line in enumerate(lines, 1):
    if line.strip() == '# COMMAND ----------':
        if current_cell:
            cells.append((cell_start_line, current_cell))
        current_cell = []
        cell_start_line = i + 1
    elif line.strip() == '# Databricks notebook source':
        cell_start_line = i + 1
        continue
    elif line.startswith('# MAGIC'):
        continue  # skip magic lines
    else:
        current_cell.append(line)

if current_cell:
    cells.append((cell_start_line, current_cell))

print(f"Total cells: {len(cells)}")
print()

for idx, (start_line, cell_lines) in enumerate(cells):
    code = '\n'.join(cell_lines).strip()
    if not code:
        continue
    try:
        ast.parse(code)
    except SyntaxError as e:
        print(f"Cell {idx+1} (file line {start_line}): SyntaxError at line {e.lineno}: {e.msg}")
        print(f"  Offending line: {e.text}")
        # Show context
        code_lines = code.split('\n')
        start = max(0, e.lineno - 3)
        end = min(len(code_lines), e.lineno + 2)
        for j in range(start, end):
            marker = ">>>" if j == e.lineno - 1 else "   "
            print(f"  {marker} {j+1}: {code_lines[j]}")
        print()
