with open('databricks/gold/03_gold_pipeline.py', encoding='utf-8') as f:
    lines = f.readlines()
cell = 0
cell_start = 1
for i, line in enumerate(lines, 1):
    if '# Databricks notebook source' in line:
        cell = 1
        cell_start = i
    elif '# COMMAND ----------' in line:
        cell += 1
        cell_start = i + 1
    if cell == 22:
        # print lines of cell 22
        pass

# Find cell 22 boundaries
cell = 0
cell_starts = []
for i, line in enumerate(lines, 1):
    if '# Databricks notebook source' in line:
        cell = 1
        cell_starts.append((1, i))
    elif '# COMMAND ----------' in line:
        cell += 1
        cell_starts.append((cell, i+1))

if len(cell_starts) >= 22:
    start = cell_starts[21][1]
    end = cell_starts[22][1] - 2 if len(cell_starts) > 22 else len(lines)
    print(f"Cell 22 starts at line {start}, ends at line {end}")
    for j in range(start-1, min(end, len(lines))):
        print(f"{j+1:4d}: {lines[j]}", end='')
else:
    print(f"Only {len(cell_starts)} cells found")
