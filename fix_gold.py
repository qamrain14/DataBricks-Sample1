"""
Fix gold pipeline: strip # MAGIC prefix from code cells (same pattern as fix_silver3.py).
Also fix known column reference issues.
"""
import os

filepath = 'databricks/gold/03_gold_pipeline.py'

with open(filepath, 'r', encoding='utf-8') as f:
    lines = f.readlines()

print(f"Original file: {len(lines)} lines")

# Show first 5 raw lines for diagnostic
for i in range(min(5, len(lines))):
    print(f"  RAW {i+1}: {repr(lines[i][:80])}")

result = []
in_markdown = False
stats = {"commands": 0, "magic_stripped": 0, "magic_kept": 0, "blank_fixed": 0}

for i, line in enumerate(lines):
    # First line is the notebook header — keep as-is
    if i == 0:
        result.append(line)
        continue

    stripped = line.strip()

    # --- COMMAND separator ---
    if stripped in ('# MAGIC # COMMAND ----------', '# COMMAND ----------'):
        result.append('# COMMAND ----------\n')
        in_markdown = False
        stats["commands"] += 1
        continue

    # --- Blank line (# MAGIC with nothing, or truly empty) ---
    if stripped in ('# MAGIC', ''):
        result.append('\n')
        stats["blank_fixed"] += 1
        continue

    # --- Markdown cell start ---
    if stripped in ('# MAGIC %md', '%md', '# MAGIC # MAGIC %md'):
        result.append('# MAGIC %md\n')
        in_markdown = True
        stats["magic_kept"] += 1
        continue

    # --- Inside markdown cell: ensure # MAGIC prefix ---
    if in_markdown:
        if line.startswith('# MAGIC '):
            result.append(line)
        else:
            result.append('# MAGIC ' + line)
        stats["magic_kept"] += 1
        continue

    # --- Code cell: strip # MAGIC prefix ---
    if line.startswith('# MAGIC '):
        result.append(line[len('# MAGIC '):])
        stats["magic_stripped"] += 1
    else:
        result.append(line)

# Write the fixed file
with open(filepath, 'w', encoding='utf-8') as f:
    f.writelines(result)

print(f"\nFixed file: {len(result)} lines")
print(f"Stats: {stats}")

# ---- VERIFICATION ----
with open(filepath, 'r', encoding='utf-8') as f:
    new_lines = f.readlines()

cmd_count = sum(1 for l in new_lines if l.strip() == '# COMMAND ----------')
dlt_in_code = [i+1 for i, l in enumerate(new_lines) if '@dlt.table' in l and not l.startswith('# MAGIC')]
dlt_in_magic = [i+1 for i, l in enumerate(new_lines) if '@dlt.table' in l and l.startswith('# MAGIC')]

# Count lines with # MAGIC that are NOT in markdown cells
bad_magic = []
in_md = False
for i, l in enumerate(new_lines):
    s = l.strip()
    if s == '# COMMAND ----------':
        in_md = False
        continue
    if s == '# MAGIC %md':
        in_md = True
        continue
    if l.startswith('# MAGIC ') and not in_md:
        bad_magic.append(i+1)

print(f"\nCOMMAND separators: {cmd_count}")
print(f"@dlt.table in code cells: {len(dlt_in_code)} at lines {dlt_in_code}")
print(f"@dlt.table still behind MAGIC: {len(dlt_in_magic)} at lines {dlt_in_magic}")
print(f"Lines with # MAGIC in non-md context: {len(bad_magic)}")
if bad_magic:
    print(f"  First 10 bad lines: {bad_magic[:10]}")
    for ln in bad_magic[:5]:
        print(f"  Line {ln}: {repr(new_lines[ln-1][:80])}")

# Show first 30 lines
print("\n--- First 30 lines ---")
for i in range(min(30, len(new_lines))):
    print(f"{i+1}: {repr(new_lines[i][:80])}")

# Show lines around first @dlt.table
if dlt_in_code:
    first = dlt_in_code[0]
    print(f"\n--- Lines around first @dlt.table (line {first}) ---")
    start = max(0, first - 3)
    end = min(len(new_lines), first + 5)
    for i in range(start, end):
        print(f"{i+1}: {repr(new_lines[i][:80])}")

print("\nDone!")
