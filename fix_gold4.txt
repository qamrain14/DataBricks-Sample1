"""Fix gold_dim_project: remove wrong column references and duplicates."""
from pathlib import Path

gold_path = Path("databricks/gold/03_gold_pipeline.py")
lines = gold_path.read_text(encoding="utf-8").splitlines()

# Lines to remove (0-indexed): these are the problematic lines in gold_dim_project
# We match by content rather than line numbers for safety
removals = [
    '        col("department").alias("sector"),',
    '        col("project_manager_id").alias("project_manager"),',
]

# For duplicates, we need to track and remove second occurrences
dup_tracker = {}
dup_lines = [
    '        col("start_date"),',
    '        col("planned_completion_date").alias("end_date"),',
]

# project_duration_days doesn't exist in silver - remove this ref
extra_removals = [
    '        col("project_duration_days"),',
]

new_lines = []
removed = []
in_dim_project = False

for i, line in enumerate(lines):
    stripped = line.rstrip()
    
    # Track which gold function we're in
    if "def gold_dim_project()" in line:
        in_dim_project = True
    elif line.startswith("def gold_") or line.startswith("# COMMAND"):
        in_dim_project = False
        dup_tracker.clear()
    
    if in_dim_project:
        # Remove wrong column references
        if stripped in removals:
            removed.append(f"  Line {i+1}: {stripped}")
            continue
        
        # Remove extra removals
        if stripped in extra_removals:
            removed.append(f"  Line {i+1}: {stripped}")
            continue
        
        # Track duplicates - keep first, remove second
        if stripped in dup_lines:
            if stripped in dup_tracker:
                removed.append(f"  Line {i+1} (dup): {stripped}")
                continue
            dup_tracker[stripped] = True
    
    new_lines.append(line)

gold_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")

print(f"Removed {len(removed)} lines:")
for r in removed:
    print(r)
print(f"\nTotal output lines: {len(new_lines)}")

# Verify no issues remain
import re
content = "\n".join(new_lines)
blocks = re.split(r"^@dlt\.table", content, flags=re.MULTILINE)
for block in blocks[1:]:
    fn_m = re.search(r"def (gold_dim_project)\(\)", block)
    if fn_m:
        cols = re.findall(r'col\("([^"]+)"\)', block)
        print(f"\ngold_dim_project col() refs: {cols}")
        bad = [c for c in cols if c in ("department", "project_manager_id", "project_duration_days")]
        if bad:
            print(f"STILL BAD: {bad}")
        else:
            print("ALL CLEAN - no bad column references!")
        break
