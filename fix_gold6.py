import re

filepath = "databricks/gold/03_gold_pipeline.py"
with open(filepath, "r") as f:
    content = f.read()

# Fix 1: Remove duplicate 'location' in dim_employee
# The pattern is: col("location"),\n        col("is_active"),\n        col("location"),
# We need to remove the SECOND col("location") line
old = '''        col("location"),
        col("is_active"),
        col("location"),
        col("cost_center"),'''
new = '''        col("location"),
        col("is_active"),
        col("cost_center"),'''

count = content.count(old)
print(f"Fix 1 - duplicate location: found {count} occurrence(s)")
if count == 1:
    content = content.replace(old, new)
    print("  -> Fixed!")
elif count == 0:
    # Try with # MAGIC prefix
    old_magic = old.replace("        col", "# MAGIC         col").replace("        col", "# MAGIC         col")
    # Actually let's search line by line
    lines = content.split("\n")
    # Find dim_employee function, then find duplicate location
    in_dim_employee = False
    location_seen = False
    remove_idx = None
    for i, line in enumerate(lines):
        stripped = line.replace("# MAGIC ", "").strip()
        if "def gold_dim_employee" in line:
            in_dim_employee = True
            location_seen = False
        elif in_dim_employee and line.strip().startswith("def ") or (in_dim_employee and "# COMMAND" in line):
            in_dim_employee = False
            location_seen = False
        elif in_dim_employee and 'col("location")' in line:
            if location_seen:
                remove_idx = i
                print(f"  -> Found duplicate location at line {i+1}: {line.rstrip()}")
                break
            else:
                location_seen = True
    
    if remove_idx is not None:
        lines.pop(remove_idx)
        content = "\n".join(lines)
        print("  -> Fixed by removing line!")
    else:
        print("  -> WARNING: Could not find duplicate location")

with open(filepath, "w") as f:
    f.write(content)

print(f"\nFinal line count: {len(content.split(chr(10)))}")

# Verify no duplicate
lines = content.split("\n")
in_dim = False
loc_count = 0
for line in lines:
    if "def gold_dim_employee" in line:
        in_dim = True
    elif in_dim and ("# COMMAND" in line or (line.strip().startswith("def ") and "dim_employee" not in line)):
        break
    elif in_dim and 'col("location")' in line:
        loc_count += 1
print(f"location references in dim_employee: {loc_count}")
