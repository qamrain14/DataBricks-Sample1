"""Fix gold pipeline issues:
1. Remove stray orphaned 'name=' lines between cell separators and @dlt.table
2. Remove duplicate/wrong column references (old col that was already fixed but left orphan)
"""
import re

path = r"databricks\gold\03_gold_pipeline.py"

with open(path, "r", encoding="utf-8") as f:
    lines = f.readlines()

print(f"Input lines: {len(lines)}")

# --- Fix 1: Remove stray orphan lines ---
# Pattern: line after "# COMMAND ----------" that has just '    name="...",\n'
# These are orphans from prior edits
out = []
i = 0
removed_orphans = 0
while i < len(lines):
    line = lines[i]
    stripped = line.strip()

    # If this is a cell separator, peek at next line
    if stripped == "# COMMAND ----------":
        out.append(line)
        i += 1
        # Skip orphan name= lines right after separator
        while i < len(lines):
            nxt = lines[i].strip()
            if nxt.startswith('name=') and nxt.endswith('",'):
                print(f"  Removing orphan line {i+1}: {nxt}")
                removed_orphans += 1
                i += 1
            else:
                break
        continue
    else:
        out.append(line)
        i += 1

print(f"Removed {removed_orphans} orphan lines")
lines = out

# --- Fix 2: Remove duplicate/wrong column references ---
# Build text from lines
text = "".join(lines)

# 2a. fact_goods_receipts: remove col("inspector_id").alias("inspector")
# Silver has received_by, not inspector_id. Keep the received_by line.
old = '        col("inspector_id").alias("inspector"),\n        col("received_by").alias("inspector"),'
new = '        col("received_by").alias("inspector"),'
if old in text:
    text = text.replace(old, new, 1)
    print("Fixed: removed inspector_id from fact_goods_receipts")
else:
    # Try just removing the inspector_id line
    old2 = '        col("inspector_id").alias("inspector"),\n'
    if old2 in text:
        text = text.replace(old2, '', 1)
        print("Fixed: removed inspector_id line from fact_goods_receipts")
    else:
        print("WARN: inspector_id pattern not found in fact_goods_receipts")

# 2b. fact_project_actuals: remove col("actual_id").alias("transaction_id")
# Silver has transaction_id, not actual_id
old = '        col("actual_id").alias("transaction_id"),\n        col("transaction_id"),'
new = '        col("transaction_id"),'
if old in text:
    text = text.replace(old, new, 1)
    print("Fixed: removed actual_id from fact_project_actuals")
else:
    old2 = '        col("actual_id").alias("transaction_id"),\n'
    if old2 in text:
        text = text.replace(old2, '', 1)
        print("Fixed: removed actual_id line from fact_project_actuals")
    else:
        print("WARN: actual_id pattern not found in fact_project_actuals")

# 2c. fact_inventory: remove col("created_by").alias("posted_by")
# Silver has posted_by, not created_by
old = '        col("created_by").alias("posted_by"),\n        col("posted_by"),'
new = '        col("posted_by"),'
if old in text:
    text = text.replace(old, new, 1)
    print("Fixed: removed created_by from fact_inventory")
else:
    old2 = '        col("created_by").alias("posted_by"),\n'
    if old2 in text:
        text = text.replace(old2, '', 1)
        print("Fixed: removed created_by line from fact_inventory")
    else:
        print("WARN: created_by pattern not found in fact_inventory")

# 2d. fact_sales: remove col("order_id").alias("sales_order_id")
# Silver has sales_order_id, not order_id
old = '        col("order_id").alias("sales_order_id"),\n        col("sales_order_id"),'
new = '        col("sales_order_id"),'
if old in text:
    text = text.replace(old, new, 1)
    print("Fixed: removed order_id from fact_sales")
else:
    old2 = '        col("order_id").alias("sales_order_id"),\n'
    if old2 in text:
        text = text.replace(old2, '', 1)
        print("Fixed: removed order_id line from fact_sales")
    else:
        print("WARN: order_id pattern not found in fact_sales")

with open(path, "w", encoding="utf-8") as f:
    f.write(text)

# --- Verify ---
with open(path, "r", encoding="utf-8") as f:
    final = f.readlines()

issues = []
dlt_count = 0
for idx, ln in enumerate(final):
    s = ln.strip()
    if "@dlt.table" in s:
        dlt_count += 1
    if s.startswith("# MAGIC") and ("import " in s or "@dlt" in s or "def " in s):
        issues.append(f"  Line {idx+1}: {s}")
    if 'inspector_id' in s:
        issues.append(f"  Line {idx+1}: still has inspector_id: {s}")
    if 'actual_id' in s:
        issues.append(f"  Line {idx+1}: still has actual_id: {s}")
    if '"created_by"' in s and 'posted_by' in s:
        issues.append(f"  Line {idx+1}: still has created_by->posted_by: {s}")
    if '"order_id"' in s and 'sales_order_id' in s:
        issues.append(f"  Line {idx+1}: still has order_id->sales_order_id: {s}")

print(f"\nTotal output lines: {len(final)}")
print(f"@dlt.table decorators: {dlt_count}")
if issues:
    print(f"REMAINING ISSUES ({len(issues)}):")
    for iss in issues:
        print(iss)
else:
    print("ALL CLEAN - no issues found!")
