"""Fix 3 gold pipeline column errors + duplicate column names."""
import re

path = r"databricks/gold/03_gold_pipeline.py"
with open(path, "r", encoding="utf-8") as f:
    lines = f.readlines()

orig_count = len(lines)
fixes = []

# Build list of (line_index, action) pairs
removals = set()
replacements = {}

for i, line in enumerate(lines):
    stripped = line.lstrip("# MAGIC ").rstrip("\n").strip()

    # --- dim_contract fixes ---
    # 1. Remove standalone col("change_order_count"), (the next line creates it correctly)
    if 'col("change_order_count"),' == stripped and i > 0:
        # Check next line has the alias version
        next_stripped = lines[i+1].lstrip("# MAGIC ").rstrip("\n").strip() if i+1 < len(lines) else ""
        if 'variation_orders' in next_stripped and 'change_order_count' in next_stripped:
            removals.add(i)
            fixes.append(f"Line {i+1}: Removed standalone col('change_order_count') (doesn't exist in silver)")

    # 2. Remove duplicate col("original_value"), (already created as alias on previous line)
    if 'col("original_value"),' == stripped:
        # Check if previous line already creates original_value as alias
        prev_stripped = lines[i-1].lstrip("# MAGIC ").rstrip("\n").strip() if i > 0 else ""
        if '.alias("original_value")' in prev_stripped:
            removals.add(i)
            fixes.append(f"Line {i+1}: Removed duplicate col('original_value') (already aliased above)")

    # 3. Remove second col("payment_terms"), duplicate
    if 'col("payment_terms"),' == stripped:
        # Check if there's already a payment_terms within ~5 lines above
        for j in range(max(0, i-5), i):
            prev_s = lines[j].lstrip("# MAGIC ").rstrip("\n").strip()
            if 'col("payment_terms")' in prev_s:
                removals.add(i)
                fixes.append(f"Line {i+1}: Removed duplicate col('payment_terms')")
                break

    # --- fact_invoices fix ---
    # 4. Remove standalone col("invoice_number"), (doesn't exist, next line creates it from invoice_ref)
    if 'col("invoice_number"),' == stripped:
        next_stripped = lines[i+1].lstrip("# MAGIC ").rstrip("\n").strip() if i+1 < len(lines) else ""
        if 'invoice_ref' in next_stripped and 'invoice_number' in next_stripped:
            removals.add(i)
            fixes.append(f"Line {i+1}: Removed standalone col('invoice_number') (doesn't exist, use invoice_ref alias)")

    # --- fact_goods_receipts fix ---
    # 5. Change col("line_item_id") to col("line_id") in po_line_items select
    if 'col("line_item_id").alias("_poli_line_id")' in stripped:
        old = 'col("line_item_id")'
        new = 'col("line_id")'
        replacements[i] = (old, new)
        fixes.append(f"Line {i+1}: Changed col('line_item_id') to col('line_id') in po_line_items select")

# Apply fixes
new_lines = []
for i, line in enumerate(lines):
    if i in removals:
        continue
    if i in replacements:
        old, new = replacements[i]
        line = line.replace(old, new)
    new_lines.append(line)

with open(path, "w", encoding="utf-8") as f:
    f.writelines(new_lines)

print(f"Original: {orig_count} lines, New: {len(new_lines)} lines")
print(f"Applied {len(fixes)} fixes:")
for fix in fixes:
    print(f"  - {fix}")
