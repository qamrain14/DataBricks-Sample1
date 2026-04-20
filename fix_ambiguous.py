"""Fix ambiguous project_id in fact_goods_receipts by removing project_id from po subselect."""
import re

filepath = "databricks/gold/03_gold_pipeline.py"

with open(filepath, "r") as f:
    content = f.read()

old = '''col("po_id").alias("_po_id"), col("project_id")'''
new = '''col("po_id").alias("_po_id")'''

count = content.count(old)
print(f"Found {count} occurrence(s) of target string")

if count == 1:
    content = content.replace(old, new)
    with open(filepath, "w") as f:
        f.write(content)
    print("Fix applied successfully")
    # Verify
    with open(filepath, "r") as f:
        verify = f.read()
    if old not in verify and new in verify:
        print("Verification PASSED")
    else:
        print("Verification FAILED")
else:
    print(f"ERROR: Expected 1 occurrence, found {count}")
