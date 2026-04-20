import os

filepath = 'databricks/gold/03_gold_pipeline.py'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# The file ends with col("currency"), and is missing the closing
# Need to add remaining columns and close the select + function
# Silver sales_orders has: delivery_terms, description, created_date, order_value, total_value, _silver_timestamp
# We add delivery_terms and _silver_timestamp as load_timestamp

append_lines = '''        col("delivery_terms"),
        col("_silver_timestamp").alias("load_timestamp")
    )
'''

if content.rstrip().endswith('col("currency"),'):
    content = content.rstrip() + '\n' + append_lines
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    print("FIXED: appended closing to fact_sales")
else:
    print(f"File ends with: {content.rstrip()[-50:]}")
    print("NOT fixing - unexpected ending")
