import sys
f=open(r"databricks/gold/03_gold_pipeline.py","r")
lines=f.readlines()
f.close()
print("Total lines:",len(lines))
errs=[]
for i,l in enumerate(lines):
 s=l.strip().replace("# MAGIC ","").replace("# MAGIC","")
 if "col("transaction_id")" in s and "alias" not in s: errs.append("FIX10 FAIL line "+str(i+1))
 if "wbs_element" in s: errs.append("FIX10 FAIL wbs_element line "+str(i+1))
 if "variance_amount" in s: errs.append("FIX10 FAIL variance_amount line "+str(i+1))
 if s=="col("notes"),": errs.append("FIX11 FAIL line "+str(i+1))
 if "change_order_value" in s: errs.append("FIX13 FAIL line "+str(i+1))
 if "sales_rep_id" in s: errs.append("FIX14 FAIL sales_rep_id line "+str(i+1))
last5="".join(lines[-5:])
if "_gold_timestamp" not in last5: errs.append("FIX14 FAIL no _gold_timestamp at end")
if errs:
 for e in errs: print(e)
 print(len(errs),"issues")
else:
 print("ALL FIXES VERIFIED")
print("Last 3 lines:")
for i in range(max(0,len(lines)-3),len(lines)): print(i+1,repr(lines[i]))
