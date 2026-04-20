"""Fix gold pipeline: strip # MAGIC from Python code lines, keep for markdown."""
import re

path = r"databricks\gold\03_gold_pipeline.py"

with open(path, "r", encoding="utf-8") as f:
    raw = f.read()

lines = raw.split("\n")
out = []
in_markdown = False

for i, line in enumerate(lines):
    stripped = line.strip()

    # Keep the very first line (notebook header)
    if i == 0 and stripped == "# Databricks notebook source":
        out.append(line)
        continue

    # Cell separator - never has # MAGIC, keep as-is
    if stripped == "# COMMAND ----------":
        out.append(line)
        in_markdown = False
        continue

    # Also handle corrupted separator: # MAGIC # COMMAND ----------
    if stripped == "# MAGIC # COMMAND ----------":
        out.append("# COMMAND ----------")
        in_markdown = False
        continue

    # Detect markdown cell start
    if stripped == "# MAGIC %md" or stripped == "%md":
        out.append("# MAGIC %md")
        in_markdown = True
        continue

    # Inside markdown cell - keep # MAGIC prefix
    if in_markdown:
        if stripped.startswith("# MAGIC "):
            out.append(line)
        elif stripped == "# MAGIC":
            out.append(line)
        elif stripped == "":
            out.append("")
        else:
            # Non-MAGIC line in markdown - add prefix
            out.append("# MAGIC " + stripped)
        continue

    # Python code line - strip # MAGIC prefix
    if stripped.startswith("# MAGIC "):
        content = line.replace("# MAGIC ", "", 1)
        out.append(content)
    elif stripped == "# MAGIC":
        out.append("")
    else:
        out.append(line)

result = "\n".join(out)

with open(path, "w", encoding="utf-8") as f:
    f.write(result)

# Verify
with open(path, "r", encoding="utf-8") as f:
    lines_out = f.readlines()

magic_python = 0
dlt_tables = 0
for ln in lines_out:
    s = ln.strip()
    if s.startswith("# MAGIC") and ("import " in s or "@dlt" in s or "def " in s):
        magic_python += 1
    if "@dlt.table" in s and not s.startswith("# MAGIC"):
        dlt_tables += 1

print(f"Lines with # MAGIC on Python code: {magic_python}")
print(f"Clean @dlt.table decorators found: {dlt_tables}")
print(f"Total output lines: {len(lines_out)}")

if magic_python == 0 and dlt_tables > 0:
    print("SUCCESS - Gold file is clean!")
else:
    print("WARNING - Issues remain")
