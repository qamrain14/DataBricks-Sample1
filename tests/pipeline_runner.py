# Databricks notebook source
"""
Utility to extract and execute Databricks DLT notebook code in test mode.
Handles the standard Databricks notebook format:
  - # Databricks notebook source
  - # COMMAND ----------
  - # MAGIC %md / # MAGIC lines
"""
import re
import sys
import os

def extract_python_cells(notebook_path):
    """Extract pure Python code from a Databricks notebook file."""
    with open(notebook_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Split into cells by separator
    cells = re.split(r"# COMMAND ----------", content)
    python_lines = []

    for cell in cells:
        lines = cell.strip().split("\n")
        if not lines or not lines[0].strip():
            lines = [l for l in lines if l.strip()]
        if not lines:
            continue

        first = lines[0].strip()

        # Skip the header line
        if first == "# Databricks notebook source":
            continue

        # Skip markdown cells
        if first.startswith("# MAGIC %md"):
            continue

        # Process regular Python cells - strip # MAGIC prefix if present
        clean = []
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("# MAGIC "):
                clean.append(line.replace("# MAGIC ", "", 1))
            elif stripped == "# MAGIC":
                clean.append("")
            else:
                clean.append(line)
        python_lines.extend(clean)
        python_lines.append("")  # blank line between cells

    return "\n".join(python_lines)


def run_pipeline(notebook_path, spark_session, dlt_mock, skip_imports=None):
    """
    Execute a DLT pipeline notebook in test mode.
    - Patches sys.modules so `import dlt` resolves to dlt_mock
    - Injects `spark` as a global
    - Calls dlt_mock.execute_pending() to run all @dlt.table functions
    Returns dict of all registered tables.
    """
    # Patch dlt module
    original_dlt = sys.modules.get("dlt")
    sys.modules["dlt"] = dlt_mock

    # Patch pyspark modules with mock_spark so pipeline imports resolve
    tests_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, tests_dir)
    import mock_spark as _ms
    import types

    _pyspark = types.ModuleType("pyspark")
    _pyspark_sql = types.ModuleType("pyspark.sql")
    _pyspark_sql_functions = types.ModuleType("pyspark.sql.functions")
    _pyspark_sql_types = types.ModuleType("pyspark.sql.types")

    # Copy all public attrs from mock_spark into the fake pyspark.sql.functions / types
    for name in dir(_ms):
        if not name.startswith("_"):
            setattr(_pyspark_sql_functions, name, getattr(_ms, name))
            setattr(_pyspark_sql_types, name, getattr(_ms, name))
    _pyspark_sql.functions = _pyspark_sql_functions
    _pyspark_sql.types = _pyspark_sql_types
    _pyspark_sql.SparkSession = _ms.SparkSession
    _pyspark_sql.DataFrame = _ms.DataFrame
    _pyspark_sql.Row = _ms.Row
    _pyspark_sql.Column = _ms.Column

    pyspark_mods = {
        "pyspark": _pyspark,
        "pyspark.sql": _pyspark_sql,
        "pyspark.sql.functions": _pyspark_sql_functions,
        "pyspark.sql.types": _pyspark_sql_types,
    }
    originals = {k: sys.modules.get(k) for k in pyspark_mods}
    sys.modules.update(pyspark_mods)

    try:
        code = extract_python_cells(notebook_path)

        # Remove lines that would fail in local mode
        filtered = []
        for line in code.split("\n"):
            stripped = line.strip()
            # Skip spark.read.format("com.crealytics.spark.excel") calls
            if "com.crealytics.spark.excel" in stripped:
                continue
            if skip_imports and any(s in stripped for s in skip_imports):
                continue
            filtered.append(line)
        code = "\n".join(filtered)

        exec_globals = {
            "spark": spark_session,
            "__builtins__": __builtins__,
            "__name__": "__main__",
        }

        exec(code, exec_globals)
        dlt_mock.execute_pending()
        return dlt_mock.get_all_tables()

    finally:
        # Restore pyspark modules
        for k, v in originals.items():
            if v is not None:
                sys.modules[k] = v
            elif k in sys.modules:
                del sys.modules[k]
        if original_dlt is not None:
            sys.modules["dlt"] = original_dlt
        elif "dlt" in sys.modules:
            del sys.modules["dlt"]
