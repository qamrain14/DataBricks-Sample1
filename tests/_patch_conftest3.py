import pathlib

fp = pathlib.Path(r"C:\Users\mohdq\src\DataBricks-Sample1\tests\conftest.py")
content = fp.read_text(encoding="utf-8")

old = 'import dlt_mock\n\n@pytest.fixture(scope="session")'
new = '''import dlt_mock
import mock_spark
import types as _types

# Patch sys.modules so `from pyspark.*` resolves to mock_spark (no JVM needed)
_pyspark = _types.ModuleType('pyspark')
_pyspark_sql = _types.ModuleType('pyspark.sql')
_pyspark_sql_functions = _types.ModuleType('pyspark.sql.functions')
_pyspark_sql_types = _types.ModuleType('pyspark.sql.types')
_pyspark_sql_window = _types.ModuleType('pyspark.sql.window')

for _name in dir(mock_spark):
    if _name.startswith('_'):
        continue
    _obj = getattr(mock_spark, _name)
    setattr(_pyspark_sql_functions, _name, _obj)
    setattr(_pyspark_sql_types, _name, _obj)
    setattr(_pyspark_sql, _name, _obj)
    setattr(_pyspark_sql_window, _name, _obj)

_pyspark_sql.functions = _pyspark_sql_functions
_pyspark_sql.types = _pyspark_sql_types
_pyspark_sql.window = _pyspark_sql_window
_pyspark.sql = _pyspark_sql

sys.modules['pyspark'] = _pyspark
sys.modules['pyspark.sql'] = _pyspark_sql
sys.modules['pyspark.sql.functions'] = _pyspark_sql_functions
sys.modules['pyspark.sql.types'] = _pyspark_sql_types
sys.modules['pyspark.sql.window'] = _pyspark_sql_window

@pytest.fixture(scope="session")'''

if old in content:
    content = content.replace(old, new, 1)
    fp.write_text(content, encoding="utf-8")
    print("PATCHED OK")
else:
    print("OLD STRING NOT FOUND")
    print(repr(content[:500]))
