# Power BI tests - imports from .pyw to avoid Databricks overwrite
import pathlib, importlib.util, sys
_p = pathlib.Path(__file__).with_name("test_powerbi_impl.pyw")
_spec = importlib.util.spec_from_file_location("test_powerbi_impl", _p)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
for _name in dir(_mod):
    if _name.startswith("Test"):
        globals()[_name] = getattr(_mod, _name)
