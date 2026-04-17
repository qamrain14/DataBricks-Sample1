"""
DLT Mock Framework for offline testing of Databricks DLT pipelines.
Simulates dlt.table, dlt.read, dlt.expect, dlt.expect_or_drop, dlt.expect_or_fail.
"""
import functools

_tables = {}
_table_metadata = {}
_expectation_results = {}
_pending_functions = []

def reset():
    _tables.clear()
    _table_metadata.clear()
    _expectation_results.clear()
    _pending_functions.clear()

def register(name, df):
    _tables[name] = df

def read(name):
    if name not in _tables:
        raise KeyError(f"Table '{name}' not found. Available: {list(_tables.keys())}")
    return _tables[name]

def get_table(name):
    return _tables.get(name)

def get_all_tables():
    return dict(_tables)

def get_expectations():
    return dict(_expectation_results)

def table(name=None, comment=None, table_properties=None, path=None):
    def decorator(func):
        tbl_name = name or func.__name__
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            df = func(*args, **kwargs)
            _tables[tbl_name] = df
            _table_metadata[tbl_name] = {
                "comment": comment,
                "table_properties": table_properties,
                "path": path,
            }
            return df
        _pending_functions.append((tbl_name, wrapper))
        return wrapper
    return decorator

def expect(rule_name, condition):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            df = func(*args, **kwargs)
            try:
                violations = df.where(f"NOT ({condition})").count()
            except Exception:
                violations = -1
            tbl_name = _find_table_name(df, rule_name)
            _expectation_results.setdefault(tbl_name, []).append(
                (rule_name, condition, violations, "warn")
            )
            return df
        return wrapper
    return decorator

def expect_or_drop(rule_name, condition):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            df = func(*args, **kwargs)
            before = df.count()
            filtered = df.where(condition)
            after = filtered.count()
            tbl_name = _find_table_name(df, rule_name)
            _expectation_results.setdefault(tbl_name, []).append(
                (rule_name, condition, before - after, "drop")
            )
            for n, d in list(_tables.items()):
                if d is df:
                    _tables[n] = filtered
                    break
            return filtered
        return wrapper
    return decorator

def expect_or_fail(rule_name, condition):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            df = func(*args, **kwargs)
            violations = df.where(f"NOT ({condition})").count()
            if violations > 0:
                raise ValueError(
                    f"DLT expect_or_fail FAILED: rule='{rule_name}', "
                    f"condition='{condition}', violations={violations}"
                )
            tbl_name = _find_table_name(df, rule_name)
            _expectation_results.setdefault(tbl_name, []).append(
                (rule_name, condition, 0, "fail")
            )
            return df
        return wrapper
    return decorator

def execute_pending():
    """Execute all pending @dlt.table functions, resolving dependencies."""
    remaining = list(_pending_functions)
    max_iter = len(remaining) + 2
    for _ in range(max_iter):
        if not remaining:
            break
        still_remaining = []
        for tbl_name, func in remaining:
            try:
                func()
            except (KeyError, Exception) as e:
                still_remaining.append((tbl_name, func))
        if len(still_remaining) == len(remaining):
            names = [n for n, _ in still_remaining]
            errors = []
            for tbl_name, func in still_remaining:
                try:
                    func()
                except Exception as e:
                    errors.append(f"{tbl_name}: {e}")
            raise RuntimeError(
                f"Cannot resolve dependencies for: {names}\nErrors:\n" + "\n".join(errors)
            )
        remaining = still_remaining
    _pending_functions.clear()

def _find_table_name(df, default):
    for n, d in _tables.items():
        if d is df:
            return n
    return default
