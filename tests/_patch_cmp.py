import pathlib
p = pathlib.Path(r'C:\Users\mohdq\src\DataBricks-Sample1\tests\mock_spark.py')
t = p.read_text(encoding='utf-8')

old = 'def _safe_cmp(a, b, op):\n    if a is None or b is None: return False\n    if op == \'>\': return a > b\n    if op == \'>=\': return a >= b\n    if op == \'<\': return a < b\n    if op == \'<=\': return a <= b\n    return False'

new = 'def _safe_cmp(a, b, op):\n    if a is None or b is None: return False\n    if type(a) is not type(b):\n        try:\n            a, b = float(a), float(b)\n        except (ValueError, TypeError):\n            return False\n    if op == \'>\': return a > b\n    if op == \'>=\': return a >= b\n    if op == \'<\': return a < b\n    if op == \'<=\': return a <= b\n    return False'

assert old in t, 'OLD STRING NOT FOUND'
t = t.replace(old, new, 1)
p.write_text(t, encoding='utf-8')
print('PATCHED OK')
