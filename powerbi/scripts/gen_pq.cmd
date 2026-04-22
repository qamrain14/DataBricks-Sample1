@echo off
python -c "
import os, sys
sys.path.insert(0, os.path.join(os.getcwd(), 'powerbi', 'scripts'))
from connection_config import ALL_TABLES, CONNECTION

out = os.path.join(os.getcwd(), 'powerbi', 'connections')
os.makedirs(out, exist_ok=True)

def pascal(s):
    return ''.join(w.capitalize() for w in s.split('_'))

count = 0
for key, t in ALL_TABLES.items():
    cols = [c for c in t.columns if not c.startswith('_')]
    rename_steps = []
    for c in cols:
        pc = pascal(c)
        if pc != c:
            rename_steps.append('{\"' + c + '\", \"' + pc + '\"}')
    
    m = 'let\n'
    m += '    Source = Databricks.Catalogs(\"' + CONNECTION.host + '\", \"' + CONNECTION.http_path + '\", [Catalog=\"' + CONNECTION.catalog + '\", Database=\"' + t.schema + '\"]),\n'
    m += '    Nav1 = Source{[Name=\"' + CONNECTION.catalog + '\"]}[Data],\n'
    m += '    Nav2 = Nav1{[Name=\"' + t.schema + '\"]}[Data],\n'
    m += '    RawTable = Nav2{[Name=\"' + t.table + '\"]}[Data],\n'
    
    internal = [c for c in t.columns if c.startswith('_')]
    if internal:
        remove_list = ', '.join(['\"' + c + '\"' for c in internal])
        m += '    RemoveInternal = Table.RemoveColumns(RawTable, {' + remove_list + '}),\n'
        prev = 'RemoveInternal'
    else:
        prev = 'RawTable'
    
    if rename_steps:
        m += '    Renamed = Table.RenameColumns(' + prev + ', {' + ', '.join(rename_steps) + '})\n'
        m += 'in\n    Renamed'
    else:
        m += 'in\n    ' + prev
    
    fpath = os.path.join(out, key + '.pq')
    with open(fpath, 'w', encoding='utf-8') as f:
        f.write(m)
    count += 1

print(f'Generated {count} .pq files in {out}')
"
