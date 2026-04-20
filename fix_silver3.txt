lines = open('databricks/silver/02_silver_pipeline.py').read().split('\n')
clean = []
for l in lines:
    if l == '# Databricks notebook source':
        clean.append(l)
    elif l.startswith('# MAGIC '):
        clean.append(l[8:])
    elif l == '# MAGIC':
        clean.append('')
    else:
        clean.append(l)

cmds = [i for i, l in enumerate(clean) if l == '# COMMAND ----------']
print(f'Found {len(cmds)} COMMAND separators after stripping')

# Now split into cells and rebuild
cells = []
current = []
for l in clean:
    if l == '# COMMAND ----------':
        cells.append(current)
        current = []
    else:
        current.append(l)
if current:
    cells.append(current)

print(f'Found {len(cells)} cells')

out = []
for i, cell in enumerate(cells):
    # Strip leading/trailing blanks
    while cell and cell[0].strip() == '':
        cell = cell[1:]
    while cell and cell[-1].strip() == '':
        cell = cell[:-1]
    if not cell:
        continue
    
    if i == 0 and cell[0] == '# Databricks notebook source':
        out.append('# Databricks notebook source')
        cell = cell[1:]
        while cell and cell[0].strip() == '':
            cell = cell[1:]
        if not cell:
            continue
        # Fall through to process remaining content as next cell
    
    if out:
        out.append('# COMMAND ----------')
        out.append('')
    
    is_md = cell[0].strip().startswith('%md')
    if is_md:
        for cl in cell:
            if cl.strip() == '':
                out.append('# MAGIC')
            else:
                out.append('# MAGIC ' + cl)
    else:
        out.extend(cell)
    out.append('')

with open('databricks/silver/02_silver_pipeline.py', 'w') as f:
    f.write('\n'.join(out))

# Verify
vlines = open('databricks/silver/02_silver_pipeline.py').readlines()
magic_code = sum(1 for l in vlines if '# MAGIC' in l and '@dlt.table' in l)
dlt_found = sum(1 for l in vlines if l.rstrip().startswith('@dlt.table'))
cmd_ok = sum(1 for l in vlines if l.rstrip() == '# COMMAND ----------')
print(f'COMMAND separators: {cmd_ok}')
print(f'@dlt.table with MAGIC (BAD): {magic_code}')
print(f'@dlt.table without MAGIC (GOOD): {dlt_found}')
print(f'Total lines: {len(vlines)}')
