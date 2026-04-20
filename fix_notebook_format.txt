"""Fix Databricks notebook format: remove # MAGIC from Python code cells, keep for %md cells."""
import sys

def fix_notebook(filepath):
    lines = open(filepath).readlines()
    out = []
    in_md_cell = False
    
    i = 0
    while i < len(lines):
        line = lines[i]
        raw = line.rstrip('\n')
        
        # Header line
        if raw == '# Databricks notebook source':
            out.append(line)
            i += 1
            continue
        
        # Cell separator - never has MAGIC (already fixed)
        if raw == '# COMMAND ----------':
            out.append(line)
            in_md_cell = False  # Reset - will determine on next content line
            i += 1
            continue
        
        # If we just entered a new cell, determine its type
        if not in_md_cell:
            # Check if this line starts a %md cell
            stripped = raw
            if stripped.startswith('# MAGIC '):
                content = stripped[len('# MAGIC '):]
            elif stripped == '# MAGIC':
                content = ''
            else:
                content = None
            
            if content is not None and content.strip().startswith('%md'):
                in_md_cell = True
            elif content == '' or content is None:
                # Empty line - just output as empty
                if stripped == '# MAGIC' or stripped == '# MAGIC ':
                    out.append('\n')
                else:
                    out.append(line)
                i += 1
                continue
            else:
                # This is a code cell - strip MAGIC
                in_md_cell = False
        
        if in_md_cell:
            # Keep # MAGIC for markdown cells
            out.append(line)
        else:
            # Strip # MAGIC from code cells
            if raw.startswith('# MAGIC '):
                out.append(raw[len('# MAGIC '):] + '\n')
            elif raw == '# MAGIC':
                out.append('\n')
            else:
                out.append(line)
        
        i += 1
    
    open(filepath, 'w').write(''.join(out))
    
    # Verify
    lines2 = open(filepath).readlines()
    seps = sum(1 for l in lines2 if '# COMMAND' in l)
    magic_code = 0
    in_md = False
    for l in lines2:
        r = l.rstrip('\n')
        if r == '# COMMAND ----------':
            in_md = False
            continue
        if r.startswith('# MAGIC %md'):
            in_md = True
            continue
        if not in_md and r.startswith('# MAGIC ') and not r.startswith('# MAGIC %md'):
            magic_code += 1
    
    print(f'{filepath}: {len(lines)} -> {len(lines2)} lines, {seps} separators, {magic_code} MAGIC in code cells')

fix_notebook(sys.argv[1])
