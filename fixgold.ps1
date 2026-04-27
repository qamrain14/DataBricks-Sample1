$p = 'databricks\gold\03_gold_pipeline.py'

$rawBytes = [IO.File]::ReadAllBytes($p)
if ($rawBytes[0] -eq 0xEF -and $rawBytes[1] -eq 0xBB -and $rawBytes[2] -eq 0xBF) {
    $rawBytes = $rawBytes[3..($rawBytes.Length-1)]
    Write-Host "Stripped UTF-8 BOM"
}

$text = [Text.Encoding]::UTF8.GetString($rawBytes) -replace "`r`n","`n" -replace "`r","`n"
$lines = $text -split "`n"
Write-Host "Total lines: $($lines.Count)"

$SEPARATOR = '# MAGIC # COMMAND ----------'

# Determine initial cell mode
$initial_mode = $null
for ($i = 1; $i -lt $lines.Count; $i++) {
    $s = $lines[$i].Trim()
    if ($s -and $s -ne $SEPARATOR) {
        if ($s.StartsWith('# MAGIC %md')) { $initial_mode = 'markdown' } else { $initial_mode = 'code' }
        break
    }
}
Write-Host "Initial cell mode: $initial_mode"

$output = [System.Collections.Generic.List[string]]::new()
$cell_mode = $initial_mode

for ($i = 0; $i -lt $lines.Count; $i++) {
    $ln = $lines[$i]
    if ($i -eq 0) { $output.Add($ln); continue }

    if ($ln.Trim() -eq $SEPARATOR) {
        $output.Add('# COMMAND ----------')
        $cell_mode = $null
        continue
    }

    if ($null -eq $cell_mode -and $ln.Trim()) {
        if ($ln.Trim().StartsWith('# MAGIC %md')) { $cell_mode = 'markdown' } else { $cell_mode = 'code' }
    }

    if ($cell_mode -eq 'markdown') {
        $output.Add($ln)
    } elseif ($cell_mode -eq 'code') {
        if ($ln.StartsWith('# MAGIC ')) { $output.Add($ln.Substring(8)) }
        elseif ($ln -eq '# MAGIC') { $output.Add('') }
        else { $output.Add($ln) }
    } else {
        $output.Add($ln)
    }
}

$fixed = $output -join "`r`n"
# Use UTF8 without BOM encoding
$enc = New-Object System.Text.UTF8Encoding $false
$outBytes = $enc.GetBytes($fixed)
[IO.File]::WriteAllBytes($p, $outBytes)

# Verify
$magic = ($output | Where-Object { $_ -like '# MAGIC *' -or $_ -eq '# MAGIC' }).Count
$nonBlank = ($output | Where-Object { $_.Trim() }).Count
$ratio = if ($nonBlank) { [math]::Round($magic / $nonBlank, 4) } else { 0 }
$clean = ($output | Where-Object { $_.Trim() -eq '# COMMAND ----------' }).Count
$bad = ($output | Where-Object { $_ -like '*# MAGIC # COMMAND*' }).Count
$hasImport = ($output | Where-Object { $_ -like '*import dlt*' -and $_ -notlike '# MAGIC*' }).Count -gt 0
$hasBom = $output[0].StartsWith([char]0xFEFF)

Write-Host "Output lines:      $($output.Count)"
Write-Host "Magic ratio:       $ratio  (must be < 0.85)"
Write-Host "Clean separators:  $clean  (must be >= 3)"
Write-Host "Corrupt seps left: $bad  (must be 0)"
Write-Host "Has 'import dlt':  $hasImport  (must be True)"
Write-Host "Has BOM:           $hasBom  (must be False)"
Write-Host "Done: $p"
