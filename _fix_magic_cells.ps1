$path = 'c:\Users\mohdq\src\DataBricks-Sample1\databricks\gold\03_gold_pipeline.py'
$lines = Get-Content $path
$inMagicCell = $false
$result = [System.Collections.Generic.List[string]]::new()
foreach ($line in $lines) {
    if ($line -eq '# Databricks notebook source') {
        $inMagicCell = $false
        $result.Add($line)
    } elseif ($line -eq '# COMMAND ----------') {
        $inMagicCell = $false
        $result.Add($line)
    } elseif ($line -match '^%\w' -and -not $inMagicCell) {
        $inMagicCell = $true
        $result.Add('# MAGIC ' + $line)
    } elseif ($inMagicCell) {
        $result.Add('# MAGIC ' + $line)
    } else {
        $result.Add($line)
    }
}
[System.IO.File]::WriteAllLines($path, $result, [System.Text.UTF8Encoding]::new($false))
Write-Host "Done. Lines written: $($result.Count)"
