$p = 'c:\Users\mohdq\src\DataBricks-Sample1\tests\test_gold_pipeline.py'
$lines = [System.IO.File]::ReadAllLines($p, [System.Text.Encoding]::UTF8)
$old = '            pattern = rf"def {fn}\(\):(.*?)(?=def |\# COMMAND)"'
$new = '            pattern = rf"def {fn}\(\):(.*?)(?=def |\# COMMAND|\Z)"'
$count = 0
$out = [System.Collections.Generic.List[string]]::new()
foreach ($line in $lines) {
    if ($line -eq $old) { $out.Add($new); $count++ } else { $out.Add($line) }
}
[System.IO.File]::WriteAllLines($p, $out, [System.Text.Encoding]::UTF8)
Write-Host "Fixed $count lines"
