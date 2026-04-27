$path = 'c:\Users\mohdq\src\DataBricks-Sample1\databricks\gold\03_gold_pipeline.py'
$lines = [System.IO.File]::ReadAllLines($path, [System.Text.Encoding]::UTF8)

$targets = @(
    @{line=70; var='df'; table='silver_vendors'},
    @{line=114; var='df'; table='silver_projects'},
    @{line=150; var='df'; table='silver_materials'},
    @{line=301; var='po'; table='silver_purchase_orders'},
    @{line=341; var='df'; table='silver_invoices'},
    @{line=385; var='gr'; table='silver_goods_receipts'},
    @{line=426; var='budgets'; table='silver_project_budgets'},
    @{line=504; var='df'; table='silver_vendor_performance'},
    @{line=556; var='contracts'; table='silver_contracts'},
    @{line=596; var='sales'; table='silver_sales_orders'}
)

$changes = 0
foreach ($t in $targets) {
    $line = $lines[$t.line]
    if ($line.Contains("$($t.var) = spark.read.table") -and $line.Contains($t.table)) {
        $lines[$t.line] = $line.Replace('spark.read.table', 'spark.readStream.table')
        $changes++
        Write-Host "OK [$changes] line $($t.line + 1): $($t.table)"
    } else {
        Write-Host "SKIP line $($t.line + 1): expected var=$($t.var) table=$($t.table)"
        Write-Host "  actual: $line"
    }
}

[System.IO.File]::WriteAllLines($path, $lines, [System.Text.Encoding]::UTF8)
Write-Host ""
Write-Host "Total changes: $changes / 10"

$content = [System.IO.File]::ReadAllText($path, [System.Text.Encoding]::UTF8)
$rs = ([regex]::Matches($content, 'readStream\.table')).Count
$r  = ([regex]::Matches($content, '(?<!readStream)\.read\.table')).Count
Write-Host "Verification -- readStream.table: $rs  |  .read.table: $r"
