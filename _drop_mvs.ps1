$ErrorActionPreference = 'Stop'
$tokenJson = & databricks auth token --profile cli-auth 2>&1 | Out-String
$token = ($tokenJson | ConvertFrom-Json).access_token
$host_url = 'https://dbc-760a206e-c226.cloud.databricks.com'
$wh_id = '7655fa24e271f9d1'
$headers = @{
    'Authorization' = "Bearer $token"
    'Content-Type'  = 'application/json'
}

$statements = @(
    'DROP MATERIALIZED VIEW IF EXISTS workspace.procurement_gold.dim_vendor',
    'DROP MATERIALIZED VIEW IF EXISTS workspace.procurement_gold.dim_project',
    'DROP MATERIALIZED VIEW IF EXISTS workspace.procurement_gold.dim_material',
    'DROP MATERIALIZED VIEW IF EXISTS workspace.procurement_gold.fact_purchase_orders',
    'DROP MATERIALIZED VIEW IF EXISTS workspace.procurement_gold.fact_invoices',
    'DROP MATERIALIZED VIEW IF EXISTS workspace.procurement_gold.fact_goods_receipts',
    'DROP MATERIALIZED VIEW IF EXISTS workspace.procurement_gold.fact_project_costs',
    'DROP MATERIALIZED VIEW IF EXISTS workspace.procurement_gold.fact_contracts',
    'DROP MATERIALIZED VIEW IF EXISTS workspace.procurement_gold.fact_vendor_performance',
    'DROP MATERIALIZED VIEW IF EXISTS workspace.procurement_gold.fact_sales'
)

foreach ($stmt in $statements) {
    $body = @{
        warehouse_id = $wh_id
        statement    = $stmt
        wait_timeout = '30s'
        disposition  = 'INLINE'
    } | ConvertTo-Json -Compress

    try {
        $r = Invoke-RestMethod -Uri "$host_url/api/2.0/sql/statements" `
            -Method Post -Headers $headers -Body $body
        Write-Host "OK: $stmt  ->  state=$($r.status.state)"
    } catch {
        Write-Host "ERR: $stmt  ->  $($_.Exception.Message)"
    }
}

Write-Host ''
Write-Host 'All DROP statements executed.'
