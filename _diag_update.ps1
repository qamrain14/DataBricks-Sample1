$t = (& databricks auth token --profile cli-auth 2>&1 | Out-String | ConvertFrom-Json).access_token
$stmt = "UPDATE workspace.procurement_gold.fact_purchase_orders SET po_value = ROUND(po_value * 1.30, 2) WHERE po_id IN (SELECT po_id FROM workspace.procurement_gold.fact_purchase_orders WHERE po_status = 'APPROVED' ORDER BY po_id ASC LIMIT 5)"
$b = @{ warehouse_id = '7655fa24e271f9d1'; statement = $stmt; wait_timeout = '30s'; disposition = 'INLINE' } | ConvertTo-Json -Compress
$h = @{ 'Authorization' = "Bearer $t"; 'Content-Type' = 'application/json' }
$r = Invoke-RestMethod -Uri 'https://dbc-760a206e-c226.cloud.databricks.com/api/2.0/sql/statements' -Method Post -Headers $h -Body $b
Write-Host "State: $($r.status.state)"
Write-Host "Error code: $($r.status.error.error_code)"
Write-Host "Error msg:  $($r.status.error.message)"
Write-Host "Full JSON:"
$r | ConvertTo-Json -Depth 10
