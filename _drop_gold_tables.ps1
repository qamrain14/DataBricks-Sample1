$t=(databricks auth token --profile cli-auth 2>&1 | Out-String | ConvertFrom-Json).access_token
$h=@{'Authorization'='Bearer '+$t;'Content-Type'='application/json'}
$tables=@('fact_goods_receipts','fact_contracts','fact_invoices','fact_inventory','fact_project_actuals','fact_project_costs','fact_sales','fact_vendor_performance','dim_vendor','dim_employee','dim_material','dim_project','dim_contract','dim_date','dim_geography','dim_cost_center','dim_sector')
foreach($tb in $tables){
    $stmt="DROP TABLE IF EXISTS workspace.procurement_gold.$tb"
    $b="{`"warehouse_id`":`"7655fa24e271f9d1`",`"statement`":`"$stmt`",`"wait_timeout`":`"50s`",`"disposition`":`"INLINE`"}"
    $r=Invoke-RestMethod -Uri 'https://dbc-760a206e-c226.cloud.databricks.com/api/2.0/sql/statements' -Method POST -Headers $h -Body $b
    Write-Host "Dropped $tb : $($r.status.state)"
}
