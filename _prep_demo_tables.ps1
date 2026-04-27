# =============================================================
# _prep_demo_tables.ps1
# PURPOSE : Convert 10 DLT-managed gold streaming tables to
#           regular writable Delta tables so demo UPDATEs work.
#
# WHAT IT DOES (31 SQL statements):
#   1. CREATE SCHEMA workspace.procurement_demo  (backup schema)
#   2. CTAS each table -> procurement_demo       (original data backup)
#   3. DROP each DLT streaming table from procurement_gold
#   4. CTAS each table from procurement_demo -> procurement_gold
#      (recreated as regular writable Delta tables)
#
# REVERT AFTER DEMO : Run _restore_gold_from_demo.ps1  OR
#                     re-run the procurement_gold_pipeline (full refresh)
# =============================================================

$ErrorActionPreference = 'Continue'

$tokenJson = & databricks auth token --profile cli-auth 2>&1 | Out-String
$token     = ($tokenJson | ConvertFrom-Json).access_token
$host_url  = 'https://dbc-760a206e-c226.cloud.databricks.com'
$wh_id     = '7655fa24e271f9d1'
$headers   = @{ 'Authorization' = "Bearer $token"; 'Content-Type' = 'application/json' }

$tables = @(
    'fact_purchase_orders',
    'fact_invoices',
    'fact_contracts',
    'fact_project_costs',
    'fact_goods_receipts',
    'fact_sales',
    'fact_vendor_performance',
    'dim_vendor',
    'dim_project',
    'dim_material'
)

$statements = [System.Collections.Generic.List[string]]::new()

# Phase 1: create backup schema
$statements.Add("CREATE SCHEMA IF NOT EXISTS workspace.procurement_demo")

# Phase 2: backup original gold data to procurement_demo
foreach ($t in $tables) {
    $statements.Add("CREATE OR REPLACE TABLE workspace.procurement_demo.$t AS SELECT * FROM workspace.procurement_gold.$t")
}

# Phase 3: drop DLT-managed streaming tables from gold schema
foreach ($t in $tables) {
    $statements.Add("DROP TABLE IF EXISTS workspace.procurement_gold.$t")
}

# Phase 4: recreate as regular writable Delta tables in gold schema
foreach ($t in $tables) {
    $statements.Add("CREATE TABLE workspace.procurement_gold.$t AS SELECT * FROM workspace.procurement_demo.$t")
}

Write-Host "Prep: $($statements.Count) statements across 4 phases ..."
Write-Host ""

$ok = 0; $err = 0; $i = 0

foreach ($stmt in $statements) {
    $i++
    $body = @{
        warehouse_id = $wh_id
        statement    = $stmt
        wait_timeout = '50s'
        disposition  = 'INLINE'
    } | ConvertTo-Json -Compress

    try {
        $r     = Invoke-RestMethod -Uri "$host_url/api/2.0/sql/statements" -Method Post -Headers $headers -Body $body -ErrorAction Stop
        $state = $r.status.state
        $len   = [Math]::Min(90, $stmt.Length)
        $prev  = ($stmt -replace '\s+', ' ').Substring(0, $len)

        if ($state -eq 'SUCCEEDED') {
            Write-Host "[$i/$($statements.Count)] OK    $prev"
            $ok++
        } else {
            $msg = $r.status.error.message
            Write-Host "[$i/$($statements.Count)] FAIL  $prev"
            Write-Host "              $msg"
            $err++
        }
    } catch {
        $len  = [Math]::Min(90, $stmt.Length)
        $prev = ($stmt -replace '\s+', ' ').Substring(0, $len)
        Write-Host "[$i/$($statements.Count)] ERR   $prev"
        Write-Host "              $($_.Exception.Message)"
        $err++
    }
}

Write-Host ""
Write-Host "Prep complete: $ok OK, $err ERR"
if ($err -gt 0) {
    Write-Host "WARNING: $err statement(s) failed - review output before running demo scripts."
} else {
    Write-Host "All gold tables are now regular writable Delta tables."
    Write-Host "Run _run_demo_set_a.ps1 to apply demo data changes."
}
