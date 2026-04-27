$ErrorActionPreference = 'Stop'
$tokenJson = & databricks auth token --profile cli-auth 2>&1 | Out-String
$token = ($tokenJson | ConvertFrom-Json).access_token
$host_url = 'https://dbc-760a206e-c226.cloud.databricks.com'
$pipeline_id = '7788297f-603e-4786-8046-14bf773e787b'
$headers = @{
    'Authorization' = "Bearer $token"
    'Content-Type'  = 'application/json'
}

$body = @{ full_refresh = $true } | ConvertTo-Json -Compress

Write-Host "Triggering full refresh for procurement_gold_pipeline ($pipeline_id)..."
$r = Invoke-RestMethod -Uri "$host_url/api/2.0/pipelines/$pipeline_id/updates" `
    -Method Post -Headers $headers -Body $body

Write-Host "Update ID: $($r.update_id)"
Write-Host "Full refresh triggered successfully."
Write-Host ""
Write-Host "Polling for completion (this may take several minutes)..."

# Poll until pipeline is no longer RUNNING/INITIALIZING
$maxWait = 1800   # 30 minutes max
$waited  = 0
$pollSec = 30

Start-Sleep -Seconds $pollSec
$waited += $pollSec

while ($waited -lt $maxWait) {
    $status = Invoke-RestMethod -Uri "$host_url/api/2.0/pipelines/$pipeline_id" `
        -Method Get -Headers $headers
    $state = $status.state
    $latestUpdate = $status.latest_updates | Select-Object -First 1
    $updateState  = if ($latestUpdate) { $latestUpdate.state } else { 'N/A' }

    Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Pipeline state=$state  update_state=$updateState  (waited ${waited}s)"

    if ($updateState -in @('COMPLETED', 'FAILED', 'CANCELED')) {
        Write-Host ""
        if ($updateState -eq 'COMPLETED') {
            Write-Host "SUCCESS: Pipeline full refresh COMPLETED."
        } else {
            Write-Host "WARNING: Pipeline update ended with state=$updateState"
        }
        break
    }

    Start-Sleep -Seconds $pollSec
    $waited += $pollSec
}

if ($waited -ge $maxWait) {
    Write-Host "TIMEOUT: Pipeline did not complete within $maxWait seconds. Check Databricks UI."
}
