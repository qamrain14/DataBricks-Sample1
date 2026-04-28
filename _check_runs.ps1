# Set DATABRICKS_TOKEN environment variable before running this script
$token = $env:DATABRICKS_TOKEN
$hostUrl = 'https://dbc-760a206e-c226.cloud.databricks.com'
$headers = @{ Authorization = "Bearer $token" }
$resp = Invoke-RestMethod -Uri "$hostUrl/api/2.1/jobs/runs/list?job_id=945319848056279&limit=5" -Headers $headers
$resp.runs | ForEach-Object {
    Write-Output ("run_id: " + $_.run_id + "  life_cycle: " + $_.state.life_cycle_state + "  result: " + $_.state.result_state)
}