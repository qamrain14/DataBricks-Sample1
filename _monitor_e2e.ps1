# Set DATABRICKS_TOKEN environment variable before running this script
$token = $env:DATABRICKS_TOKEN
$hostUrl = 'https://dbc-760a206e-c226.cloud.databricks.com'
$headers = @{ Authorization = "Bearer $token" }
$run_id = '548847524677834'
$resp = Invoke-RestMethod -Uri "$hostUrl/api/2.1/jobs/runs/get?run_id=$run_id" -Headers $headers
Write-Output ("Life cycle: " + $resp.state.life_cycle_state)
Write-Output ("Result:     " + $resp.state.result_state)
Write-Output ("Message:    " + $resp.state.state_message)
Write-Output ""
Write-Output "Tasks:"
$resp.tasks | ForEach-Object {
    Write-Output ("  " + $_.task_key + " -> " + $_.state.life_cycle_state + " / " + $_.state.result_state)
}