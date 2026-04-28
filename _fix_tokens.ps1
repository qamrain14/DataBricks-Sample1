# Fix hardcoded tokens - replace with env var reference
$checkRunsContent = @'
# Set DATABRICKS_TOKEN environment variable before running this script
$token = $env:DATABRICKS_TOKEN
$hostUrl = 'https://dbc-760a206e-c226.cloud.databricks.com'
$headers = @{ Authorization = "Bearer $token" }
$resp = Invoke-RestMethod -Uri "$hostUrl/api/2.1/jobs/runs/list?job_id=945319848056279&limit=5" -Headers $headers
$resp.runs | ForEach-Object {
    Write-Output ("run_id: " + $_.run_id + "  life_cycle: " + $_.state.life_cycle_state + "  result: " + $_.state.result_state)
}
'@

$monitorE2eContent = @'
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
'@

[System.IO.File]::WriteAllText('c:\Users\mohdq\src\DataBricks-Sample1\_check_runs.ps1', $checkRunsContent, [System.Text.UTF8Encoding]::new($false))
[System.IO.File]::WriteAllText('c:\Users\mohdq\src\DataBricks-Sample1\_monitor_e2e.ps1', $monitorE2eContent, [System.Text.UTF8Encoding]::new($false))

Write-Host "Done - tokens replaced with env var references"
Get-Content 'c:\Users\mohdq\src\DataBricks-Sample1\_check_runs.ps1' | Select-Object -First 3
Get-Content 'c:\Users\mohdq\src\DataBricks-Sample1\_monitor_e2e.ps1' | Select-Object -First 3
