$env:DATABRICKS_METADATA_SERVICE_URL='http://127.0.0.1:61861/3cd48f7a-a2c2-4967-8ab6-8ca627a8d759'
$env:DATABRICKS_AUTH_TYPE='metadata-service'
$env:DATABRICKS_HOST='https://dbc-760a206e-c226.cloud.databricks.com'
Set-Location 'C:\Users\mohdq\src\DataBricks-Sample1'
$cliPath='c:\Users\mohdq\.vscode\extensions\databricks.databricks-2.10.6-win32-x64\bin\databricks.exe'
$runId='601329915805716'

while ($true) {
    $raw = & $cliPath jobs get-run $runId
    $obj = $raw | ConvertFrom-Json
    $state = $obj.state.life_cycle_state
    $ts = Get-Date -Format 'HH:mm:ss'
    $taskSummary = ($obj.tasks | ForEach-Object { "$($_.task_key)=$($_.state.life_cycle_state)" }) -join ', '
    Write-Host "[$ts] State: $state | Tasks: $taskSummary"
    if ($state -eq 'TERMINATED' -or $state -eq 'SKIPPED' -or $state -eq 'INTERNAL_ERROR') { break }
    Start-Sleep -Seconds 30
}

Write-Host "Final result: $($obj.state.result_state)"
$obj.tasks | ForEach-Object { Write-Host "Task: $($_.task_key) => $($_.state.result_state)" }
