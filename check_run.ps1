$env:DATABRICKS_METADATA_SERVICE_URL='http://127.0.0.1:61861/3cd48f7a-a2c2-4967-8ab6-8ca627a8d759'
$env:DATABRICKS_AUTH_TYPE='metadata-service'
$env:DATABRICKS_HOST='https://dbc-760a206e-c226.cloud.databricks.com'
$cliPath = 'c:\Users\mohdq\.vscode\extensions\databricks.databricks-2.10.6-win32-x64\bin\databricks.exe'
Set-Location 'C:\Users\mohdq\src\DataBricks-Sample1'
do {
  Start-Sleep 30
  $runJson = & $cliPath jobs get-run 1121057798052645 2>$null
  $run = $runJson | ConvertFrom-Json
  $state = $run.state.life_cycle_state
  $result = $run.state.result_state
  Write-Host "$(Get-Date -Format 'HH:mm:ss') Overall: $state $result"
  $run.tasks | ForEach-Object { Write-Host "  $($_.task_key): $($_.state.life_cycle_state) $($_.state.result_state)" }
} while ($state -eq 'RUNNING')
Write-Host "FINAL STATE: $state / $result"
