$ErrorActionPreference = 'Continue'
$tokenJson = & databricks auth token --profile cli-auth 2>&1 | Out-String
$token = ($tokenJson | ConvertFrom-Json).access_token
$host_url = 'https://dbc-760a206e-c226.cloud.databricks.com'
$headers = @{
    'Authorization' = "Bearer $token"
    'Content-Type'  = 'application/json'
}

# List pipelines
$r = Invoke-RestMethod -Uri "$host_url/api/2.0/pipelines" -Method Get -Headers $headers
foreach ($p in $r.statuses) {
    Write-Host "Pipeline: name=$($p.name)  id=$($p.pipeline_id)  state=$($p.state)"
}
