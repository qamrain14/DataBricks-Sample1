$ErrorActionPreference = 'Continue'
$tokenJson = & databricks auth token --profile cli-auth 2>&1 | Out-String
$token = ($tokenJson | ConvertFrom-Json).access_token
$host_url = 'https://dbc-760a206e-c226.cloud.databricks.com'
$wh_id = '7655fa24e271f9d1'
$headers = @{ 'Authorization' = "Bearer $token"; 'Content-Type' = 'application/json' }

Write-Host "Executing demo_upsert_set_a.sql ..."

$sqlContent = Get-Content -Path 'databricks\scripts\demo_upsert_set_a.sql' -Raw -Encoding UTF8

# Strip all comment lines (whole lines starting with --) before splitting
$lines = $sqlContent -split '\r?\n'
$noComments = ($lines | Where-Object { $_ -notmatch '^\s*--' }) -join "`n"

# Split on semicolons, trim, drop empty
$statements = $noComments -split ';' | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }

Write-Host "Found $($statements.Count) statements to execute."

$ok = 0; $err = 0
foreach ($stmt in $statements) {
        $body = @{ warehouse_id = $wh_id; statement = $stmt; wait_timeout = '30s'; disposition = 'INLINE' } | ConvertTo-Json -Compress
    try {
        $r       = Invoke-RestMethod -Uri "$host_url/api/2.0/sql/statements" -Method Post -Headers $headers -Body $body -ErrorAction Stop
        $state   = $r.status.state
        $flat    = ($stmt -replace '\s+', ' ').Trim()
        $preview = if ($flat.Length -gt 80) { $flat.Substring(0,80) + '...' } else { $flat }
        if ($state -eq 'SUCCEEDED') {
            Write-Host "OK   [$state]  $preview"
            $ok++
        } else {
            $msg = $r.status.error.message
            Write-Host "FAIL [$state]  $preview"
            Write-Host "     $msg"
            $err++
        }
    } catch {
        $flat    = ($stmt -replace '\s+', ' ').Trim()
        $preview = if ($flat.Length -gt 80) { $flat.Substring(0,80) + '...' } else { $flat }
        Write-Host "ERR  $preview  ->  $($_.Exception.Message)"
        $err++
    }
}

Write-Host ""
Write-Host "Set A complete: $ok OK, $err ERR"
