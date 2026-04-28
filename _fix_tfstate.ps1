$statePath = 'c:\Users\mohdq\src\DataBricks-Sample1\.databricks\bundle\local\terraform\terraform.tfstate'
$state = Get-Content $statePath | ConvertFrom-Json

# Define the three pipelines to import
$pipelines = @(
    @{ name = "procurement_bronze_pipeline"; id = "28973439-4c46-4ee4-952c-375e8e6dfa91" },
    @{ name = "procurement_silver_pipeline"; id = "b9207b48-1feb-460f-afe3-d904d0fb1381" },
    @{ name = "procurement_gold_pipeline";   id = "7788297f-603e-4786-8046-14bf773e787b" }
)

foreach ($p in $pipelines) {
    $resource = [PSCustomObject]@{
        mode      = "managed"
        type      = "databricks_pipeline"
        name      = $p.name
        provider  = 'provider["registry.terraform.io/databricks/databricks"]'
        instances = @(
            [PSCustomObject]@{
                schema_version = 0
                attributes     = [PSCustomObject]@{
                    id = $p.id
                }
                sensitive_attributes = @()
                private = ""
            }
        )
    }
    $state.resources += $resource
}

$state.serial = $state.serial + 1

$json = $state | ConvertTo-Json -Depth 10
[System.IO.File]::WriteAllText($statePath, $json, [System.Text.UTF8Encoding]::new($false))
Write-Output "Done. Resources in state:"
($state | ConvertFrom-Json -ErrorAction SilentlyContinue) 2>$null
$state.resources | ForEach-Object { $_.type + '.' + $_.name + ' = ' + ($_.instances[0].attributes.id) }
