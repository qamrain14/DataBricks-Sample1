$ErrorActionPreference = 'Stop'

# ====== PART E: Insert DAX measures into BIM ======

$bimPath = 'c:\Users\mohdq\src\DataBricks-Sample1\powerbi\model\procurement_model.bim'
$bimLines = [System.IO.File]::ReadAllLines($bimPath, [System.Text.Encoding]::UTF8)
Write-Host "BIM loaded: $($bimLines.Count) lines"

$idx = 3875
if (-not $bimLines[$idx].Contains('],')) {
    Write-Host "FAIL: line 3876 is not '],' — got: $($bimLines[$idx])"
    exit 1
}
if (-not $bimLines[$idx + 1].Contains('"partitions"')) {
    Write-Host "FAIL: line 3877 does not contain 'partitions' — got: $($bimLines[$idx+1])"
    exit 1
}

$found = $false
for ($k = $idx; $k -ge ($idx - 60); $k--) {
    if ($bimLines[$k].Contains('TotalLineValue')) { $found = $true; break }
}
if (-not $found) {
    Write-Host "FAIL: TotalLineValue not found near line 3876"
    exit 1
}
Write-Host "BIM checks passed. Inserting measures block..."

$newBim = [System.Collections.Generic.List[string]]::new()
for ($i = 0; $i -lt $bimLines.Count; $i++) {
    $newBim.Add($bimLines[$i])
    if ($i -eq $idx) {
        $newBim.Add('        "measures": [')
        $newBim.Add('          {')
        $newBim.Add('            "name": "Spend by Sector",')
        $newBim.Add('            "expression": "SUM(CubeProcurementSpend[TotalSpend])",')
        $newBim.Add('            "formatString": "$#,##0.00"')
        $newBim.Add('          },')
        $newBim.Add('          {')
        $newBim.Add('            "name": "Vendor Count",')
        $newBim.Add('            "expression": "DISTINCTCOUNT(CubeProcurementSpend[VendorId])",')
        $newBim.Add('            "formatString": "#,##0"')
        $newBim.Add('          },')
        $newBim.Add('          {')
        $newBim.Add('            "name": "YoY Growth",')
        $newBim.Add('            "expression": "AVERAGE(CubeProcurementSpend[YoyGrowthPct])",')
        $newBim.Add('            "formatString": "0.00%"')
        $newBim.Add('          }')
        $newBim.Add('        ],')
    }
}
[System.IO.File]::WriteAllLines($bimPath, $newBim, [System.Text.Encoding]::UTF8)
Write-Host "BIM done. New total: $($newBim.Count) lines (was $($bimLines.Count))"

# ====== CODE COVERAGE: Edit tests/test_gold_pipeline.py ======

$testPath = 'c:\Users\mohdq\src\DataBricks-Sample1\tests\test_gold_pipeline.py'
$testLines = [System.IO.File]::ReadAllLines($testPath, [System.Text.Encoding]::UTF8)
Write-Host "Test file loaded: $($testLines.Count) lines"

# Find 'import pytest' line index
$reIdx = -1
for ($i = 0; $i -lt $testLines.Count; $i++) {
    if ($testLines[$i].Trim() -eq 'import pytest') { $reIdx = $i; break }
}
if ($reIdx -lt 0) { Write-Host "FAIL: 'import pytest' not found"; exit 1 }

# Check if 'import re' already present
$hasRe = $false
for ($i = 0; $i -lt $testLines.Count; $i++) {
    if ($testLines[$i].Trim() -eq 'import re') { $hasRe = $true; break }
}

# Build updated list with 'import re' inserted after 'import pytest'
$newTest = [System.Collections.Generic.List[string]]::new()
for ($i = 0; $i -lt $testLines.Count; $i++) {
    $newTest.Add($testLines[$i])
    if ($i -eq $reIdx -and -not $hasRe) {
        $newTest.Add('import re')
        Write-Host "Inserted 'import re' after line $($reIdx + 1)"
    }
}

# Append TestStreamingTableConversions class
$newTest.Add('')
$newTest.Add('')
$newTest.Add('class TestStreamingTableConversions:')
$newTest.Add('    """Verify 10 gold DLT functions use readStream (-> Streaming Table) and')
$newTest.Add('       the remaining keep batch reads (-> Materialized View)."""')
$newTest.Add('')
$newTest.Add('    STREAMING_TABLES = [')
$newTest.Add('        "gold_dim_vendor", "gold_dim_project", "gold_dim_material",')
$newTest.Add('        "gold_fact_purchase_orders", "gold_fact_invoices",')
$newTest.Add('        "gold_fact_goods_receipts", "gold_fact_project_costs",')
$newTest.Add('        "gold_fact_vendor_performance", "gold_fact_contracts",')
$newTest.Add('        "gold_fact_sales",')
$newTest.Add('    ]')
$newTest.Add('    BATCH_TABLES = [')
$newTest.Add('        "gold_dim_employee", "gold_dim_geography", "gold_dim_contract",')
$newTest.Add('        "gold_dim_cost_center", "gold_dim_sector",')
$newTest.Add('        "gold_fact_project_actuals", "gold_fact_inventory",')
$newTest.Add('    ]')
$newTest.Add('')
$newTest.Add('    @pytest.fixture(scope="class")')
$newTest.Add('    def source_text(self):')
$newTest.Add('        with open(GOLD_PATH, "r", encoding="utf-8") as f:')
$newTest.Add('            return f.read()')
$newTest.Add('')
$newTest.Add('    def test_streaming_tables_use_readstream(self, source_text):')
$newTest.Add('        for fn in self.STREAMING_TABLES:')
$newTest.Add('            pattern = rf"def {fn}\(\):(.*?)(?=def |\# COMMAND)"')
$newTest.Add('            match = re.search(pattern, source_text, re.DOTALL)')
$newTest.Add('            assert match, f"{fn} function body not found in source"')
$newTest.Add('            assert "readStream.table" in match.group(1), \')
$newTest.Add('                f"{fn} should use readStream.table but uses batch read"')
$newTest.Add('')
$newTest.Add('    def test_batch_tables_do_not_use_readstream(self, source_text):')
$newTest.Add('        for fn in self.BATCH_TABLES:')
$newTest.Add('            pattern = rf"def {fn}\(\):(.*?)(?=def |\# COMMAND)"')
$newTest.Add('            match = re.search(pattern, source_text, re.DOTALL)')
$newTest.Add('            if match:')
$newTest.Add('                assert "readStream.table" not in match.group(1), \')
$newTest.Add('                    f"{fn} should NOT use readStream.table"')
$newTest.Add('')
$newTest.Add('    def test_stream_static_lookup_reads_preserved(self, source_text):')
$newTest.Add('        assert ''spark.read.table(f"{CATALOG}.procurement_silver.silver_po_line_items")'' in source_text')
$newTest.Add('        assert ''spark.read.table(f"{CATALOG}.procurement_silver.silver_purchase_orders").select('' in source_text')
$newTest.Add('        assert ''spark.read.table(f"{CATALOG}.procurement_silver.silver_materials").select('' in source_text')
$newTest.Add('')
$newTest.Add('    def test_dlt_table_decorator_count(self, source_text):')
$newTest.Add('        count = source_text.count("@dlt.table(")')
$newTest.Add('        assert count == 18, f"Expected 18 @dlt.table decorators, found {count}"')

[System.IO.File]::WriteAllLines($testPath, $newTest, [System.Text.Encoding]::UTF8)
Write-Host "Test file saved. Total lines: $($newTest.Count)"

Write-Host ""
Write-Host "=== ALL DONE ==="
