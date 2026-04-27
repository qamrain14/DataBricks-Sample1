Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ─── Part E: Insert measures block into CubeProcurementSpend in BIM ───────────
$bimPath = 'c:\Users\mohdq\src\DataBricks-Sample1\powerbi\model\procurement_model.bim'
$bimLines = [System.IO.File]::ReadAllLines($bimPath, [System.Text.Encoding]::UTF8)

# Confirm target position: line 3876 (1-indexed) = index 3875 closes columns ],
# line 3877 (1-indexed) = index 3876 starts "partitions": [
$idx = 3875  # 0-based: line 3876
$check1 = $bimLines[$idx]
$check2 = $bimLines[$idx + 1]
Write-Host "BIM line 3876 (1-idx): '$check1'"
Write-Host "BIM line 3877 (1-idx): '$check2'"
if (-not $check1.Contains('],') -or -not $check2.Contains('"partitions"')) {
    Write-Host "ERROR: Unexpected BIM context at index $idx. Aborting."
    exit 1
}

# Double-check this is CubeProcurementSpend by searching backwards for TotalLineValue
# (unique to that table) within 60 lines
$found = $false
for ($k = $idx; $k -ge [Math]::Max(0, $idx - 60); $k--) {
    if ($bimLines[$k].Contains('"TotalLineValue"')) { $found = $true; break }
}
if (-not $found) {
    Write-Host "ERROR: TotalLineValue not found near line 3876 — wrong table. Aborting."
    exit 1
}
Write-Host "Confirmed: insertion point is inside CubeProcurementSpend table."

$measuresBlock = @(
    '        "measures": [',
    '          {',
    '            "name": "Spend by Sector",',
    '            "expression": "SUM(CubeProcurementSpend[TotalSpend])",',
    '            "formatString": "$#,##0.00"',
    '          },',
    '          {',
    '            "name": "Vendor Count",',
    '            "expression": "DISTINCTCOUNT(CubeProcurementSpend[VendorId])",',
    '            "formatString": "#,##0"',
    '          },',
    '          {',
    '            "name": "YoY Growth",',
    '            "expression": "AVERAGE(CubeProcurementSpend[YoyGrowthPct])",',
    '            "formatString": "0.00%"',
    '          }',
    '        ],'
)

$newBim = [System.Collections.Generic.List[string]]::new()
for ($i = 0; $i -lt $bimLines.Count; $i++) {
    $newBim.Add($bimLines[$i])
    if ($i -eq $idx) {
        foreach ($m in $measuresBlock) { $newBim.Add($m) }
    }
}
[System.IO.File]::WriteAllLines($bimPath, $newBim, [System.Text.Encoding]::UTF8)
Write-Host "BIM: inserted $($measuresBlock.Count) measure lines after column close. Total lines: $($newBim.Count)"

# ─── Code Coverage: Edit tests/test_gold_pipeline.py ─────────────────────────
$testPath = 'c:\Users\mohdq\src\DataBricks-Sample1\tests\test_gold_pipeline.py'
$testLines = [System.IO.File]::ReadAllLines($testPath, [System.Text.Encoding]::UTF8)
Write-Host "Test file: $($testLines.Count) lines"

# 1. Add "import re" after "import pytest" (line 4, 1-indexed = index 3)
$reIdx = -1
for ($i = 0; $i -lt $testLines.Count; $i++) {
    if ($testLines[$i].Trim() -eq 'import pytest') { $reIdx = $i; break }
}
if ($reIdx -lt 0) { Write-Host "ERROR: 'import pytest' not found. Aborting."; exit 1 }

$alreadyHasRe = $false
for ($i = 0; $i -lt $testLines.Count; $i++) {
    if ($testLines[$i].Trim() -eq 'import re') { $alreadyHasRe = $true; break }
}

$newTest = [System.Collections.Generic.List[string]]::new()
for ($i = 0; $i -lt $testLines.Count; $i++) {
    $newTest.Add($testLines[$i])
    if ($i -eq $reIdx -and -not $alreadyHasRe) {
        $newTest.Add('import re')
        Write-Host "Inserted 'import re' after line $($reIdx+1) (1-idx)"
    }
}

# 2. Append TestStreamingTableConversions class
$classLines = @(
    '',
    '',
    'class TestStreamingTableConversions:',
    '    """Verify 10 gold DLT functions use readStream (-> Streaming Table) and',
    '       the remaining keep batch reads (-> Materialized View)."""',
    '',
    '    STREAMING_TABLES = [',
    '        "gold_dim_vendor", "gold_dim_project", "gold_dim_material",',
    '        "gold_fact_purchase_orders", "gold_fact_invoices",',
    '        "gold_fact_goods_receipts", "gold_fact_project_costs",',
    '        "gold_fact_vendor_performance", "gold_fact_contracts",',
    '        "gold_fact_sales",',
    '    ]',
    '    BATCH_TABLES = [',
    '        "gold_dim_employee", "gold_dim_geography", "gold_dim_contract",',
    '        "gold_dim_cost_center", "gold_dim_sector",',
    '        "gold_fact_project_actuals", "gold_fact_inventory",',
    '    ]',
    '',
    '    @pytest.fixture(scope="class")',
    '    def source_text(self):',
    '        with open(GOLD_PATH, "r", encoding="utf-8") as f:',
    '            return f.read()',
    '',
    '    def test_streaming_tables_use_readstream(self, source_text):',
    '        for fn in self.STREAMING_TABLES:',
    '            pattern = rf"def {fn}\(\):(.*?)(?=def |\# COMMAND)"',
    '            match = re.search(pattern, source_text, re.DOTALL)',
    '            assert match, f"{fn} function body not found in source"',
    '            assert "readStream.table" in match.group(1), \',
    '                f"{fn} should use readStream.table but uses batch read"',
    '',
    '    def test_batch_tables_do_not_use_readstream(self, source_text):',
    '        for fn in self.BATCH_TABLES:',
    '            pattern = rf"def {fn}\(\):(.*?)(?=def |\# COMMAND)"',
    '            match = re.search(pattern, source_text, re.DOTALL)',
    '            if match:',
    '                assert "readStream.table" not in match.group(1), \',
    '                    f"{fn} should NOT use readStream.table"',
    '',
    '    def test_stream_static_lookup_reads_preserved(self, source_text):',
    '        assert ''spark.read.table(f"{CATALOG}.procurement_silver.silver_po_line_items")'' in source_text',
    '        assert ''spark.read.table(f"{CATALOG}.procurement_silver.silver_purchase_orders").select('' in source_text',
    '        assert ''spark.read.table(f"{CATALOG}.procurement_silver.silver_materials").select('' in source_text',
    '',
    '    def test_dlt_table_decorator_count(self, source_text):',
    '        count = source_text.count("@dlt.table(")',
    '        assert count == 18, f"Expected 18 @dlt.table decorators, found {count}"'
)

foreach ($cl in $classLines) { $newTest.Add($cl) }
Write-Host "Appended TestStreamingTableConversions ($($classLines.Count) lines)"

[System.IO.File]::WriteAllLines($testPath, $newTest, [System.Text.Encoding]::UTF8)
Write-Host "Test file saved. Total lines: $($newTest.Count)"

Write-Host ""
Write-Host "=== ALL DONE ==="
