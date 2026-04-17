# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer &ndash; Star Schema (Dimensional Model)
# MAGIC Builds **dimensions** and **fact tables** from the Silver layer.
# MAGIC | Layer | Tables |
# MAGIC |-------|--------|
# MAGIC | Dimensions | dim_date, dim_vendor, dim_project, dim_material, dim_employee, dim_geography, dim_contract, dim_cost_center, dim_sector |
# MAGIC | Facts | fact_purchase_orders, fact_invoices, fact_goods_receipts, fact_project_costs, fact_project_actuals, fact_vendor_performance, fact_inventory, fact_contracts, fact_sales |

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, when, trim, upper, lit, current_timestamp, to_date,
    year, month, dayofmonth, dayofweek, quarter, date_format,
    concat, lpad, coalesce, round as spark_round, sha2,
    sum as spark_sum, count as spark_count, max as spark_max
)
from pyspark.sql.types import BooleanType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensions

# COMMAND ----------

@dlt.table(
    name="dim_date",
    comment="Date dimension - full calendar 2020-2030",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_dim_date():
    df = spark.sql(
        "SELECT explode(sequence("
        "to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day"
        ")) AS full_date"
    )
    return df.select(
        date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        col("full_date").alias("date"),
        year("full_date").alias("year"),
        quarter("full_date").alias("quarter"),
        month("full_date").alias("month"),
        dayofmonth("full_date").alias("day"),
        dayofweek("full_date").alias("day_of_week"),
        date_format("full_date", "EEEE").alias("day_name"),
        date_format("full_date", "MMMM").alias("month_name"),
        concat(
            year("full_date").cast("string"), lit("-"),
            lpad(month("full_date").cast("string"), 2, "0")
        ).alias("year_month"),
        when(month("full_date") <= 6,
            concat(lit("FY"), year("full_date").cast("string"), lit("-H1"))
        ).otherwise(
            concat(lit("FY"), year("full_date").cast("string"), lit("-H2"))
        ).alias("fiscal_period"),
        year("full_date").alias("fiscal_year"),
        quarter("full_date").alias("fiscal_quarter")
    )

# COMMAND ----------

@dlt.table(
    name="dim_vendor",
    comment="Vendor dimension with tier and status derivations",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_dim_vendor():
    df = dlt.read("silver_vendors")
    return df.select(
        col("vendor_id"),
        col("vendor_code"),
        col("vendor_name"),
        col("vendor_type"),
        col("sector_specialization").alias("vendor_sector"),
        upper(col("sector_specialization")).alias("sector"),
        col("country"),
        col("state"),
        col("city"),
        col("country").alias("vendor_country"),
        col("state").alias("vendor_state"),
        col("city").alias("vendor_city"),
        col("payment_terms_days"),
        col("credit_limit_usd"),
        col("performance_rating"),
        col("risk_category"),
        col("prequalification_status"),
        col("prequalification_expiry"),
        col("iso_certified"),
        col("iso_cert_number"),
        col("hse_rating"),
        col("minority_owned"),
        col("small_business"),
        col("primary_contact"),
        col("contact_email"),
        col("contact_phone"),
        col("annual_turnover_usd"),
        col("years_in_business"),
        when(col("performance_rating") >= 4, lit("STRATEGIC"))
            .when(col("performance_rating") >= 3, lit("PREFERRED"))
            .otherwise(lit("STANDARD")).alias("tier"),
        when(upper(col("prequalification_status")) == "APPROVED", lit("ACTIVE"))
            .when(upper(col("prequalification_status")) == "PENDING", lit("PENDING"))
            .otherwise(lit("INACTIVE")).alias("status"),
        lit(True).cast(BooleanType()).alias("_is_current"),
        current_timestamp().alias("_gold_timestamp")
    )

# COMMAND ----------

@dlt.table(
    name="dim_project",
    comment="Project dimension with region derivation",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_dim_project():
    df = dlt.read("silver_projects")
    return df.select(
        col("project_id"),
        col("project_code"),
        col("project_name"),
        col("project_type"),
        col("sector"),
        col("sub_sector"),
        col("client_name"),
        col("project_manager"),
        col("country"),
        col("state"),
        col("city"),
        col("contract_value_usd"),
        col("approved_budget_usd"),
        col("project_status"),
        col("project_status").alias("status"),
        col("start_date"),
        col("planned_completion_date").alias("end_date"),
        col("actual_completion_date"),
        col("planned_duration_days").alias("project_duration_days"),
        col("budget_variance_pct"),
        col("phase"),
        col("priority"),
        col("risk_level"),
        when(col("country").isin("US", "CA", "MX"), lit("NORTH AMERICA"))
            .when(col("country").isin("GB", "DE", "FR", "IT", "ES", "NL", "NO", "SE"), lit("EUROPE"))
            .when(col("country").isin("AE", "SA", "QA", "KW", "OM", "BH"), lit("MIDDLE EAST"))
            .when(col("country").isin("AU", "NZ", "SG", "MY", "IN", "JP", "KR", "CN"), lit("ASIA PACIFIC"))
            .otherwise(lit("OTHER")).alias("region"),
        lit(True).cast(BooleanType()).alias("_is_current"),
        current_timestamp().alias("_gold_timestamp")
    )

# COMMAND ----------

@dlt.table(
    name="dim_material",
    comment="Material dimension",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_dim_material():
    df = dlt.read("silver_materials")
    return df.select(
        col("material_id"),
        col("material_code"),
        col("material_name"),
        col("category"),
        col("category").alias("material_category"),
        col("sub_category"),
        col("unit_price"),
        col("weight_kg"),
        col("lead_time_days"),
        col("is_active"),
        lit(True).cast(BooleanType()).alias("_is_current"),
        current_timestamp().alias("_gold_timestamp")
    )

# COMMAND ----------

@dlt.table(
    name="dim_employee",
    comment="Employee dimension",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_dim_employee():
    df = dlt.read("silver_employees")
    return df.select(
        col("employee_id"),
        col("employee_code"),
        col("full_name"),
        col("department"),
        col("job_title"),
        col("grade"),
        col("approval_limit"),
        col("is_active"),
        col("location"),
        col("cost_center"),
        col("hire_date"),
        lit(True).cast(BooleanType()).alias("_is_current"),
        current_timestamp().alias("_gold_timestamp")
    )

# COMMAND ----------

@dlt.table(
    name="dim_geography",
    comment="Geography dimension - distinct country/state/city with region",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_dim_geography():
    projects = dlt.read("silver_projects").select("country", "state", "city")
    vendors = dlt.read("silver_vendors").select("country", "state", "city")
    geo = projects.union(vendors).distinct()
    return geo.select(
        sha2(concat(col("country"), lit("|"), col("state"), lit("|"), col("city")), 256).alias("geography_key"),
        col("country"),
        col("state"),
        col("city"),
        when(col("country").isin("US", "CA", "MX"), lit("NORTH AMERICA"))
            .when(col("country").isin("GB", "DE", "FR", "IT", "ES", "NL", "NO", "SE"), lit("EUROPE"))
            .when(col("country").isin("AE", "SA", "QA", "KW", "OM", "BH"), lit("MIDDLE EAST"))
            .when(col("country").isin("AU", "NZ", "SG", "MY", "IN", "JP", "KR", "CN"), lit("ASIA PACIFIC"))
            .otherwise(lit("OTHER")).alias("region"),
        lit(True).cast(BooleanType()).alias("_is_current"),
        current_timestamp().alias("_gold_timestamp")
    )

# COMMAND ----------

@dlt.table(
    name="dim_contract",
    comment="Contract dimension with cost growth derivation",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_dim_contract():
    df = dlt.read("silver_contracts")
    return df.select(
        col("contract_id"),
        col("vendor_id"),
        col("project_id"),
        col("contract_type"),
        col("contract_status"),
        col("contract_status").alias("status"),
        col("contract_value"),
        col("contract_value").alias("original_value"),
        (col("contract_value") + coalesce(col("change_order_value"), lit(0))).alias("revised_value"),
        col("start_date"),
        col("end_date"),
        col("contract_duration_days"),
        col("payment_terms"),
        col("penalty_clause"),
        col("performance_bond"),
        col("retention_pct"),
        col("change_order_count"),
        col("change_order_value"),
        when(col("contract_value") > 0,
            spark_round(coalesce(col("change_order_value"), lit(0)) / col("contract_value") * 100, 2)
        ).otherwise(lit(0.0)).alias("cost_growth_pct"),
        col("approved_by"),
        col("created_date"),
        lit(True).cast(BooleanType()).alias("_is_current"),
        current_timestamp().alias("_gold_timestamp")
    )

# COMMAND ----------

@dlt.table(
    name="dim_cost_center",
    comment="Cost center dimension - derived from employee data",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_dim_cost_center():
    df = dlt.read("silver_employees")
    return df.select("cost_center", "department").distinct().select(
        col("cost_center").alias("cost_center_key"),
        col("cost_center").alias("cost_center_code"),
        col("department").alias("cost_center_category"),
        lit(True).cast(BooleanType()).alias("_is_current"),
        current_timestamp().alias("_gold_timestamp")
    )

# COMMAND ----------

@dlt.table(
    name="dim_sector",
    comment="Sector dimension - union of vendor and project sectors",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_dim_sector():
    v_sec = dlt.read("silver_vendors").select(
        upper(trim(col("sector_specialization"))).alias("sector_name")
    )
    p_sec = dlt.read("silver_projects").select(
        upper(trim(col("sector"))).alias("sector_name")
    )
    return v_sec.union(p_sec).distinct().where(col("sector_name").isNotNull()).select(
        col("sector_name").alias("sector_key"),
        col("sector_name"),
        lit(True).cast(BooleanType()).alias("_is_current"),
        current_timestamp().alias("_gold_timestamp")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Facts

# COMMAND ----------

@dlt.table(
    name="fact_purchase_orders",
    comment="Purchase order facts with line-item aggregates",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_fact_purchase_orders():
    po = dlt.read("silver_purchase_orders")
    poli_agg = (
        dlt.read("silver_po_line_items")
        .groupBy("po_id")
        .agg(
            spark_sum("line_value").alias("line_total"),
            spark_sum("qty_ordered").alias("total_quantity")
        )
    )
    df = po.join(poli_agg, "po_id", "left")
    return df.select(
        col("po_id"),
        col("vendor_id"),
        col("project_id"),
        col("po_date"),
        col("delivery_date"),
        col("delivery_date").alias("expected_delivery_date"),
        col("approval_date"),
        col("po_value"),
        col("tax_amount"),
        col("total_value"),
        col("total_value").alias("total_amount"),
        col("po_status"),
        col("po_type"),
        col("priority"),
        col("currency"),
        col("payment_terms"),
        coalesce(col("line_total"), lit(0)).alias("line_total"),
        coalesce(col("total_quantity"), lit(0)).alias("total_quantity"),
        current_timestamp().alias("_gold_timestamp")
    )

# COMMAND ----------

@dlt.table(
    name="fact_invoices",
    comment="Invoice facts with aging analysis",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_fact_invoices():
    df = dlt.read("silver_invoices")
    return df.select(
        col("invoice_id"),
        col("vendor_id"),
        col("po_id"),
        col("project_id"),
        col("contract_id"),
        col("invoice_number"),
        col("invoice_status"),
        col("invoice_type"),
        col("payment_status"),
        col("invoice_date"),
        col("received_date"),
        col("due_date"),
        col("payment_date"),
        col("invoice_amount"),
        col("invoice_amount").alias("net_amount"),
        col("tax_rate"),
        col("tax_amount"),
        col("total_amount"),
        col("total_amount").alias("gross_amount"),
        col("paid_amount"),
        col("retention_amount"),
        col("days_to_pay"),
        col("days_overdue"),
        col("days_overdue").alias("days_outstanding"),
        when(col("days_overdue") <= 0, lit("CURRENT"))
            .when(col("days_overdue") <= 30, lit("1-30 DAYS"))
            .when(col("days_overdue") <= 60, lit("31-60 DAYS"))
            .when(col("days_overdue") <= 90, lit("61-90 DAYS"))
            .otherwise(lit("90+ DAYS")).alias("aging_bucket"),
        col("three_way_match"),
        col("grn_ref"),
        col("currency"),
        col("approved_by"),
        col("notes"),
        current_timestamp().alias("_gold_timestamp")
    )

# COMMAND ----------

@dlt.table(
    name="fact_goods_receipts",
    comment="Goods receipt facts with quality metrics",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_fact_goods_receipts():
    gr = dlt.read("silver_goods_receipts")
    po = dlt.read("silver_purchase_orders").select(
        col("po_id").alias("_po_id"), col("project_id")
    )
    poli = dlt.read("silver_po_line_items").select(
        col("line_id").alias("_poli_line_id"),
        col("unit_price").alias("_poli_unit_price")
    )
    df = (
        gr.join(po, gr["po_id"] == po["_po_id"], "left")
          .drop("_po_id")
          .join(poli, gr["line_item_id"] == poli["_poli_line_id"], "left")
          .drop("_poli_line_id")
    )
    return df.select(
        col("grn_id"),
        col("po_id"),
        col("line_item_id"),
        col("vendor_id"),
        col("material_id"),
        col("project_id"),
        col("receipt_date"),
        col("qty_received").alias("quantity_received"),
        col("qty_accepted").alias("quantity_accepted"),
        col("qty_rejected").alias("quantity_rejected"),
        col("rejection_rate"),
        (lit(100) - col("rejection_rate")).alias("acceptance_rate"),
        (col("qty_accepted") * coalesce(col("_poli_unit_price"), lit(0))).alias("receipt_value"),
        col("grn_status"),
        col("storage_location"),
        col("inspector"),
        col("remarks"),
        col("created_date"),
        current_timestamp().alias("_gold_timestamp")
    )

# COMMAND ----------

@dlt.table(
    name="fact_project_costs",
    comment="Project cost facts - budget vs actual with variance",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_fact_project_costs():
    budgets = dlt.read("silver_project_budgets")
    actuals_agg = (
        dlt.read("silver_project_actuals")
        .groupBy("project_id", "wbs_code", "cost_type")
        .agg(
            spark_sum("actual_amount").alias("_act_total"),
            spark_max("posting_date").alias("_act_posting_date")
        )
    )
    df = (
        budgets.alias("b")
        .join(
            actuals_agg.alias("a"),
            (col("b.project_id") == col("a.project_id"))
            & (col("b.wbs_code") == col("a.wbs_code"))
            & (upper(col("b.cost_type")) == col("a.cost_type")),
            "left",
        )
    )
    budget_col = col("b.revised_amount")
    actual_col = coalesce(col("a._act_total"), col("b.actual_spent"))
    return df.select(
        col("b.budget_id"),
        col("b.project_id"),
        col("b.wbs_code").alias("wbs_element"),
        col("b.wbs_code"),
        col("b.cost_type"),
        budget_col.alias("budget_amount"),
        col("b.original_amount").alias("original_budget"),
        col("b.committed_amount"),
        actual_col.alias("actual_amount"),
        (budget_col - actual_col).alias("remaining_amount"),
        when(budget_col > 0,
            spark_round(actual_col / budget_col * 100, 2)
        ).otherwise(lit(0.0)).alias("utilization_pct"),
        (budget_col - actual_col).alias("variance_amount"),
        when(budget_col > 0,
            spark_round((budget_col - actual_col) / budget_col * 100, 2)
        ).otherwise(lit(0.0)).alias("variance_pct"),
        col("b.budget_status"),
        col("b.period_start"),
        col("b.period_end"),
        col("a._act_posting_date").alias("posting_date"),
        current_timestamp().alias("_gold_timestamp")
    )

# COMMAND ----------

@dlt.table(
    name="fact_project_actuals",
    comment="Project actuals detail facts",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_fact_project_actuals():
    df = dlt.read("silver_project_actuals")
    return df.select(
        col("transaction_id"),
        col("project_id"),
        col("vendor_id"),
        col("po_id"),
        col("contract_id"),
        col("invoice_id"),
        col("wbs_code").alias("wbs_element"),
        col("wbs_code"),
        col("cost_type"),
        col("cost_status"),
        col("transaction_date"),
        col("posting_date"),
        col("actual_amount"),
        col("budget_amount"),
        col("variance"),
        col("variance").alias("variance_amount"),
        col("fiscal_period"),
        current_timestamp().alias("_gold_timestamp")
    )

# COMMAND ----------

@dlt.table(
    name="fact_vendor_performance",
    comment="Vendor performance evaluation facts",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_fact_vendor_performance():
    df = dlt.read("silver_vendor_performance")
    return df.select(
        col("performance_id"),
        col("vendor_id"),
        col("project_id"),
        col("evaluation_date"),
        col("evaluation_period"),
        col("delivery_score"),
        col("quality_score"),
        col("commercial_score"),
        col("hse_score"),
        col("compliance_score"),
        col("overall_score"),
        col("recommendation"),
        col("evaluator_id"),
        col("notes"),
        col("created_date"),
        current_timestamp().alias("_gold_timestamp")
    )

# COMMAND ----------

@dlt.table(
    name="fact_inventory",
    comment="Inventory movement facts",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_fact_inventory():
    df = dlt.read("silver_inventory")
    return df.select(
        col("movement_id"),
        col("material_id"),
        col("project_id"),
        col("grn_id"),
        col("movement_type"),
        col("movement_date"),
        col("quantity"),
        col("unit_cost"),
        col("total_value"),
        col("from_location"),
        col("to_location"),
        col("batch_number"),
        col("unit_of_measure"),
        col("reason_code"),
        col("posted_by"),
        col("approved_by"),
        col("remarks"),
        col("created_date"),
        current_timestamp().alias("_gold_timestamp")
    )

# COMMAND ----------

@dlt.table(
    name="fact_contracts",
    comment="Contract facts with item aggregates",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_fact_contracts():
    contracts = dlt.read("silver_contracts")
    items_agg = (
        dlt.read("silver_contract_items")
        .groupBy("contract_id")
        .agg(
            spark_sum("line_value").alias("items_total"),
            spark_count("item_id").alias("item_count")
        )
    )
    df = contracts.join(items_agg, "contract_id", "left")
    return df.select(
        col("contract_id"),
        col("vendor_id"),
        col("project_id"),
        col("contract_type"),
        col("contract_status"),
        col("contract_value"),
        (col("contract_value") + coalesce(col("change_order_value"), lit(0))).alias("revised_value"),
        col("start_date"),
        col("end_date"),
        col("contract_duration_days"),
        col("change_order_count"),
        col("change_order_value"),
        coalesce(col("items_total"), lit(0)).alias("items_total"),
        coalesce(col("item_count"), lit(0)).alias("item_count"),
        when(col("contract_value") > 0,
            spark_round(coalesce(col("items_total"), lit(0)) / col("contract_value") * 100, 2)
        ).otherwise(lit(0.0)).alias("value_utilisation_pct"),
        col("payment_terms"),
        col("penalty_clause"),
        col("performance_bond"),
        col("retention_pct"),
        current_timestamp().alias("_gold_timestamp")
    )

# COMMAND ----------

@dlt.table(
    name="fact_sales",
    comment="Sales order facts",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_fact_sales():
    sales = dlt.read("silver_sales_orders")
    materials = dlt.read("silver_materials").select(
        col("material_id").alias("_mat_id"),
        col("material_name").alias("product_name")
    )
    df = sales.join(materials, sales["material_id"] == materials["_mat_id"], "left").drop("_mat_id")
    return df.select(
        col("sales_order_id"),
        col("project_id"),
        col("material_id"),
        col("material_id").alias("product_id"),
        col("product_name"),
        col("sales_rep_id"),
        col("order_type"),
        col("order_status"),
        col("payment_status"),
        col("customer_name"),
        sha2(col("customer_name"), 256).alias("customer_id"),
        col("customer_type"),
        col("customer_type").alias("sector"),
        col("order_date"),
        col("order_date").alias("sale_date"),
        col("delivery_date"),
        col("quantity"),
        col("unit_price"),
        col("revenue"),
        col("cogs"),
        col("gross_margin"),
        col("gross_margin").alias("gross_margin_amount"),
        col("margin_pct"),
        col("tax_amount"),
        col("total_amount"),
        col("currency"),
        current_timestamp().alias("_gold_timestamp")
    )