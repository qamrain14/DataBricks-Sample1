# Databricks notebook source
"""
Power BI Connection Configuration for Databricks Procurement Data Platform.

Centralises all connection details, table metadata, and column schemas
so that Power Query M generators, BIM builders, and deploy scripts can
reference a single source of truth.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# Databricks connection settings
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class DatabricksConnection:
    """Immutable connection parameters for the Databricks SQL Warehouse."""

    host: str = "dbc-760a206e-c226.cloud.databricks.com"
    http_path: str = "/sql/1.0/warehouses/7655fa24e271f9d1"
    catalog: str = "workspace"
    port: int = 443
    driver: str = "Simba Spark ODBC Driver"
    dsn_name: str = "Databricks_Procurement"
    auth_type: str = "pat"  # personal-access-token

    @property
    def jdbc_url(self) -> str:
        return (
            f"jdbc:spark://{self.host}:{self.port}/default"
            f";transportMode=http"
            f";ssl=1"
            f";httpPath={self.http_path}"
        )

    @property
    def odbc_connection_string(self) -> str:
        return (
            f"Driver={{{self.driver}}};"
            f"Host={self.host};"
            f"Port={self.port};"
            f"HTTPPath={self.http_path};"
            f"ThriftTransport=2;"
            f"SSL=1;"
            f"AuthMech=3;"
            f"Catalog={self.catalog};"
        )


CONNECTION = DatabricksConnection()


# ---------------------------------------------------------------------------
# Table metadata helper
# ---------------------------------------------------------------------------
@dataclass
class TableMeta:
    """Metadata for a single table or cube."""

    schema: str
    table: str
    columns: List[str]
    description: str = ""
    refresh_mode: str = "import"  # import | directquery
    partition_key: Optional[str] = None

    @property
    def full_name(self) -> str:
        return f"{CONNECTION.catalog}.{self.schema}.{self.table}"


# ---------------------------------------------------------------------------
# Gold dimension tables
# ---------------------------------------------------------------------------
GOLD_DIMENSIONS: Dict[str, TableMeta] = {
    "dim_date": TableMeta(
        schema="procurement_gold",
        table="dim_date",
        description="Calendar dimension with fiscal periods",
        columns=[
            "date_key", "date", "year", "quarter", "month", "day",
            "day_of_week", "day_name", "month_name", "year_month",
            "fiscal_period", "fiscal_year", "fiscal_quarter",
        ],
    ),
    "dim_vendor": TableMeta(
        schema="procurement_gold",
        table="dim_vendor",
        description="Vendor master dimension",
        columns=[
            "vendor_id", "vendor_code", "vendor_name", "vendor_type",
            "vendor_sector", "sector", "vendor_country", "vendor_state",
            "vendor_city", "payment_terms_days", "credit_limit_usd",
            "performance_rating", "risk_category", "prequalification_status",
            "prequalification_expiry", "iso_certified", "hse_rating",
            "minority_owned", "small_business", "primary_contact",
            "contact_email", "contact_phone", "annual_turnover_usd",
            "years_in_business", "tier", "status",
            "_is_current", "_gold_timestamp",
        ],
    ),
    "dim_project": TableMeta(
        schema="procurement_gold",
        table="dim_project",
        description="Project master dimension",
        columns=[
            "project_id", "project_code", "project_name", "project_type",
            "client_name", "sector", "state", "project_manager",
            "contract_value_usd", "approved_budget_usd", "project_status",
            "status", "start_date", "end_date", "actual_completion_date",
            "project_duration_days", "priority", "risk_level", "region",
            "_is_current", "_gold_timestamp",
        ],
    ),
    "dim_material": TableMeta(
        schema="procurement_gold",
        table="dim_material",
        description="Material master dimension",
        columns=[
            "material_id", "material_code", "material_name", "category",
            "material_category", "sub_category", "unit_price", "weight_kg",
            "lead_time_days", "is_active", "_is_current", "_gold_timestamp",
        ],
    ),
    "dim_employee": TableMeta(
        schema="procurement_gold",
        table="dim_employee",
        description="Employee / approver dimension",
        columns=[
            "employee_id", "employee_code", "full_name", "department",
            "job_title", "grade", "approval_limit", "location",
            "is_active", "cost_center", "hire_date",
            "_is_current", "_gold_timestamp",
        ],
    ),
    "dim_geography": TableMeta(
        schema="procurement_gold",
        table="dim_geography",
        description="Geographic hierarchy dimension",
        columns=[
            "geography_key", "country", "state", "city", "region",
            "_is_current", "_gold_timestamp",
        ],
    ),
    "dim_contract": TableMeta(
        schema="procurement_gold",
        table="dim_contract",
        description="Contract dimension",
        columns=[
            "contract_id", "vendor_id", "project_id", "contract_type",
            "contract_status", "status", "contract_value", "original_value",
            "revised_value", "start_date", "end_date",
            "contract_duration_days", "payment_terms", "retention_pct",
            "performance_bond", "change_order_count", "change_order_value",
            "cost_growth_pct", "approved_by",
            "_is_current", "_gold_timestamp",
        ],
    ),
    "dim_cost_center": TableMeta(
        schema="procurement_gold",
        table="dim_cost_center",
        description="Cost-centre dimension",
        columns=[
            "cost_center_key", "cost_center_code", "cost_center_category",
            "_is_current", "_gold_timestamp",
        ],
    ),
    "dim_sector": TableMeta(
        schema="procurement_gold",
        table="dim_sector",
        description="Sector / industry dimension",
        columns=[
            "sector_key", "sector_name",
            "_is_current", "_gold_timestamp",
        ],
    ),
}


# ---------------------------------------------------------------------------
# Gold fact tables
# ---------------------------------------------------------------------------
GOLD_FACTS: Dict[str, TableMeta] = {
    "fact_purchase_orders": TableMeta(
        schema="procurement_gold",
        table="fact_purchase_orders",
        description="Purchase order facts with line-item aggregates",
        columns=[
            "po_id", "vendor_id", "project_id", "po_date",
            "delivery_date", "expected_delivery_date", "approval_date",
            "po_value", "tax_amount", "total_value", "total_amount",
            "po_status", "po_type", "priority", "currency",
            "payment_terms", "line_total", "total_quantity",
            "_gold_timestamp",
        ],
    ),
    "fact_invoices": TableMeta(
        schema="procurement_gold",
        table="fact_invoices",
        description="Invoice facts with aging analysis",
        columns=[
            "invoice_id", "vendor_id", "po_id", "project_id",
            "contract_id", "invoice_number", "invoice_type",
            "payment_status", "invoice_date", "due_date", "payment_date",
            "invoice_amount", "net_amount", "tax_rate", "tax_amount",
            "total_amount", "gross_amount", "paid_amount",
            "retention_amount", "days_to_pay", "days_overdue",
            "days_outstanding", "aging_bucket", "currency",
            "approved_by", "notes", "_gold_timestamp",
        ],
    ),
    "fact_goods_receipts": TableMeta(
        schema="procurement_gold",
        table="fact_goods_receipts",
        description="Goods receipt facts with quality metrics",
        columns=[
            "grn_id", "po_id", "line_item_id", "vendor_id",
            "material_id", "project_id", "receipt_date",
            "quantity_received", "quantity_accepted", "quantity_rejected",
            "rejection_rate", "acceptance_rate", "receipt_value",
            "grn_status", "storage_location", "inspector",
            "_gold_timestamp",
        ],
    ),
    "fact_project_costs": TableMeta(
        schema="procurement_gold",
        table="fact_project_costs",
        description="Project cost facts – budget vs actual with variance",
        columns=[
            "budget_id", "project_id", "wbs_element", "wbs_code",
            "cost_type", "budget_amount", "original_budget",
            "committed_amount", "actual_amount", "remaining_amount",
            "utilization_pct", "variance_amount", "variance_pct",
            "budget_status", "period_start", "period_end",
            "posting_date", "_gold_timestamp",
        ],
    ),
    "fact_project_actuals": TableMeta(
        schema="procurement_gold",
        table="fact_project_actuals",
        description="Project actuals detail facts",
        columns=[
            "transaction_id", "po_id", "invoice_id", "wbs_code",
            "cost_type", "cost_category", "cost_status",
            "transaction_date", "posting_date", "actual_amount",
            "budget_amount", "variance", "fiscal_period",
            "_gold_timestamp",
        ],
    ),
    "fact_vendor_performance": TableMeta(
        schema="procurement_gold",
        table="fact_vendor_performance",
        description="Vendor performance evaluation facts",
        columns=[
            "performance_id", "vendor_id", "project_id",
            "evaluation_date", "evaluation_period", "delivery_score",
            "quality_score", "commercial_score", "hse_score",
            "compliance_score", "overall_score", "recommendation",
            "evaluator_id", "notes", "_gold_timestamp",
        ],
    ),
    "fact_inventory": TableMeta(
        schema="procurement_gold",
        table="fact_inventory",
        description="Inventory movement facts",
        columns=[
            "movement_id", "material_id", "grn_id", "movement_type",
            "movement_date", "quantity", "unit_cost", "total_value",
            "from_location", "to_location", "batch_number",
            "unit_of_measure", "reason_code", "posted_by",
            "_gold_timestamp",
        ],
    ),
    "fact_contracts": TableMeta(
        schema="procurement_gold",
        table="fact_contracts",
        description="Contract facts with item aggregates",
        columns=[
            "contract_id", "vendor_id", "project_id", "contract_type",
            "contract_status", "contract_value", "revised_value",
            "start_date", "change_order_count", "change_order_value",
            "items_total", "item_count", "value_utilisation_pct",
            "payment_terms", "retention_pct", "penalty_clause",
            "performance_bond", "currency", "_gold_timestamp",
        ],
    ),
    "fact_sales": TableMeta(
        schema="procurement_gold",
        table="fact_sales",
        description="Sales order facts",
        columns=[
            "sales_order_id", "product_name", "customer_name",
            "customer_id", "customer_type", "order_type", "order_status",
            "payment_status", "order_date", "delivery_date", "quantity",
            "unit_price", "revenue", "cogs", "gross_margin",
            "margin_pct", "tax_amount", "total_amount", "currency",
            "delivery_terms", "load_timestamp",
        ],
    ),
}


# ---------------------------------------------------------------------------
# Semantic cubes (procurement_semantic schema)
# ---------------------------------------------------------------------------
SEMANTIC_CUBES: Dict[str, TableMeta] = {
    "cube_procurement_spend": TableMeta(
        schema="procurement_semantic",
        table="cube_procurement_spend",
        description="Overall procurement spend by vendor, project, and time",
        columns=[
            "vendor_id", "vendor_name", "vendor_sector", "project_id",
            "project_name", "year", "quarter", "year_month", "po_count",
            "total_spend", "total_line_value", "total_quantity",
            "avg_po_value", "prev_year_spend", "yoy_growth_pct",
        ],
    ),
    "cube_vendor_performance": TableMeta(
        schema="procurement_semantic",
        table="cube_vendor_performance",
        description="Vendor scorecard with evaluation metrics",
        columns=[
            "vendor_id", "vendor_name", "sector", "tier", "country",
            "evaluation_period", "avg_delivery_score", "avg_quality_score",
            "avg_commercial_score", "avg_hse_score", "avg_overall_score",
            "latest_recommendation", "evaluation_count", "vendor_rank",
            "score_trend",
        ],
    ),
    "cube_contract_status": TableMeta(
        schema="procurement_semantic",
        table="cube_contract_status",
        description="Contract status tracker with risk flags",
        columns=[
            "contract_id", "contract_type", "status", "vendor_id",
            "vendor_name", "project_id", "project_name", "start_date",
            "end_date", "original_value", "revised_value", "items_total",
            "item_count", "contract_duration_days", "cost_growth_pct",
            "value_utilisation_pct", "cost_risk_flag",
            "schedule_risk_flag", "remaining_value",
        ],
    ),
    "cube_invoice_aging": TableMeta(
        schema="procurement_semantic",
        table="cube_invoice_aging",
        description="Invoice aging analysis by vendor and project",
        columns=[
            "vendor_id", "vendor_name", "vendor_sector", "project_id",
            "project_name", "year", "quarter", "year_month",
            "aging_bucket", "invoice_count", "total_net_amount",
            "total_gross_amount", "total_tax_amount",
            "avg_days_outstanding", "max_days_outstanding",
            "overdue_amount", "overdue_count", "overdue_pct",
        ],
    ),
    "cube_project_costs": TableMeta(
        schema="procurement_semantic",
        table="cube_project_costs",
        description="Project cost overview with CPI and budget flags",
        columns=[
            "project_id", "project_name", "project_status", "sector",
            "region", "priority", "project_duration_days", "total_budget",
            "total_actual", "total_committed", "total_exposure",
            "total_variance", "avg_variance_pct", "cpi",
            "is_over_budget", "budget_utilisation_pct", "wbs_count",
            "cost_type_count",
        ],
    ),
    "cube_goods_receipt_quality": TableMeta(
        schema="procurement_semantic",
        table="cube_goods_receipt_quality",
        description="Goods receipt quality analysis",
        columns=[
            "vendor_id", "vendor_name", "vendor_sector", "material_id",
            "material_name", "material_category", "receipt_count",
            "total_qty_received", "total_qty_accepted",
            "total_qty_rejected", "avg_acceptance_rate",
            "min_acceptance_rate", "total_receipt_value",
            "defect_rate_pct", "quality_tier",
            "estimated_rejection_value",
        ],
    ),
    "cube_budget_vs_actual": TableMeta(
        schema="procurement_gold",
        table="cube_budget_vs_actual",
        description="Budget vs actual cost comparison",
        columns=[
            "project_id", "project_name", "sector", "region",
            "wbs_element", "cost_type", "budget_amount", "actual_amount",
            "committed_amount", "total_exposure", "variance_amount",
            "avg_variance_pct", "variance_class", "cpi",
            "budget_consumed_pct",
        ],
    ),
    "cube_sales_analysis": TableMeta(
        schema="procurement_semantic",
        table="cube_sales_analysis",
        description="Sales analysis by product, customer, and time",
        columns=[
            "product_name", "customer_name", "customer_type",
            "order_type", "year", "quarter", "year_month",
            "sales_count", "total_quantity", "total_revenue",
            "total_cogs", "total_margin", "margin_pct",
            "avg_unit_price", "prev_year_revenue",
            "revenue_yoy_growth_pct",
        ],
    ),
    "cube_inventory_status": TableMeta(
        schema="procurement_semantic",
        table="cube_inventory_status",
        description="Inventory movement analysis",
        columns=[
            "material_id", "material_name", "material_category", "year",
            "quarter", "year_month", "receipt_count", "issue_count",
            "return_count", "total_movement_count", "qty_received",
            "qty_issued", "qty_returned", "net_quantity_flow",
            "receipt_value", "issue_value", "return_value",
            "total_movement_value", "avg_unit_cost",
            "issue_to_receipt_ratio",
        ],
    ),
    "cube_spend_by_sector": TableMeta(
        schema="procurement_semantic",
        table="cube_spend_by_sector",
        description="Spend analysis by sector",
        columns=[
            "sector_key", "sector_name", "vendor_id", "vendor_name",
            "country", "year", "quarter", "po_count", "total_spend",
            "total_quantity", "avg_po_value",
            "vendor_sector_share_pct", "sector_spend_rank",
            "prev_year_spend", "yoy_growth_pct",
        ],
    ),
    "cube_delivery_performance": TableMeta(
        schema="procurement_semantic",
        table="cube_delivery_performance",
        description="Delivery performance tracking",
        columns=[
            "vendor_id", "vendor_name", "vendor_sector", "year",
            "quarter", "year_month", "delivery_count",
            "avg_variance_days", "max_variance_days",
            "min_variance_days", "on_time_rate_pct", "on_time_count",
            "late_1wk_count", "late_1mo_count", "late_over_mo_count",
            "total_value_delivered", "prev_quarter_on_time_pct",
        ],
    ),
    "cube_cost_variance": TableMeta(
        schema="procurement_semantic",
        table="cube_cost_variance",
        description="Cost variance analysis by project and WBS",
        columns=[
            "project_id", "project_name", "project_status", "sector",
            "region", "cost_type", "wbs_element", "budget_amount",
            "actual_amount", "committed_amount", "variance_amount",
            "overrun_amount", "overrun_severity", "cpi",
            "remaining_budget", "burn_rate_pct", "overrun_rank",
        ],
    ),
    "cube_vendor_risk": TableMeta(
        schema="procurement_semantic",
        table="cube_vendor_risk",
        description="Vendor risk composite scoring",
        columns=[
            "vendor_id", "vendor_name", "vendor_sector", "vendor_country",
            "vendor_city", "tier", "vendor_status", "avg_overall_score",
            "avg_delivery_score", "avg_quality_score",
            "avg_commercial_score", "avg_hse_score", "min_overall_score",
            "evaluation_count", "invoice_count", "overdue_invoices",
            "avg_days_outstanding", "total_invoiced", "overdue_rate_pct",
            "avg_acceptance_rate", "total_rejected",
            "composite_risk_score", "risk_tier", "risk_rank",
        ],
    ),
    "cube_project_timeline": TableMeta(
        schema="procurement_semantic",
        table="cube_project_timeline",
        description="Project timeline and schedule analysis",
        columns=[
            "project_id", "project_name", "project_status", "sector",
            "region", "priority", "start_date", "end_date",
            "planned_duration_days", "first_spend_date",
            "last_spend_date", "actual_active_days",
            "schedule_variance_days", "schedule_status",
            "planned_pct_complete", "cost_pct_complete", "total_actual",
            "total_budget", "active_wbs_count", "contract_count",
            "active_contracts", "completed_contracts",
            "total_contract_value",
        ],
    ),
    "cube_procurement_efficiency": TableMeta(
        schema="procurement_semantic",
        table="cube_procurement_efficiency",
        description="Procure-to-pay cycle-time analysis",
        columns=[
            "vendor_id", "vendor_name", "vendor_sector", "year",
            "quarter", "year_month", "po_count",
            "avg_order_to_receipt_days", "median_order_to_receipt",
            "avg_receipt_to_invoice_days", "median_receipt_to_invoice",
            "avg_invoice_to_payment_days", "median_invoice_to_payment",
            "avg_total_p2p_days", "median_total_p2p", "min_p2p_days",
            "max_p2p_days", "total_spend",
            "prev_quarter_avg_p2p_days", "complete_p2p_pct",
        ],
    ),
}


# ---------------------------------------------------------------------------
# Combined registry for iteration
# ---------------------------------------------------------------------------
ALL_TABLES: Dict[str, TableMeta] = {
    **GOLD_DIMENSIONS,
    **GOLD_FACTS,
    **SEMANTIC_CUBES,
}


def get_power_query_source(table: TableMeta) -> str:
    """Return a Power Query M snippet to load this table via Databricks.Catalogs."""
    return (
        f'let\n'
        f'    Source = Databricks.Catalogs("{CONNECTION.host}", "{CONNECTION.http_path}", '
        f'[Catalog="{CONNECTION.catalog}", Database="{table.schema}"]),\n'
        f'    Nav1 = Source{{[Name="{CONNECTION.catalog}"]}}[Data],\n'
        f'    Nav2 = Nav1{{[Name="{table.schema}"]}}[Data],\n'
        f'    Table = Nav2{{[Name="{table.table}"]}}[Data]\n'
        f'in\n'
        f'    Table'
    )