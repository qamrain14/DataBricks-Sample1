# DataBricks-Sample1

A sample Databricks project demonstrating ETL pipelines with Python, PySpark, SQL, and Delta Lake. This project is designed to run **locally** without requiring an Azure Databricks workspace.

## 🚀 Features

- **Local PySpark Execution**: Run everything locally without Databricks
- **Delta Lake Support**: ACID transactions and time travel locally
- **ETL Pipeline**: Complete Extract-Transform-Load pipeline
- **SQL Queries**: Ready-to-use SQL analytics queries
- **Jupyter Notebooks**: Interactive PySpark notebooks
- **Databricks Asset Bundles**: Ready for deployment when you have a workspace
- **Comprehensive Tests**: pytest test suite included

## 📁 Project Structure

```
DataBricks-Sample1/
├── databricks.yml          # Databricks Asset Bundle config
├── pyproject.toml          # Python project configuration
├── requirements.txt        # Python dependencies
├── .env                    # Environment variables
├── .gitignore
├── README.md
├── src/                    # Source code
│   ├── __init__.py
│   ├── main.py             # Main ETL entry point
│   ├── spark_session.py    # Spark session management
│   ├── etl/
│   │   ├── __init__.py
│   │   ├── extract.py      # Data extraction
│   │   ├── transform.py    # Data transformations
│   │   └── load.py         # Data loading
│   └── utils/
│       ├── __init__.py
│       └── logger.py       # Logging utilities
├── sql/                    # SQL query files
│   ├── customer_analysis.sql
│   ├── product_analysis.sql
│   ├── time_analysis.sql
│   └── geographic_analysis.sql
├── notebooks/              # Jupyter notebooks
│   ├── 01_etl_pipeline_demo.ipynb
│   ├── 02_pyspark_sql_deep_dive.ipynb
│   └── 03_delta_lake_operations.ipynb
├── tests/                  # Test suite
│   ├── conftest.py
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── scripts/                # Utility scripts
│   ├── run_pipeline.py
│   ├── run_sql.py
│   ├── setup.bat           # Windows setup
│   └── setup.sh            # Linux/Mac setup
├── data/                   # Data directory
│   ├── input/
│   └── output/
└── resources/              # Databricks job definitions
```

## 🛠️ Quick Start

### Prerequisites

- Python 3.10+
- Java 8 or 11 (required for Spark)

### Setup

**Windows:**
```batch
cd DataBricks-Sample1
scripts\setup.bat
```

**Linux/Mac:**
```bash
cd DataBricks-Sample1
chmod +x scripts/setup.sh
./scripts/setup.sh
```

**Manual Setup:**
```bash
# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate
# Activate (Linux/Mac)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install project
pip install -e .
```

### Run the ETL Pipeline

```bash
python scripts/run_pipeline.py
```

### Run Tests

```bash
pytest tests/ -v
```

### Run SQL Queries

```bash
# List available SQL files
python scripts/run_sql.py --list

# Run a specific query file
python scripts/run_sql.py --query-file customer_analysis.sql
```

### Run Notebooks

```bash
jupyter notebook notebooks/
```

## 📊 Sample Data

The project generates sample sales data with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| order_id | string | Unique order identifier |
| customer_id | string | Customer identifier |
| first_name | string | Customer first name |
| last_name | string | Customer last name |
| email | string | Customer email |
| city | string | Customer city |
| country | string | Customer country |
| product_name | string | Product name |
| category | string | Product category |
| quantity | integer | Order quantity |
| unit_price | double | Price per unit |
| order_date | date | Order date |

## 🔧 Configuration

### Environment Variables (.env)

```env
SPARK_HOME=/path/to/spark
LOG_LEVEL=INFO
OUTPUT_PATH=./data/output
```

### Databricks Configuration (databricks.yml)

When you have access to a Databricks workspace, update:

```yaml
workspace:
  host: https://your-workspace.cloud.databricks.com

targets:
  dev:
    default: true
    workspace:
      host: https://your-dev-workspace.cloud.databricks.com
```

## 📓 Notebooks Overview

1. **01_etl_pipeline_demo.ipynb**: Complete ETL pipeline walkthrough
2. **02_pyspark_sql_deep_dive.ipynb**: Advanced PySpark SQL operations
3. **03_delta_lake_operations.ipynb**: Delta Lake CRUD, time travel, merges

## 🧪 Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=src --cov-report=html

# Run specific test file
pytest tests/test_transform.py -v
```

## 📦 Deployment to Databricks

When you have a Databricks workspace:

1. Install Databricks CLI:
   ```bash
   pip install databricks-cli
   ```

2. Configure authentication:
   ```bash
   databricks configure --token
   ```

3. Deploy the bundle:
   ```bash
   databricks bundle deploy -t dev
   ```

## 🤝 VS Code Integration

This project works with the following VS Code extensions:
- Databricks (official)
- Databricks Power Tools
- Python
- Jupyter

## 📝 License

MIT License

## 🙋 Support

For issues and questions, please create an issue in the repository.
