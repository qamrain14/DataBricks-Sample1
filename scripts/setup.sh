#!/bin/bash
# ===================================================
# Setup Script for DataBricks-Sample1 Project
# ===================================================
# This script sets up the Python virtual environment
# and installs all required dependencies.

set -e

echo "==================================================="
echo "DataBricks-Sample1 Project Setup"
echo "==================================================="

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python3 is not installed"
    echo "Please install Python 3.10+ and try again"
    exit 1
fi

echo ""
echo "Step 1: Creating virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "Virtual environment created."
else
    echo "Virtual environment already exists."
fi

echo ""
echo "Step 2: Activating virtual environment..."
source venv/bin/activate

echo ""
echo "Step 3: Upgrading pip..."
pip install --upgrade pip

echo ""
echo "Step 4: Installing dependencies..."
pip install -r requirements.txt

echo ""
echo "Step 5: Installing project in development mode..."
pip install -e .

echo ""
echo "==================================================="
echo "Setup Complete!"
echo "==================================================="
echo ""
echo "To activate the environment, run:"
echo "  source venv/bin/activate"
echo ""
echo "To run the ETL pipeline:"
echo "  python scripts/run_pipeline.py"
echo ""
echo "To run tests:"
echo "  pytest tests/ -v"
echo ""
echo "To run SQL queries:"
echo "  python scripts/run_sql.py --query-file customer_analysis.sql"
echo ""
