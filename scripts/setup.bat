@echo off
REM ===================================================
REM Setup Script for DataBricks-Sample1 Project
REM ===================================================
REM This script sets up the Python virtual environment
REM and installs all required dependencies.

echo ===================================================
echo DataBricks-Sample1 Project Setup
echo ===================================================

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.10+ and try again
    exit /b 1
)

echo.
echo Step 1: Creating virtual environment...
if not exist "venv" (
    python -m venv venv
    echo Virtual environment created.
) else (
    echo Virtual environment already exists.
)

echo.
echo Step 2: Activating virtual environment...
call venv\Scripts\activate.bat

echo.
echo Step 3: Upgrading pip...
python -m pip install --upgrade pip

echo.
echo Step 4: Installing dependencies...
pip install -r requirements.txt

echo.
echo Step 5: Installing project in development mode...
pip install -e .

echo.
echo ===================================================
echo Setup Complete!
echo ===================================================
echo.
echo To activate the environment, run:
echo   venv\Scripts\activate
echo.
echo To run the ETL pipeline:
echo   python scripts\run_pipeline.py
echo.
echo To run tests:
echo   pytest tests\ -v
echo.
echo To run SQL queries:
echo   python scripts\run_sql.py --query-file customer_analysis.sql
echo.
