@echo off
REM ═══════════════════════════════════════════════════════════════
REM Power BI - Databricks ODBC Driver Setup
REM Configures ODBC DSN for Databricks SQL Warehouse connection
REM ═══════════════════════════════════════════════════════════════

echo === Databricks ODBC Driver Setup for Power BI ===
echo.

REM Check for admin privileges
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: This script must be run as Administrator.
    echo Right-click and select "Run as administrator".
    pause
    exit /b 1
)

REM Configuration
set DSN_NAME=Databricks_Procurement
set HOST=dbc-760a206e-c226.cloud.databricks.com
set PORT=443
set HTTP_PATH=/sql/1.0/warehouses/7655fa24e271f9d1
set CATALOG=workspace
set SCHEMA=procurement_semantic
set AUTH_MECH=3
set DRIVER_NAME=Simba Spark ODBC Driver

REM Check if Databricks ODBC driver is installed
reg query "HKLM\SOFTWARE\ODBC\ODBCINST.INI\Simba Spark ODBC Driver" >nul 2>&1
if %errorlevel% neq 0 (
    echo WARNING: Simba Spark ODBC Driver not found.
    echo.
    echo Please download and install from:
    echo   https://www.databricks.com/spark/odbc-drivers-download
    echo.
    echo After installing the driver, re-run this script.
    pause
    exit /b 1
)

echo Driver found: %DRIVER_NAME%
echo.

REM Create System DSN for semantic layer
echo Creating System DSN: %DSN_NAME%...
reg add "HKLM\SOFTWARE\ODBC\ODBC.INI\%DSN_NAME%" /v Driver /t REG_SZ /d "C:\Program Files\Simba Spark ODBC Driver\lib\SparkODBC_sb64.dll" /f >nul
reg add "HKLM\SOFTWARE\ODBC\ODBC.INI\%DSN_NAME%" /v Description /t REG_SZ /d "Databricks Procurement Semantic Layer" /f >nul
reg add "HKLM\SOFTWARE\ODBC\ODBC.INI\%DSN_NAME%" /v Host /t REG_SZ /d "%HOST%" /f >nul
reg add "HKLM\SOFTWARE\ODBC\ODBC.INI\%DSN_NAME%" /v Port /t REG_SZ /d "%PORT%" /f >nul
reg add "HKLM\SOFTWARE\ODBC\ODBC.INI\%DSN_NAME%" /v HTTPPath /t REG_SZ /d "%HTTP_PATH%" /f >nul
reg add "HKLM\SOFTWARE\ODBC\ODBC.INI\%DSN_NAME%" /v Schema /t REG_SZ /d "%SCHEMA%" /f >nul
reg add "HKLM\SOFTWARE\ODBC\ODBC.INI\%DSN_NAME%" /v Catalog /t REG_SZ /d "%CATALOG%" /f >nul
reg add "HKLM\SOFTWARE\ODBC\ODBC.INI\%DSN_NAME%" /v AuthMech /t REG_SZ /d "%AUTH_MECH%" /f >nul
reg add "HKLM\SOFTWARE\ODBC\ODBC.INI\%DSN_NAME%" /v ThriftTransport /t REG_SZ /d "2" /f >nul
reg add "HKLM\SOFTWARE\ODBC\ODBC.INI\%DSN_NAME%" /v SSL /t REG_SZ /d "1" /f >nul
reg add "HKLM\SOFTWARE\ODBC\ODBC.INI\%DSN_NAME%" /v SparkServerType /t REG_SZ /d "3" /f >nul

REM Register the DSN in the data sources list
reg add "HKLM\SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources" /v "%DSN_NAME%" /t REG_SZ /d "%DRIVER_NAME%" /f >nul

echo.
echo DSN '%DSN_NAME%' created successfully.
echo.
echo ┌──────────────────────────────────────────────────────────┐
echo │  Connection Details:                                     │
echo │  Host:      %HOST%                                      │
echo │  Catalog:   %CATALOG%                                   │
echo │  Schema:    %SCHEMA%                                    │
echo │  Auth:      Personal Access Token (set in Power BI)     │
echo └──────────────────────────────────────────────────────────┘
echo.
echo NEXT STEPS:
echo   1. Open Power BI Desktop
echo   2. Get Data ^> ODBC ^> Select '%DSN_NAME%'
echo   3. Enter your Databricks Personal Access Token as password
echo   4. Select tables from procurement_semantic schema
echo.
pause
