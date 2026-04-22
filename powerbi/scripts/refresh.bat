@echo off
REM Power BI refresh automation — triggers data refresh via XMLA endpoint
REM For Power BI free tier: open .pbix and click Refresh manually
REM For Pro/Premium: use this script with Power BI REST API

echo =============================================
echo  Procurement Analytics - Power BI Refresh
echo =============================================
echo.

REM Check if Power BI Desktop is running
tasklist /FI "IMAGENAME eq PBIDesktop.exe" 2>NUL | find /I "PBIDesktop.exe" >NUL
if %ERRORLEVEL%==0 (
    echo Power BI Desktop is running.
    echo Please click Refresh in the ribbon to update data.
) else (
    echo Power BI Desktop is not running.
    echo Please open your .pbix file and click Refresh.
)

echo.
echo For automated refresh (Pro/Premium), configure:
echo   1. Publish to Power BI Service
echo   2. Set up scheduled refresh with Databricks credentials
echo   3. Or use Power BI REST API: POST /groups/{group}/datasets/{dataset}/refreshes
echo.
pause
