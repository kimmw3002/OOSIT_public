@echo off
:: MarketWatch NYSE Scheduler - Windows Auto-Start Script
:: This script runs the scheduler in a loop, automatically restarting if it crashes

cd /d "%~dp0"

:: Create startup marker file
echo [%date% %time%] Started by: %1 >> startup_log.txt
if "%1"=="" echo [%date% %time%] Started manually (no parameter) >> startup_log.txt

echo Starting MarketWatch NYSE Scheduler...
echo This script will automatically restart the scheduler if it crashes.
echo Press Ctrl+C twice to stop.

:restart_loop
echo.
echo [%date% %time%] Starting scheduler...
python marketwatch_nyse_scheduler.py

:: Check if Python exited with error
if %errorlevel% neq 0 (
    echo [%date% %time%] Scheduler crashed with error code %errorlevel%
    echo Restarting in 10 seconds...
    timeout /t 10 /nobreak > nul
    goto restart_loop
)

:: Normal exit (Ctrl+C)
echo [%date% %time%] Scheduler stopped normally.
pause