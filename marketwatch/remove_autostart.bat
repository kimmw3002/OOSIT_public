@echo off
:: Remove MarketWatch NYSE Scheduler auto-start tasks

echo Removing MarketWatch NYSE Scheduler auto-start task...
echo.

schtasks /delete /tn "MarketWatch NYSE Scheduler" /f

:: Remove startup log if it exists
if exist "%~dp0startup_log.txt" (
    del "%~dp0startup_log.txt"
    echo Removed startup_log.txt
)

echo.
echo Auto-start task removed successfully!
echo The scheduler will no longer start automatically.
echo.
pause