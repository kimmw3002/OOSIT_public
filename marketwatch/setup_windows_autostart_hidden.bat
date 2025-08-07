@echo off
:: Setup Windows Task Scheduler for MarketWatch NYSE Scheduler (Hidden Mode)
:: Run this script as Administrator to create a scheduled task that runs in background

echo Setting up MarketWatch NYSE Scheduler for Windows auto-start (Hidden Mode)...
echo This will create a scheduled task that runs in the background without any visible window.
echo.
echo Press any key to continue or Ctrl+C to cancel...
pause > nul

:: Get the current directory
set "SCRIPT_DIR=%~dp0"
set "BATCH_FILE=%SCRIPT_DIR%marketwatch_scheduler_windows.bat"
set "VBS_FILE=%SCRIPT_DIR%run_hidden.vbs"

:: Check if VBS file exists
if not exist "%VBS_FILE%" (
    echo ERROR: run_hidden.vbs not found in %SCRIPT_DIR%
    echo Please ensure run_hidden.vbs exists before running this setup.
    pause
    exit /b 1
)

:: Delete existing task if it exists
schtasks /delete /tn "MarketWatch NYSE Scheduler" /f >nul 2>&1

:: Create a single scheduled task for system startup with 1 minute delay (hidden)
:: Note: We create a basic task first, then use PowerShell to set battery settings
schtasks /create /tn "MarketWatch NYSE Scheduler" /tr "wscript.exe \"%VBS_FILE%\"" /sc onstart /delay 0001:00 /ru "%USERNAME%" /rl highest /f

:: Configure the task to run on battery power
echo Configuring task to run on battery power...
powershell -Command "Set-ScheduledTask -TaskName 'MarketWatch NYSE Scheduler' -Settings (New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries)" >nul 2>&1

echo.
echo Scheduled tasks created successfully!
echo.
echo The scheduler will now:
echo 1. Start automatically 1 minute after Windows starts (hidden in background)
echo 2. Restart automatically if it crashes
echo 3. Run completely invisible with no command window
echo.
echo To start the scheduler now:
echo   wscript "%VBS_FILE%"
echo.
echo Testing the setup...
schtasks /run /tn "MarketWatch NYSE Scheduler" >nul 2>&1
timeout /t 3 /nobreak >nul
echo.
echo To check if it's running:
echo   tasklist ^| findstr python
echo.
echo To manage the scheduled tasks:
echo   - Open Task Scheduler (taskschd.msc)
echo   - Look for "MarketWatch NYSE Scheduler" tasks
echo.
echo To remove auto-start:
echo   schtasks /delete /tn "MarketWatch NYSE Scheduler" /f
echo.
pause