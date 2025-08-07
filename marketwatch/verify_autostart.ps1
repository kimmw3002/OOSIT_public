# MarketWatch Auto-Start Verification Script
Write-Host "=== MarketWatch Auto-Start Verification ===" -ForegroundColor Cyan
Write-Host ""

# 1. Check if Python process is running
Write-Host "1. Checking for Python processes..." -ForegroundColor Yellow
$pythonProcesses = Get-Process python -ErrorAction SilentlyContinue
if ($pythonProcesses) {
    Write-Host "   [Y] Python is running:" -ForegroundColor Green
    $pythonProcesses | Format-Table Id, ProcessName, StartTime, CPU
} else {
    Write-Host "   [N] No Python processes found" -ForegroundColor Red
}
Write-Host ""

# 2. Check scheduled task
Write-Host "2. Checking scheduled task..." -ForegroundColor Yellow
$taskName = "MarketWatch NYSE Scheduler"
$task = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($task) {
    $taskInfo = Get-ScheduledTaskInfo -TaskName $taskName
    Write-Host "   [Y] $taskName" -ForegroundColor Green
    Write-Host "     State: $($task.State)"
    Write-Host "     Last Run: $($taskInfo.LastRunTime)"
    Write-Host "     Result: $($taskInfo.LastTaskResult) (0 = Success)"
    Write-Host "     Next Run: $($taskInfo.NextRunTime)"
    
    # Check battery settings
    $settings = $task.Settings
    if ($settings.DisallowStartIfOnBatteries -eq $false) {
        Write-Host "     Battery Mode: [Y] Enabled (runs on battery)" -ForegroundColor Green
    } else {
        Write-Host "     Battery Mode: [N] Disabled (won't run on battery)" -ForegroundColor Red
    }
    if ($settings.StopIfGoingOnBatteries) {
        Write-Host "     Stop on Battery: [N] Yes (will stop when switching to battery)" -ForegroundColor Red
    } else {
        Write-Host "     Stop on Battery: [Y] No (continues on battery)" -ForegroundColor Green
    }
} else {
    Write-Host "   [N] $taskName not found" -ForegroundColor Red
}
Write-Host ""

# 3. Check startup log
Write-Host "3. Checking startup log..." -ForegroundColor Yellow
if (Test-Path "startup_log.txt") {
    Write-Host "   [Y] Startup log found:" -ForegroundColor Green
    Get-Content "startup_log.txt" | ForEach-Object { Write-Host "     $_" }
    
    # Check if it was auto-started
    $autoStarted = Get-Content "startup_log.txt" | Select-String "AUTO-STARTED"
    if ($autoStarted) {
        Write-Host "   [Y] Process was AUTO-STARTED by scheduled task!" -ForegroundColor Green
    } else {
        Write-Host "   [!] Process was started manually (not by scheduled task)" -ForegroundColor Yellow
    }
} else {
    Write-Host "   [N] No startup log found" -ForegroundColor Red
    Write-Host "     This means the scheduler hasn't started yet" -ForegroundColor Yellow
}
Write-Host ""

# 4. Check task scheduler event logs
Write-Host "4. Checking recent task scheduler events..." -ForegroundColor Yellow
try {
    $events = Get-WinEvent -FilterHashtable @{LogName='Microsoft-Windows-TaskScheduler/Operational'; ID=201,202,203} -MaxEvents 10 -ErrorAction SilentlyContinue | 
              Where-Object {$_.Message -like "*MarketWatch*"}
    
    if ($events) {
        Write-Host "   Recent task events:" -ForegroundColor Green
        $events | ForEach-Object {
            Write-Host "     $($_.TimeCreated): $($_.Message.Split("`n")[0])"
        }
    } else {
        Write-Host "   No recent MarketWatch task events found" -ForegroundColor Yellow
    }
} catch {
    Write-Host "   Unable to access event logs (may need admin rights)" -ForegroundColor Yellow
}
Write-Host ""

# 5. Test VBS script
Write-Host "5. Checking VBS script..." -ForegroundColor Yellow
if (Test-Path "run_hidden.vbs") {
    Write-Host "   [Y] run_hidden.vbs exists" -ForegroundColor Green
    $vbsContent = Get-Content "run_hidden.vbs" -Raw
    if ($vbsContent -like "*AUTO-STARTED*") {
        Write-Host "   [Y] VBS script configured to mark auto-start" -ForegroundColor Green
    }
} else {
    Write-Host "   [N] run_hidden.vbs not found" -ForegroundColor Red
}
Write-Host ""

# 6. Provide next steps
Write-Host "=== Next Steps ===" -ForegroundColor Cyan
Write-Host "1. If Python is NOT running after restart:"
Write-Host "   - Run this command to manually trigger the task:"
Write-Host "     Start-ScheduledTask -TaskName 'MarketWatch NYSE Scheduler'" -ForegroundColor White
Write-Host ""
Write-Host "2. To test the VBS script directly:"
Write-Host "     wscript run_hidden.vbs" -ForegroundColor White
Write-Host ""
Write-Host "3. To see detailed task info:"
Write-Host "     Get-ScheduledTask -TaskName 'MarketWatch NYSE Scheduler' | Get-ScheduledTaskInfo" -ForegroundColor White
Write-Host ""
Write-Host "4. To export task details for debugging:"
Write-Host "     schtasks /query /tn 'MarketWatch NYSE Scheduler' /v /fo list" -ForegroundColor White