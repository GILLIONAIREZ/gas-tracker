# Registers an hourly Windows Task Scheduler job for the gas price tracker.
# Run once as admin: powershell -ExecutionPolicy Bypass -File C:\GasTracker\setup_task.ps1

$pythonw  = "C:\Users\Gillionaire\AppData\Local\Programs\Python\Python313\pythonw.exe"
$script   = "C:\GasTracker\gas_tracker.py"
$taskName = "GasTrackerHourly"

$action   = New-ScheduledTaskAction -Execute $pythonw -Argument $script

# Repeat every hour indefinitely, start 1 minute from now
$trigger  = New-ScheduledTaskTrigger -Once -At ((Get-Date).AddMinutes(1)) `
                -RepetitionInterval (New-TimeSpan -Hours 1)

$settings = New-ScheduledTaskSettingsSet `
                -ExecutionTimeLimit  (New-TimeSpan -Minutes 5) `
                -RunOnlyIfNetworkAvailable `
                -StartWhenAvailable `
                -MultipleInstances IgnoreNew

Register-ScheduledTask `
    -TaskName   $taskName `
    -Action     $action `
    -Trigger    $trigger `
    -Settings   $settings `
    -Description "Checks AAA gas price hourly to find daily update time. Emails on new day price." `
    -Force

Write-Host "Task '$taskName' registered successfully."
Write-Host "Next run: $((Get-ScheduledTask -TaskName $taskName).Triggers[0].StartBoundary)"
