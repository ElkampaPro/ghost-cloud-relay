@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion
cd /d "%~dp0"
echo [Ghost Service] جاري إيقاف خادم الشبح...
if exist "%~dp0server.pid" (
    set /p PID=<"%~dp0server.pid"
    if defined PID (
        wmic process where "processid=!PID! and name='node.exe' and commandline like '%%ghost-cloud-server%%'" get processid 2>nul | findstr /r "[0-9]" >nul
        if !errorlevel! equ 0 (
            taskkill /f /pid !PID! >nul 2>&1
            del "%~dp0server.pid" >nul 2>&1
            echo [Ghost Service] تم إيقاف خادم الشبح بدقة (PID: !PID!) بنجاح دون المساس بأي خدمات أخرى.
            goto :done
        ) else (
            del "%~dp0server.pid" >nul 2>&1
            echo [Ghost Service] تم تنظيف ملف PID (PID: !PID! لا يطابق خادم الشبح أو غير نشط).
        )
    )
)
echo [Ghost Service] جاري فحص عملية node المرتبطة بـ server.js الخاص بهذا المشروع...
for /f "tokens=2" %%p in ('wmic process where "name='node.exe' and commandline like '%%ghost-cloud-server%%server.js%%'" get processid 2^>nul ^| findstr /r "[0-9]"') do (
    taskkill /f /pid %%p >nul 2>&1
    echo [Ghost Service] تم إيقاف عملية server.js (PID: %%p).
)
:done
echo [Ghost Service] اكتمل فحص وإيقاف خدمة الشبح.
pause
