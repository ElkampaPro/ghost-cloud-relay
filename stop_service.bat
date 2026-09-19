@echo off
chcp 65001 >nul
echo [Ghost Service] جاري إيقاف خادم الشبح...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :3000 ^| findstr LISTENING') do (
    taskkill /f /pid %%a >nul 2>&1
)
echo [Ghost Service] تم إيقاف الخدمة بنجاح.
pause
