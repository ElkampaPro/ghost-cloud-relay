@echo off
chcp 65001 >nul
echo [Ghost Service] جاري تثبيت خادم الشبح ليعمل تلقائياً مع تشغيل الويندوز في صمت...

set "STARTUP_FOLDER=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup"
set "TARGET_VBS=%~dp0start_silent.vbs"

echo Set oWS = WScript.CreateObject("WScript.Shell") > "%TEMP%\CreateShortcut.vbs"
echo sLinkFile = "%STARTUP_FOLDER%\GhostCloudRelay.lnk" >> "%TEMP%\CreateShortcut.vbs"
echo Set oLink = oWS.CreateShortcut(sLinkFile) >> "%TEMP%\CreateShortcut.vbs"
echo oLink.TargetPath = "wscript.exe" >> "%TEMP%\CreateShortcut.vbs"
echo oLink.Arguments = """%TARGET_VBS%""" >> "%TEMP%\CreateShortcut.vbs"
echo oLink.WorkingDirectory = "%~dp0" >> "%TEMP%\CreateShortcut.vbs"
echo oLink.WindowStyle = 0 >> "%TEMP%\CreateShortcut.vbs"
echo oLink.Save >> "%TEMP%\CreateShortcut.vbs"

cscript //nologo "%TEMP%\CreateShortcut.vbs"
del "%TEMP%\CreateShortcut.vbs"

echo =======================================================
echo ✅ تم تثبيت الخدمة بنجاح في قائمة بدء تشغيل الويندوز!
echo سيعمل الخادم تلقائياً في الخلفية في صمت تام عند كل تشغيل للكمبيوتر.
echo =======================================================
pause
