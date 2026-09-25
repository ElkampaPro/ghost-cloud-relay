@echo off
setlocal
cd /d "%~dp0"
set NODE_EXE=node
where node >nul 2>nul
if %errorlevel% neq 0 (
    if exist "C:\Program Files\nodejs\node.exe" (
        set "NODE_EXE=C:\Program Files\nodejs\node.exe"
    ) else (
        echo [Ghost Cloud] Error: Node.js is not found on PATH or in Program Files.
        pause
        exit /b 1
    )
)
"%NODE_EXE%" server.js >> "%~dp0service.log" 2>&1
