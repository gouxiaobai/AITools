@echo off
setlocal
chcp 65001 >nul
title GameProxyTool Build

echo ============================================
echo   GameProxyTool Build Script
echo ============================================
echo.

set "PY_CMD="
where python >nul 2>&1
if not errorlevel 1 set "PY_CMD=python"
if not defined PY_CMD where py >nul 2>&1 && set "PY_CMD=py -3"
if not defined PY_CMD (
    for /d %%D in ("%LocalAppData%\Programs\Python\Python*") do (
        if exist "%%~fD\python.exe" set "PY_CMD=%%~fD\python.exe"
    )
)
if not defined PY_CMD (
    echo [ERROR] Python was not found in PATH.
    echo Install Python 3.10+ and retry:
    echo https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [0/3] Checking dependencies...
call "%PY_CMD%" -c "import flask, msgpack, openpyxl, PyInstaller" >nul 2>&1
if errorlevel 1 (
    echo [INFO] Missing dependency detected. Installing...
    call "%PY_CMD%" -m pip install flask msgpack openpyxl pyinstaller --quiet
    if errorlevel 1 (
        echo [ERROR] Failed to install dependencies.
        pause
        exit /b 1
    )
)

echo [1/3] Cleaning old outputs...
if exist "dist\GameProxyTool.exe" (
    del /f /q "dist\GameProxyTool.exe" >nul 2>&1
    if exist "dist\GameProxyTool.exe" (
        echo [ERROR] dist\GameProxyTool.exe is locked by another process.
        echo Close the running GameProxyTool.exe and run build.bat again.
        pause
        exit /b 1
    )
)
if exist "dist" rmdir /s /q "dist"
if exist "build" rmdir /s /q "build"
if exist "GameProxyTool.spec" del /q "GameProxyTool.spec"
if exist "dist" (
    echo [ERROR] Failed to remove dist directory.
    pause
    exit /b 1
)
if exist "build" (
    echo [ERROR] Failed to remove build directory.
    pause
    exit /b 1
)

echo [2/3] Building executable...
call "%PY_CMD%" -m PyInstaller ^
    --onefile ^
    --name "GameProxyTool" ^
    --add-data "static;static" ^
    --hidden-import flask ^
    --hidden-import msgpack ^
    --hidden-import werkzeug ^
    --hidden-import werkzeug.serving ^
    --hidden-import werkzeug.routing ^
    --hidden-import werkzeug.exceptions ^
    --collect-all flask ^
    main.py

if errorlevel 1 (
    echo.
    echo [ERROR] Build failed. Check the messages above.
    pause
    exit /b 1
)

echo [3/3] Build completed successfully.
echo Output: dist\GameProxyTool.exe
echo.
echo Example:
echo   GameProxyTool.exe --server-host ^<GAME_SERVER_IP^>
echo.
pause
exit /b 0
