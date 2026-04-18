@echo off
setlocal EnableExtensions

cd /d "%~dp0"

where python >nul 2>nul || (
  echo [ERROR] python not found in PATH.
  echo Install Python and reopen terminal.
  pause
  exit /b 1
)

if not exist ".env" (
  echo [ERROR] Missing .env file in project root.
  echo [INFO] Create .env from template with:
  echo        copy .env.example .env
  pause
  exit /b 1
)

python run_server_tunnel.py %*
set "EXITCODE=%ERRORLEVEL%"

if not "%EXITCODE%"=="0" (
  echo.
  echo [ERROR] Launcher exited with code %EXITCODE%.
  pause
)

exit /b %EXITCODE%