@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d "%~dp0"

rem ============================================================
rem  SAME VALUES ON ALL VPS
rem ============================================================
set "DATABASE_URL=postgresql://postgres:%%23beyservers%%21@192.168.0.47:5432/app_db"
set "REDIS_URL=redis://:%%23beyservers%%21@192.168.0.47:6379/0"
set "BOT_AUTO_START=0"
set "CHROMEDRIVER_PATH="
set "LOCK_ALERT_BOT_TOKEN=8588832368:AAFPIEUcJDnn68PZY0seuQwvqAopL_4T1_0"
set "LOCK_ALERT_CHAT_IDS="
set "FLASK_SECRET_KEY=KtbSBdA5J8Pbaf80Dk4x5ib6ZPR4OlIZhzgwwM9nwtSHYFmerQjUSscf_TZb-DHi6IBce_y0na20Rc2tXLfUMQ"
set "CLOUDFLARE_TUNNEL_TOKEN=eyJhIjoiMTQ0NWVjMzBkY2M2MGI2NmRkNWQ4ZTAzMGMzNzkxZTIiLCJ0IjoiNTA3ZTQyNWEtZjQzOS00Zjc5LWExYTgtZjFhZmE0ZmIwYjZkIiwicyI6Ik5qQmpObU0yWXpZdE5ETmxZeTAwTjJReUxXSXdOalV0WVdFd01qRTBZbUV5WVRCaCJ9"

rem ============================================================
rem  PRECHECKS
rem ============================================================
where python >nul 2>nul || (
  echo [ERROR] python not found in PATH.
  echo Install Python and reopen terminal.
  pause
  exit /b 1
)

where cloudflared >nul 2>nul || (
  echo [ERROR] cloudflared not found in PATH.
  echo Install with: winget install -e --id Cloudflare.cloudflared
  pause
  exit /b 1
)

if not defined DATABASE_URL (
  echo [ERROR] DATABASE_URL is empty.
  pause
  exit /b 1
)

if not defined REDIS_URL (
  echo [ERROR] REDIS_URL is empty.
  pause
  exit /b 1
)

if not defined FLASK_SECRET_KEY (
  echo [ERROR] FLASK_SECRET_KEY is empty.
  pause
  exit /b 1
)

if not defined CLOUDFLARE_TUNNEL_TOKEN (
  echo [ERROR] CLOUDFLARE_TUNNEL_TOKEN is empty.
  pause
  exit /b 1
)

if defined CHROMEDRIVER_PATH (
  if not exist "%CHROMEDRIVER_PATH%" (
    echo [ERROR] CHROMEDRIVER_PATH does not exist: %CHROMEDRIVER_PATH%
    pause
    exit /b 1
  )
)

python -c "import redis" >nul 2>nul || (
  echo [ERROR] Python package 'redis' is not installed.
  echo Install with: pip install redis
  pause
  exit /b 1
)

echo.
echo [INFO] Checking Redis connectivity...
python -c "import os,redis; u=os.environ.get('REDIS_URL','').strip(); r=redis.Redis.from_url(u, decode_responses=True, socket_connect_timeout=5, socket_timeout=5); r.ping(); print('REDIS_PING_OK')" || (
  echo [ERROR] Redis ping failed using REDIS_URL.
  echo Verify Redis host, password, and firewall for port 6379.
  pause
  exit /b 1
)

echo.
echo [INFO] Syncing Redis settings in database...
python -c "import os; from config import database; database.init_db(); lock_token=os.environ.get('LOCK_ALERT_BOT_TOKEN','').strip(); lock_chat_ids=[x.strip() for x in os.environ.get('LOCK_ALERT_CHAT_IDS','').split(',') if x.strip()]; payload={'REDIS_COORDINATION_ENABLED': True, 'REDIS_URL': os.environ.get('REDIS_URL','').strip(), 'REDIS_FAIL_CLOSED': True, 'LOCK_ALERT_BOT_TOKEN': lock_token}; payload.update({'LOCK_ALERT_CHAT_IDS': lock_chat_ids} if lock_chat_ids else {}); database.save_settings(payload); print('REDIS_SETTINGS_SYNCED')" || (
  echo [ERROR] Failed to sync Redis settings into database.
  pause
  exit /b 1
)

echo.
echo [INFO] Resolving ChromeDriver path...
set "AUTO_CHROMEDRIVER_PATH="
for /f "usebackq delims=" %%I in (`python -c "from core.browser import _detect_chrome_version,_resolve_chromedriver_path,_auto_install_chromedriver; v=_detect_chrome_version(); p=_resolve_chromedriver_path() or _auto_install_chromedriver(v); print(p or '')"`) do set "AUTO_CHROMEDRIVER_PATH=%%I"

if not defined CHROMEDRIVER_PATH (
  if defined AUTO_CHROMEDRIVER_PATH (
    set "CHROMEDRIVER_PATH=!AUTO_CHROMEDRIVER_PATH!"
    echo [INFO] Auto-resolved CHROMEDRIVER_PATH: !CHROMEDRIVER_PATH!
  ) else (
    echo [WARN] ChromeDriver path not resolved in preflight.
    echo [WARN] Bot will still try runtime bootstrap during browser startup.
  )
)

echo.
echo [INFO] Starting Flask server...
start "Model DM Bot Server" cmd /k "cd /d %~dp0 && set DATABASE_URL=%DATABASE_URL% && set REDIS_URL=%REDIS_URL% && set BOT_AUTO_START=%BOT_AUTO_START% && set CHROMEDRIVER_PATH=%CHROMEDRIVER_PATH% && set FLASK_SECRET_KEY=%FLASK_SECRET_KEY% && python server.py"

timeout /t 3 /nobreak >nul

echo.
echo [INFO] Starting Cloudflare tunnel connector...
echo [INFO] Press Ctrl+C to stop tunnel on this VPS.
echo.

:run_tunnel
cloudflared tunnel run --token "%CLOUDFLARE_TUNNEL_TOKEN%"
set "EXITCODE=%ERRORLEVEL%"
echo [WARN] cloudflared exited with code !EXITCODE!. Restarting in 5 seconds...
timeout /t 5 /nobreak >nul
goto run_tunnel