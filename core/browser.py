"""
Browser factory using undetected-chromedriver Chrome sessions.
"""
import random
import subprocess
import re
import logging
import base64
import glob
import os
import time
import ctypes
import json
import tempfile
import shutil
import atexit
import socket
import socketserver
import select
import threading
from urllib.parse import urlsplit, unquote
import undetected_chromedriver as uc

try:
    from webdriver_manager.chrome import ChromeDriverManager
except Exception:  # pragma: no cover - optional dependency at runtime
    ChromeDriverManager = None

logger = logging.getLogger("model_dm_bot")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
]

_TEMP_PROXY_EXTENSION_DIRS = []
_TEMP_BROWSER_PROFILE_DIRS = []
_LOCAL_PROXY_SERVERS = []
_CHROMEDRIVER_CACHE_LOCK = threading.Lock()
_AUTO_CHROMEDRIVER_PATH = ""
_AUTO_CHROMEDRIVER_ATTEMPTED = False
_AUTO_CHROMEDRIVER_LAST_ERROR = ""


def _safe_remove_dir(path: str, retries: int = 6, delay_seconds: float = 0.2):
    target = str(path or "").strip()
    if not target:
        return

    for _ in range(max(1, int(retries))):
        try:
            shutil.rmtree(target, ignore_errors=False)
            return
        except FileNotFoundError:
            return
        except Exception:
            time.sleep(max(0.0, float(delay_seconds)))

    # Last fallback should never raise.
    try:
        shutil.rmtree(target, ignore_errors=True)
    except Exception:
        pass


def _cleanup_proxy_resources():
    while _LOCAL_PROXY_SERVERS:
        server = _LOCAL_PROXY_SERVERS.pop()
        try:
            server.shutdown()
        except Exception:
            pass
        try:
            server.server_close()
        except Exception:
            pass

    while _TEMP_PROXY_EXTENSION_DIRS:
        folder = _TEMP_PROXY_EXTENSION_DIRS.pop()
        try:
            shutil.rmtree(folder, ignore_errors=True)
        except Exception:
            pass

    while _TEMP_BROWSER_PROFILE_DIRS:
        folder = _TEMP_BROWSER_PROFILE_DIRS.pop()
        _safe_remove_dir(folder)


atexit.register(_cleanup_proxy_resources)


class _AuthenticatedForwardProxy(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, server_address, handler_cls, upstream_host, upstream_port, auth_header):
        super().__init__(server_address, handler_cls)
        self.upstream_host = str(upstream_host)
        self.upstream_port = int(upstream_port)
        self.auth_header = str(auth_header)


class _AuthenticatedForwardProxyHandler(socketserver.BaseRequestHandler):
    _HEADER_LIMIT = 128 * 1024

    def _recv_headers(self, sock) -> bytes:
        data = b""
        while b"\r\n\r\n" not in data and len(data) < self._HEADER_LIMIT:
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
        return data

    def _pipe_bidirectional(self, client_sock, upstream_sock):
        sockets = [client_sock, upstream_sock]
        while True:
            try:
                ready, _, _ = select.select(sockets, [], [], 60)
            except OSError:
                return
            if not ready:
                return
            for source in ready:
                target = upstream_sock if source is client_sock else client_sock
                try:
                    chunk = source.recv(8192)
                except OSError:
                    return
                if not chunk:
                    return
                try:
                    target.sendall(chunk)
                except OSError:
                    return

    def handle(self):
        client_sock = self.request
        client_sock.settimeout(60)

        request_blob = self._recv_headers(client_sock)
        if not request_blob:
            return

        header_end = request_blob.find(b"\r\n\r\n")
        if header_end < 0:
            return

        header_bytes = request_blob[:header_end]
        pending_body = request_blob[header_end + 4 :]

        try:
            header_text = header_bytes.decode("iso-8859-1", errors="replace")
        except Exception:
            return

        lines = header_text.split("\r\n")
        if not lines:
            return

        parts = lines[0].split(" ", 2)
        if len(parts) != 3:
            return

        method, target, version = parts[0].upper(), parts[1], parts[2]

        try:
            upstream_sock = socket.create_connection(
                (self.server.upstream_host, self.server.upstream_port), timeout=30
            )
            upstream_sock.settimeout(60)
        except Exception:
            try:
                client_sock.sendall(b"HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n")
            except Exception:
                pass
            return

        try:
            if method == "CONNECT":
                connect_payload = (
                    f"CONNECT {target} HTTP/1.1\r\n"
                    f"Host: {target}\r\n"
                    f"Proxy-Authorization: {self.server.auth_header}\r\n"
                    "Proxy-Connection: Keep-Alive\r\n"
                    "Connection: Keep-Alive\r\n\r\n"
                ).encode("iso-8859-1")
                upstream_sock.sendall(connect_payload)

                upstream_response = self._recv_headers(upstream_sock)
                if not upstream_response:
                    client_sock.sendall(b"HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n")
                    return

                status_line = upstream_response.split(b"\r\n", 1)[0]
                if b" 200 " not in status_line and not status_line.startswith(b"HTTP/1.0 200"):
                    client_sock.sendall(upstream_response)
                    return

                client_sock.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                self._pipe_bidirectional(client_sock, upstream_sock)
                return

            forwarded_headers = []
            for line in lines[1:]:
                if ":" not in line:
                    continue
                name, value = line.split(":", 1)
                key = name.strip().lower()
                if key in ("proxy-authorization", "proxy-connection"):
                    continue
                forwarded_headers.append((name.strip(), value.lstrip()))

            forwarded_headers.append(("Proxy-Authorization", self.server.auth_header))

            payload = f"{method} {target} {version}\r\n".encode("iso-8859-1")
            for name, value in forwarded_headers:
                payload += f"{name}: {value}\r\n".encode("iso-8859-1")
            payload += b"\r\n" + pending_body
            upstream_sock.sendall(payload)

            self._pipe_bidirectional(client_sock, upstream_sock)
        finally:
            try:
                upstream_sock.close()
            except Exception:
                pass


def _parse_ints(text: str) -> set:
    return {int(x) for x in re.findall(r"\d+", str(text or ""))}


def _chrome_child_pids(chromedriver_pid: int) -> set:
    pids = set()
    if not chromedriver_pid:
        return pids

    # Prefer WMIC first (widely available on older Windows installs), fallback to PowerShell.
    try:
        result = subprocess.run(
            [
                "wmic",
                "process",
                "where",
                f"(ParentProcessId={chromedriver_pid})",
                "get",
                "ProcessId",
                "/value",
            ],
            capture_output=True,
            text=True,
            timeout=3,
        )
        if result.returncode == 0:
            pids.update(_parse_ints(result.stdout))
    except Exception:
        pass

    if pids:
        return pids

    try:
        ps_cmd = (
            f"Get-CimInstance Win32_Process -Filter \"ParentProcessId={chromedriver_pid}\" "
            "| Select-Object -ExpandProperty ProcessId"
        )
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_cmd],
            capture_output=True,
            text=True,
            timeout=4,
        )
        if result.returncode == 0:
            pids.update(_parse_ints(result.stdout))
    except Exception:
        pass

    return pids


def _bring_window_to_front_windows(driver):
    if os.name != "nt":
        return

    try:
        user32 = ctypes.windll.user32
    except Exception:
        return

    SW_RESTORE = 9
    HWND_TOPMOST = -1
    HWND_NOTOPMOST = -2
    SWP_NOSIZE = 0x0001
    SWP_NOMOVE = 0x0002
    SWP_SHOWWINDOW = 0x0040

    title_hint = ""
    try:
        title_hint = str(driver.title or "").strip().lower()
    except Exception:
        pass

    target_pids = set()
    try:
        service_proc = getattr(getattr(driver, "service", None), "process", None)
        if service_proc and getattr(service_proc, "pid", None):
            target_pids.update(_chrome_child_pids(int(service_proc.pid)))
    except Exception:
        pass

    candidates = []
    enum_callback = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_void_p, ctypes.c_void_p)

    @enum_callback
    def _enum_windows(hwnd, _):
        try:
            hwnd = int(hwnd)
            if not user32.IsWindowVisible(hwnd):
                return True

            length = user32.GetWindowTextLengthW(hwnd)
            if length <= 0:
                return True

            title_buf = ctypes.create_unicode_buffer(length + 1)
            user32.GetWindowTextW(hwnd, title_buf, length + 1)
            title = title_buf.value.strip()
            if not title:
                return True

            class_buf = ctypes.create_unicode_buffer(256)
            user32.GetClassNameW(hwnd, class_buf, 256)
            class_name = class_buf.value
            if class_name != "Chrome_WidgetWin_1":
                return True

            pid = ctypes.c_ulong(0)
            user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
            win_pid = int(pid.value)

            if target_pids and win_pid not in target_pids:
                return True

            title_lower = title.lower()
            score = 1 if "chrome" in title_lower else 0
            if "instagram" in title_lower:
                score += 3
            if title_hint and title_hint in title_lower:
                score += 5
            if "new tab" in title_lower:
                score -= 1

            candidates.append((score, hwnd))
        except Exception:
            pass
        return True

    try:
        user32.EnumWindows(_enum_windows, 0)
    except Exception:
        return

    if not candidates:
        return

    candidates.sort(key=lambda x: x[0], reverse=True)
    hwnd = int(candidates[0][1])

    try:
        user32.ShowWindow(hwnd, SW_RESTORE)
        user32.SetWindowPos(
            hwnd,
            HWND_TOPMOST,
            0,
            0,
            0,
            0,
            SWP_NOMOVE | SWP_NOSIZE | SWP_SHOWWINDOW,
        )
        user32.SetWindowPos(
            hwnd,
            HWND_NOTOPMOST,
            0,
            0,
            0,
            0,
            SWP_NOMOVE | SWP_NOSIZE | SWP_SHOWWINDOW,
        )
        user32.BringWindowToTop(hwnd)
        user32.SetForegroundWindow(hwnd)
    except Exception:
        pass


def _maximize_and_focus_browser(driver):
    try:
        window_info = driver.execute_cdp_cmd("Browser.getWindowForTarget", {})
        window_id = window_info.get("windowId") if isinstance(window_info, dict) else None
        if window_id is not None:
            driver.execute_cdp_cmd(
                "Browser.setWindowBounds",
                {"windowId": window_id, "bounds": {"windowState": "maximized"}},
            )
    except Exception:
        pass

    try:
        driver.maximize_window()
    except Exception:
        pass

    try:
        driver.execute_cdp_cmd("Page.bringToFront", {})
    except Exception:
        pass

    _bring_window_to_front_windows(driver)


def _detect_chrome_version() -> int:
    """
    Auto-detect the installed Chrome major version.
    Falls back to 145 if detection fails.
    """
    try:
        # Windows: query registry for Chrome version
        result = subprocess.run(
            ['reg', 'query', r'HKEY_CURRENT_USER\Software\Google\Chrome\BLBeacon', '/v', 'version'],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            match = re.search(r'(\d+)\.', result.stdout)
            if match:
                version = int(match.group(1))
                logger.info(f"[Browser] Detected Chrome version: {version}")
                return version
    except Exception:
        pass

    try:
        # Fallback: try running chrome --version
        result = subprocess.run(
            ['chrome', '--version'], capture_output=True, text=True, timeout=5
        )
        match = re.search(r'(\d+)\.', result.stdout)
        if match:
            return int(match.group(1))
    except Exception:
        pass

    logger.info("[Browser] Could not detect Chrome version, defaulting to 145")
    return 145


def _resolve_chromedriver_path() -> str:
    """Return a valid ChromeDriver path from env vars when provided."""
    global _AUTO_CHROMEDRIVER_PATH

    candidates = [
        os.environ.get("CHROMEDRIVER_PATH", ""),
        os.environ.get("UC_CHROMEDRIVER_PATH", ""),
        _AUTO_CHROMEDRIVER_PATH,
    ]

    for raw in candidates:
        candidate = os.path.expandvars(os.path.expanduser(str(raw or "").strip().strip('"')))
        if not candidate:
            continue

        if os.path.isfile(candidate):
            return candidate

        logger.warning(f"[Browser] Ignoring missing ChromeDriver path: {candidate}")

    discovered_from_path = shutil.which("chromedriver")
    if discovered_from_path and os.path.isfile(discovered_from_path):
        return discovered_from_path

    if os.name == "nt":
        local_app_data = str(os.environ.get("LOCALAPPDATA", "") or "").strip()
        program_files = str(os.environ.get("ProgramFiles", "") or "").strip()
        program_files_x86 = str(os.environ.get("ProgramFiles(x86)", "") or "").strip()

        direct_candidates = [
            os.path.join(program_files, "Google", "Chrome", "Application", "chromedriver.exe"),
            os.path.join(program_files_x86, "Google", "Chrome", "Application", "chromedriver.exe"),
            os.path.join(local_app_data, "Google", "Chrome", "Application", "chromedriver.exe"),
            os.path.join(local_app_data, "Google", "Chrome for Testing", "chromedriver-win64", "chromedriver.exe"),
        ]
        for candidate in direct_candidates:
            clean = str(candidate or "").strip()
            if clean and os.path.isfile(clean):
                logger.info(f"[Browser] Auto-discovered ChromeDriver path: {clean}")
                return clean

        if local_app_data:
            win_patterns = [
                os.path.join(
                    local_app_data,
                    "Microsoft",
                    "WinGet",
                    "Packages",
                    "Chromium.ChromeDriver_*",
                    "**",
                    "chromedriver.exe",
                ),
                os.path.join(
                    local_app_data,
                    "Google",
                    "Chrome for Testing",
                    "**",
                    "chromedriver.exe",
                ),
            ]

            discovered = []
            for pattern in win_patterns:
                try:
                    discovered.extend(glob.glob(pattern, recursive=True))
                except Exception:
                    pass

            discovered = [p for p in discovered if os.path.isfile(p)]
            if discovered:
                discovered.sort(key=lambda p: os.path.getmtime(p), reverse=True)
                auto_path = discovered[0]
                logger.info(f"[Browser] Auto-discovered ChromeDriver path: {auto_path}")
                return auto_path

    return ""


def _normalize_downloaded_chromedriver_path(raw_path: str) -> str:
    candidate = os.path.expandvars(os.path.expanduser(str(raw_path or "").strip().strip('"')))
    if not candidate:
        return ""

    if os.path.isdir(candidate):
        exe_name = "chromedriver.exe" if os.name == "nt" else "chromedriver"
        nested = os.path.join(candidate, exe_name)
        if os.path.isfile(nested):
            return nested
        return ""

    if os.path.isfile(candidate):
        base_name = os.path.basename(candidate).lower()
        if "chromedriver" in base_name:
            return candidate

        sibling = os.path.join(
            os.path.dirname(candidate),
            "chromedriver.exe" if os.name == "nt" else "chromedriver",
        )
        if os.path.isfile(sibling):
            return sibling

    return ""


def _auto_install_chromedriver(chrome_version: int) -> str:
    """Try one-time ChromeDriver bootstrap when no local binary is available."""
    global _AUTO_CHROMEDRIVER_PATH
    global _AUTO_CHROMEDRIVER_ATTEMPTED
    global _AUTO_CHROMEDRIVER_LAST_ERROR

    with _CHROMEDRIVER_CACHE_LOCK:
        if _AUTO_CHROMEDRIVER_PATH and os.path.isfile(_AUTO_CHROMEDRIVER_PATH):
            return _AUTO_CHROMEDRIVER_PATH

        if _AUTO_CHROMEDRIVER_ATTEMPTED:
            return ""

        _AUTO_CHROMEDRIVER_ATTEMPTED = True

    if ChromeDriverManager is None:
        _AUTO_CHROMEDRIVER_LAST_ERROR = "webdriver-manager package import failed"
        return ""

    major = max(1, int(chrome_version or 0))
    attempts = [
        {},
        {"driver_version": str(major)},
        {"driver_version": f"{major}.0.0.0"},
        {"driver_version": f"{max(1, major - 1)}.0.0.0"},
    ]

    attempt_errors = []
    for kwargs in attempts:
        try:
            downloaded = ChromeDriverManager(**kwargs).install()
            normalized = _normalize_downloaded_chromedriver_path(downloaded)
            if normalized and os.path.isfile(normalized):
                with _CHROMEDRIVER_CACHE_LOCK:
                    _AUTO_CHROMEDRIVER_PATH = normalized
                    _AUTO_CHROMEDRIVER_LAST_ERROR = ""
                logger.info(f"[Browser] Auto-downloaded ChromeDriver: {normalized}")
                return normalized

            attempt_errors.append(f"{kwargs or {'auto': True}} -> invalid path '{downloaded}'")
        except Exception as e:
            attempt_errors.append(f"{kwargs or {'auto': True}} -> {e!r}")

    with _CHROMEDRIVER_CACHE_LOCK:
        _AUTO_CHROMEDRIVER_LAST_ERROR = " | ".join(attempt_errors)

    logger.warning(f"[Browser] ChromeDriver auto-bootstrap failed: {_AUTO_CHROMEDRIVER_LAST_ERROR}")
    return ""


def _parse_proxy_config(proxy_value: str) -> dict:
    """Parse proxy input into a normalized config.

    Supported examples:
    - host:port
    - host:port:username:password
    - username:password@host:port
    - http://host:port
    - socks5://username:password@host:port
    """
    clean = str(proxy_value or "").strip()
    if not clean:
        return {}

    scheme = "http"
    host = ""
    port = 0
    username = ""
    password = ""

    if "://" in clean:
        parts = urlsplit(clean)
        scheme = str(parts.scheme or "http").strip().lower()
        host = str(parts.hostname or "").strip()
        port = int(parts.port or 0)
        username = unquote(parts.username or "")
        password = unquote(parts.password or "")
    else:
        raw = clean
        if "@" in raw:
            creds_part, host_part = raw.rsplit("@", 1)
            if ":" not in creds_part:
                raise ValueError(f"Invalid proxy credentials format: {clean}")
            username, password = creds_part.split(":", 1)
            raw = host_part

        parts = raw.split(":")
        if len(parts) == 2:
            host, port_text = parts
            port = int(port_text)
        elif len(parts) == 4:
            # Support both host:port:user:pass and user:pass:host:port
            if parts[1].isdigit() and not parts[3].isdigit():
                host, port_text, username, password = parts
            elif parts[3].isdigit() and not parts[1].isdigit():
                username, password, host, port_text = parts
            elif parts[1].isdigit():
                host, port_text, username, password = parts
            else:
                username, password, host, port_text = parts
            port = int(port_text)
        else:
            raise ValueError(f"Unsupported proxy format: {clean}")

        host = str(host or "").strip()

    if not host:
        raise ValueError(f"Missing proxy host: {clean}")
    if port <= 0 or port > 65535:
        raise ValueError(f"Invalid proxy port: {clean}")

    if scheme == "https":
        scheme = "http"
    if scheme == "socks":
        scheme = "socks5"

    if scheme not in ("http", "socks4", "socks5", "quic"):
        raise ValueError(f"Unsupported proxy scheme '{scheme}' in: {clean}")

    # Chrome does not reliably support username/password auth for SOCKS proxies.
    if scheme in ("socks4", "socks5") and (username or password):
        raise ValueError(
            f"SOCKS proxy auth is not supported for automated Chrome sessions: {clean}"
        )

    return {
        "scheme": scheme,
        "host": host,
        "port": int(port),
        "username": str(username or "").strip(),
        "password": str(password or ""),
    }


def _proxy_server_from_config(proxy_config: dict) -> str:
    if not proxy_config:
        return ""
    return f"{proxy_config['scheme']}://{proxy_config['host']}:{proxy_config['port']}"


def _start_local_proxy_tunnel(proxy_config: dict):
    username = str(proxy_config.get("username") or "").strip()
    password = str(proxy_config.get("password") or "")
    if not username and not password:
        return _proxy_server_from_config(proxy_config), None

    scheme = str(proxy_config.get("scheme") or "http").strip().lower()
    if scheme != "http":
        raise ValueError("Authenticated proxy tunnel currently supports only HTTP upstream proxies")

    host = str(proxy_config.get("host") or "").strip()
    port = int(proxy_config.get("port") or 0)
    if not host or port <= 0:
        raise ValueError("Invalid upstream proxy config for authenticated tunnel")

    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    auth_header = f"Basic {token}"

    server = _AuthenticatedForwardProxy(
        ("127.0.0.1", 0),
        _AuthenticatedForwardProxyHandler,
        host,
        port,
        auth_header,
    )
    _LOCAL_PROXY_SERVERS.append(server)

    thread = threading.Thread(
        target=server.serve_forever,
        name=f"proxy_tunnel_{host}_{port}",
        daemon=True,
    )
    thread.start()

    local_host, local_port = server.server_address
    return f"http://{local_host}:{local_port}", server


def _build_proxy_auth_extension(proxy_config: dict) -> str:
    """Create a temporary MV3 extension that supplies proxy credentials."""
    username = str(proxy_config.get("username") or "").strip()
    password = str(proxy_config.get("password") or "")
    if not username and not password:
        return ""

    ext_dir = tempfile.mkdtemp(prefix="ig_proxy_auth_")
    _TEMP_PROXY_EXTENSION_DIRS.append(ext_dir)

    manifest = {
        "name": "IG Proxy Auth",
        "version": "1.0.0",
        "manifest_version": 3,
        "permissions": [
            "webRequest",
            "webRequestAuthProvider",
        ],
        "host_permissions": ["<all_urls>"],
        "background": {"service_worker": "background.js"},
    }

    with open(os.path.join(ext_dir, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh)

    background_js = (
        "chrome.webRequest.onAuthRequired.addListener("
        " function(details, callbackFn) {"
        "  if (!details || !details.isProxy) { callbackFn({}); return; }"
        "  callbackFn({authCredentials: {"
        f"   username: {json.dumps(username)},"
        f"   password: {json.dumps(password)}"
        "  }});"
        " },"
        " {urls: ['<all_urls>']},"
        " ['asyncBlocking']"
        ");"
    )

    with open(os.path.join(ext_dir, "background.js"), "w", encoding="utf-8") as fh:
        fh.write(background_js)

    return ext_dir


def _mask_proxy_for_log(proxy_server: str) -> str:
    clean = str(proxy_server or "").strip()
    if not clean:
        return ""

    if "@" not in clean:
        return clean

    if "://" in clean:
        scheme, rest = clean.split("://", 1)
        prefix = f"{scheme}://"
    else:
        rest = clean
        prefix = ""

    creds, host = rest.rsplit("@", 1)
    if ":" in creds:
        username = creds.split(":", 1)[0]
        safe_creds = f"{username}:***"
    else:
        safe_creds = "***"

    return f"{prefix}{safe_creds}@{host}"


def _chromedriver_major_version(chromedriver_path: str):
    candidate = str(chromedriver_path or "").strip()
    if not candidate or not os.path.isfile(candidate):
        return None

    try:
        result = subprocess.run(
            [candidate, "--version"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:
        return None

    text = f"{result.stdout}\n{result.stderr}"
    match = re.search(r"(\d+)\.", text)
    if not match:
        return None

    try:
        return int(match.group(1))
    except Exception:
        return None


def _build_chrome_options(headless: bool, temp_profile_dir: str, user_agent: str, effective_proxy: str = ""):
    options = uc.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-notifications")
    options.add_argument("--incognito")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-features=PasswordManagerOnboarding,AutofillServerCommunication,AccountConsistency")
    options.add_argument(f"--user-agent={user_agent}")
    options.add_experimental_option(
        "prefs",
        {
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
            "profile.default_content_setting_values.notifications": 2,
        },
    )
    options.add_argument(f"--user-data-dir={temp_profile_dir}")

    if effective_proxy:
        options.add_argument(f"--proxy-server={effective_proxy}")
        options.add_argument("--proxy-bypass-list=<-loopback>")

    return options


def create_driver(headless=False, proxy=None):
    """
    Create an undetected Chrome browser instance.

    Returns:
        uc.Chrome: Configured undetected Chrome driver
    """
    if bool(headless):
        logger.warning("[Browser] Headless request ignored (forced visible browser mode)")

    # Force visible browser mode for all runtime paths.
    headless = False

    chrome_version = _detect_chrome_version()

    # Use a unique temporary Chrome user-data directory per driver so each
    # browser session starts fully fresh (no carry-over cookies/storage).
    temp_profile_dir = tempfile.mkdtemp(prefix="ig_chrome_profile_")
    _TEMP_BROWSER_PROFILE_DIRS.append(temp_profile_dir)

    selected_user_agent = random.choice(USER_AGENTS)

    proxy_config = _parse_proxy_config(proxy)
    proxy_server = _proxy_server_from_config(proxy_config)
    has_proxy_auth = bool(proxy_config.get("username") or proxy_config.get("password"))
    local_tunnel_server = None
    effective_proxy = ""
    if proxy_server:
        effective_proxy = proxy_server
        if has_proxy_auth:
            effective_proxy, local_tunnel_server = _start_local_proxy_tunnel(proxy_config)
            logger.info(
                "[Browser] Proxy auth tunnel enabled: "
                f"{_mask_proxy_for_log(effective_proxy)} -> {_mask_proxy_for_log(proxy_server)}"
            )
        logger.info(f"[Browser] Proxy enabled: {_mask_proxy_for_log(proxy_server)}")

    driver = None
    chromedriver_path = _resolve_chromedriver_path()
    if chromedriver_path:
        driver_major = _chromedriver_major_version(chromedriver_path)
        if driver_major and driver_major != chrome_version:
            logger.warning(
                "[Browser] ChromeDriver major mismatch "
                f"(driver={driver_major}, chrome={chrome_version}). Trying auto-bootstrap."
            )

    auto_bootstrap_path = ""
    if not chromedriver_path:
        auto_bootstrap_path = _auto_install_chromedriver(chrome_version)
    else:
        existing_major = _chromedriver_major_version(chromedriver_path)
        if existing_major and existing_major != chrome_version:
            auto_bootstrap_path = _auto_install_chromedriver(chrome_version)

    path_candidates = []
    for candidate in [chromedriver_path, auto_bootstrap_path, ""]:
        clean = str(candidate or "").strip()
        if clean in path_candidates:
            continue
        path_candidates.append(clean)

    attempt_specs = []
    seen_specs = set()
    for path_candidate in path_candidates:
        for version_pin in (chrome_version, None):
            key = (path_candidate, version_pin)
            if key in seen_specs:
                continue
            seen_specs.add(key)
            attempt_specs.append((path_candidate, version_pin))

    startup_errors = []
    selected_driver_path = ""
    for path_candidate, version_pin in attempt_specs:
        options = _build_chrome_options(
            headless=bool(headless),
            temp_profile_dir=temp_profile_dir,
            user_agent=selected_user_agent,
            effective_proxy=effective_proxy,
        )

        chrome_kwargs = {
            "options": options,
            "use_subprocess": True,
        }
        if path_candidate:
            chrome_kwargs["driver_executable_path"] = path_candidate

        label_parts = []
        if path_candidate:
            label_parts.append(f"path={path_candidate}")
        else:
            label_parts.append("path=auto")
        if version_pin is None:
            label_parts.append("version_main=None")
        else:
            label_parts.append(f"version_main={version_pin}")
        attempt_label = ", ".join(label_parts)

        try:
            if version_pin is None:
                driver = uc.Chrome(**chrome_kwargs)
            else:
                driver = uc.Chrome(version_main=version_pin, **chrome_kwargs)

            selected_driver_path = path_candidate
            logger.info(f"[Browser] Chrome started successfully ({attempt_label})")
            break
        except Exception as e:
            startup_errors.append(f"{attempt_label}: {e!r}")

    if driver is None:
        if local_tunnel_server is not None:
            try:
                local_tunnel_server.shutdown()
            except Exception:
                pass
            try:
                local_tunnel_server.server_close()
            except Exception:
                pass
            try:
                _LOCAL_PROXY_SERVERS.remove(local_tunnel_server)
            except Exception:
                pass

        try:
            _TEMP_BROWSER_PROFILE_DIRS.remove(temp_profile_dir)
        except Exception:
            pass
        _safe_remove_dir(temp_profile_dir)

        autodownload_error = ""
        with _CHROMEDRIVER_CACHE_LOCK:
            autodownload_error = str(_AUTO_CHROMEDRIVER_LAST_ERROR or "").strip()

        detail = " | ".join(startup_errors) if startup_errors else "unknown"
        if not chromedriver_path and not auto_bootstrap_path and autodownload_error:
            detail = f"{detail} | chromedriver_bootstrap={autodownload_error}"
        raise RuntimeError(f"Chrome startup failed ({detail})")

    if selected_driver_path:
        logger.info(f"[Browser] Using ChromeDriver path: {selected_driver_path}")

    if local_tunnel_server is not None:
        setattr(driver, "_local_proxy_tunnel", local_tunnel_server)
    setattr(driver, "_temp_user_data_dir", temp_profile_dir)

    # Additional stealth: override navigator properties
    driver.execute_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
        window.chrome = { runtime: {} };
    """)

    if not headless:
        # Give the top-level browser window a moment to fully materialize, then force focus.
        time.sleep(0.35)
        _maximize_and_focus_browser(driver)

    return driver


def close_driver(driver):
    """Safely close the browser driver (handles Windows cleanup errors)."""
    if not driver:
        return

    tunnel = getattr(driver, "_local_proxy_tunnel", None)
    temp_profile_dir = getattr(driver, "_temp_user_data_dir", None)
    if tunnel is not None:
        try:
            tunnel.shutdown()
        except Exception:
            pass
        try:
            tunnel.server_close()
        except Exception:
            pass
        try:
            _LOCAL_PROXY_SERVERS.remove(tunnel)
        except ValueError:
            pass
        except Exception:
            pass

    try:
        driver.close()
    except Exception:
        pass
    try:
        driver.quit()
    except OSError:
        pass  # WinError 6: The handle is invalid — safe to ignore
    except Exception:
        pass

    if temp_profile_dir:
        try:
            _TEMP_BROWSER_PROFILE_DIRS.remove(temp_profile_dir)
        except Exception:
            pass
        _safe_remove_dir(temp_profile_dir)
