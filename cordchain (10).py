#!/usr/bin/env python3
"""
CORDCHAIN — MedCore -> MEGA.nz cordchain app. One file. dex in -> cordchain.json out.

RUN ON COLAB (cells):
    from google.colab import drive
    drive.mount('/content/drive')
    %run /content/drive/MyDrive/Cordchain/cordchain.py                     (ALL providers)
    %run /content/drive/MyDrive/Cordchain/cordchain.py --provider Marrow   (one provider)
    %run /content/drive/MyDrive/Cordchain/cordchain.py --provider Marrow DAMS
    %run /content/drive/MyDrive/Cordchain/cordchain.py Marrow DAMS         (bare names work too)
    %run /content/drive/MyDrive/Cordchain/cordchain.py --list              (list providers + counts)

PROJECT DIR (created automatically): /content/drive/MyDrive/Cordchain/
    cordchain.json    THE one state file — ONLY uploaded + url-verified videos go in
                      (per-video details + the mega.nz url, appended one by one).
                      Delete it any time for a clean start — the app recreates it
                      instantly and simply re-uploads whatever is missing from it.
    mega_credentials.json / .txt   every MEGA account used
    mega_auth.json / medcore_account.json   current sessions
    dex.cache.json    auto-cached dex (offline fallback)
    progress prints go to the console; temp downloads live in /content/medcore_tmp

NO ARGUMENTS REQUIRED. A bare run transfers EVERY provider; only uploaded +
url-verified videos land in cordchain.json — there is no index.json, no
progress.json, nothing else to check or maintain. --provider narrows ONE run
to those providers (names are case-insensitive, a unique part of a name is
enough, commas work); the other providers simply stay pending for the next
run. --list prints every provider with its video count.

THE WORKFLOW (exactly this, nothing else):
    1. no cordchain.json -> the app creates one immediately (empty ledger)
    2. dex is fetched from DEX_URL (the full multi-provider list); --dex overrides
    3. workers transfer videos one-by-one (PARALLEL is auto-set to 1 — MedCore
         punishes 2+ TCP connections from one IP; the HTTP/2 path multiplexes
         DL_H2_STREAMS ranged streams over a single connection instead)
         -> upload to MEGA as <Provider>/<Subject>/<NN - Name>.mp4
         -> print the video details + the exported mega.nz url
         -> validate FAST: one mega.nz api call proves the link is live and
            the byte size matches (no full re-download of every video)
         -> ONLY THEN the video is appended to cordchain.json
    4. anything not uploaded, or without a working mega url, is NEVER written
       to cordchain.json — a rerun retries exactly those videos in sequence
    5. MEGA storage full -> a brand-new MEGA account is generated automatically
       (f-o/MEGA-Account-Generator; every account is login-verified first).
       MedCore temp accounts rotate ONLY on real auth rejections — a removed
       video (404) is parked as failed honestly, zero fake "account suspended"
       messages, zero account churn.
    6. rerunning the app resumes: videos already recorded in cordchain.json
       are skipped; everything else is retried in sequence.

dex.md FORMATS (both accepted, auto-detected):
    a) a dumped dex.json (the JSON with the library/store structure) — raw,
       inside a code fence, or after any heading: the {"library"...} object is
       auto-extracted, so a pasted dump can never be mistaken for markdown
    b) markdown:   # Provider
                   ## Subject
                   ### Section
                   - [Lecture name](https://...stream-url...)
       every http(s) link becomes a video in document order; the video id is
       the uuid found in the url, or a hash of the url when it has none.
       mega.nz links are outputs, not sources, and are ignored.

Optional CLI args: --provider NAME [NAME ...] (filter this run), --dex
PATH_OR_URL (alternate dex source), --list (providers + counts, then exit).

Optional env knobs: PARALLEL (auto-forced to 1 unless PARALLEL_FORCE=1 —
MedCore punishes concurrent TCP connections; speed comes from DL_H2_STREAMS
(default 8) h2 streams over ONE connection, plus DL_STALL_RETRIES /
DL_H2_ROUNDS stall-recovery knobs), DL_CONNECTIONS (legacy fallback, default
6), MULTI_MIN_MB (default 24 — smaller files use one connection),
VERIFY_FULL (default false = fast mega.nz api validation; true = legacy full
re-download proof), SNO (start at global #N), VERIFY_MD5 (md5 check with
VERIFY_FULL), VERIFY_RETRIES, MEGA_EMAIL/MEGA_PASSWORD,
MEDCORE_EMAIL/MEDCORE_PASSWORD (temp account only — your own account is never
modified), DEX_URL, MAX_RETRIES, CLEANUP_TEMP, CHUNK_SIZE,
MEGA_MARGIN_MB, MEGA_AUTOGEN,
UPLOAD_CONNECTIONS (default 4 parallel upload connections), UPLOAD_BLOCK_MB (default 4).
"""

import argparse
import base64
import csv
import gc
import hashlib
import json
import os
import platform
import random
import re
import secrets
import shutil
import string
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import binascii
import struct
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests

DEX_URL = os.environ.get("DEX_URL", "https://raw.githubusercontent.com/Runz1337/Data3/refs/heads/main/dex.json")
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", str(8 * 1024 * 1024)))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))
RATE_LIMIT = int(os.environ.get("RATE_LIMIT", "600"))
RATE_WINDOW_S = int(os.environ.get("RATE_WINDOW_S", "300"))
JITTER_BASE = float(os.environ.get("JITTER_BASE", "1.5"))
JITTER_MAX = float(os.environ.get("JITTER_MAX", "8.0"))
BACKOFF_BASE = float(os.environ.get("BACKOFF_BASE", "2.0"))
CLEANUP_TEMP = os.environ.get("CLEANUP_TEMP", "true").lower() == "true"
MEGA_MARGIN_BYTES = int(os.environ.get("MEGA_MARGIN_MB", "250")) * 1024 * 1024
MEGA_UPLOAD_RETRIES = int(os.environ.get("MEGA_UPLOAD_RETRIES", "3"))
MEGA_PUT_TIMEOUT_S = int(os.environ.get("MEGA_PUT_TIMEOUT_S", "900"))
MEGA_GET_TIMEOUT_S = int(os.environ.get("MEGA_GET_TIMEOUT_S", "7200"))
MEDCORE_EMAIL = os.environ.get("MEDCORE_EMAIL", "")
MEDCORE_PASSWORD = os.environ.get("MEDCORE_PASSWORD", "")
MEGA_EMAIL = os.environ.get("MEGA_EMAIL", "")
MEGA_PASSWORD = os.environ.get("MEGA_PASSWORD", "")
QUOTA_KEYWORDS = ("quota", "overquota", "eoverquota", "full")
PARALLEL = max(1, int(os.environ.get("PARALLEL", "3")))
DL_CONNECTIONS = max(1, int(os.environ.get("DL_CONNECTIONS", "6")))
MULTI_MIN_BYTES = int(os.environ.get("MULTI_MIN_MB", "24")) * 1024 * 1024
VERIFY_FULL = os.environ.get("VERIFY_FULL", "false").lower() == "true"
ROTATE_MIN_GAP_S = float(os.environ.get("ROTATE_MIN_GAP_S", "20"))
MEGA_NUMBERING = os.environ.get("MEGA_NUMBERING", "true").lower() == "true"
MEGA_AUTOGEN = os.environ.get("MEGA_AUTOGEN", "true").lower() == "true"
SNO = int(os.environ.get("SNO", "0"))
VERIFY_MD5 = os.environ.get("VERIFY_MD5", "false").lower() == "true"
DL_CHUNK_SIZE = max(64 * 1024, int(os.environ.get("DL_CHUNK_SIZE", str(1024 * 1024))))
DL_H2_STREAMS = max(2, int(os.environ.get("DL_H2_STREAMS", "8")))
DL_STALL_RETRIES = max(2, int(os.environ.get("DL_STALL_RETRIES", "4")))  # consecutive ZERO-progress attempts a segment tolerates per round
DL_H2_ROUNDS = max(1, int(os.environ.get("DL_H2_ROUNDS", "3")))          # consecutive rounds with no file-wide progress before the file gives up
UPLOAD_CONNECTIONS = max(1, int(os.environ.get("UPLOAD_CONNECTIONS", "4")))
UPLOAD_BLOCK_MB = max(1, int(os.environ.get("UPLOAD_BLOCK_MB", "4")))
RAM_EVICT_EVERY = max(16 * 1024 * 1024, int(os.environ.get("RAM_EVICT_EVERY_MB", "128")) * 1024 * 1024)
RAM_EVICT_MB = max(256, int(os.environ.get("RAM_EVICT_MB", "1400")))
RAM_FLOOR_MB = max(128, int(os.environ.get("RAM_FLOOR_MB", "700")))
RAM_GUARD_S = max(1.0, float(os.environ.get("RAM_GUARD_S", "4")))
MEDCORE_ROTATE_ROUNDS = max(1, int(os.environ.get("MEDCORE_ROTATE_ROUNDS", "3")))
AUTH_RECOVER_ATTEMPTS = max(1, int(os.environ.get("AUTH_RECOVER_ATTEMPTS", "4")))
MEGA_MAX_SWITCHES = max(3, int(os.environ.get("MEGA_MAX_SWITCHES", "15")))
MEGA_SESSION_KEYWORDS = ("not logged in", "login required", "session", "blocked", "suspended",
                         "banned", "deactivated", "credentials", "nonexistent user", "access denied")
VERIFY_RETRIES = int(os.environ.get("VERIFY_RETRIES", "2"))
MEGA_GEN_REPO = "https://github.com/f-o/MEGA-Account-Generator"
MEGA_GEN_HOME = Path(os.environ.get("MEGA_GEN_HOME", str(Path.home() / ".megagen")))
_MEGA_ARCH = {"x86_64": "x86_64", "amd64": "x86_64", "aarch64": "aarch64", "arm64": "aarch64"}.get(
    platform.machine().lower(), platform.machine().lower())
MEGA_TOOLS_URL = os.environ.get(
    "MEGA_TOOLS_URL", f"https://xff.cz/megatools/builds/builds/megatools-1.11.5.20250706-linux-{_MEGA_ARCH}.tar.gz")
MEGA_GEN_TIMEOUT_S = int(os.environ.get("MEGA_GEN_TIMEOUT_S", "420"))
MEGA_GEN_ATTEMPTS = int(os.environ.get("MEGA_GEN_ATTEMPTS", "2"))
MEGA_LINK_RE = re.compile(r"https://mega\.nz/(?:file|folder)/[A-Za-z0-9_-]+#[A-Za-z0-9_-]+")
MEGA_LEGACY_LINK_RE = re.compile(r"https://mega\.nz/#![A-Za-z0-9_-]+![A-Za-z0-9_-]+")
UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)
MD_LINK_RE = re.compile(r"\[([^\]]*)\]\((https?://[^)\s]+)\)")
BARE_URL_RE = re.compile(r"(?<![(\[])(https?://[^\s\)\]>\"']+)")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")
DEX_JSON_HINT_RE = re.compile(r'"(?:library|store|streamUrl|lectureName)"')
JSON_EXTRACT_PROBES = 8


class UserAbort(Exception):
    pass


class MedCoreAuthError(RuntimeError):
    pass


class NoRangeError(RuntimeError):
    pass


class Http1Fallback(Exception):
    pass


def is_mega_link(url: str) -> bool:
    u = url or ""
    return bool(MEGA_LINK_RE.search(u) or MEGA_LEGACY_LINK_RE.search(u))


def mega_link_handle(link: str) -> str:
    m = MEGA_LINK_RE.search(link or "") or MEGA_LEGACY_LINK_RE.search(link or "")
    if not m:
        return ""
    u = m.group(0)
    if "/file/" in u:
        return u.split("/file/", 1)[1].split("#")[0]
    if "/#!" in u:
        return u.split("/#!", 1)[1].split("!")[0]
    return ""


def segment_bounds(total: int, conns: int):
    if total <= 0 or conns < 1:
        return []
    seg = max(1, total // conns)
    bounds = []
    start = 0
    for i in range(conns):
        end = total - 1 if i == conns - 1 else start + seg - 1
        if end < start:
            break
        bounds.append((start, end))
        start = end + 1
    return bounds


def mega_public_size_probe(link: str, attempts: int = 3):
    handle = mega_link_handle(link)
    if not handle:
        return "dead", "link carries no file handle"
    last = ""
    for attempt in range(1, attempts + 1):
        try:
            r = requests.post("https://g.api.mega.co.nz/cs", params={"id": str(attempt)},
                              json=[{"a": "g", "p": handle}], timeout=20)
            data = r.json()
            if isinstance(data, list) and data:
                first = data[0]
                if isinstance(first, dict) and "s" in first:
                    return "ok", int(first["s"])
                if isinstance(first, int):
                    return "dead", f"mega api error {first}"
            return "dead", f"unexpected mega api response: {str(data)[:80]}"
        except Exception as e:
            last = str(e)
            time.sleep(BACKOFF_BASE * attempt + random.uniform(0, JITTER_BASE))
    return "unreachable", last or "mega api unreachable"


def sanitise(name: str) -> str:
    name = (name or "").replace("\\", "/").strip("/")
    name = re.sub(r"[^\w.\- ]", "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:180] or "untitled"


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def human(n: float) -> str:
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{int(n)}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


def errsig(e: BaseException, cap: int = 160) -> str:
    """Short one-line exception signature for inline logs (type + first 160 chars)."""
    msg = " ".join(str(e).split())
    return f"{type(e).__name__}: {msg[:cap]}" if msg else type(e).__name__


def drop_file_cache(path) -> None:
    try:
        if not hasattr(os, "posix_fadvise") or not path or not Path(path).exists():
            return
        fd = os.open(str(path), os.O_RDONLY)
        try:
            os.fsync(fd)
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)
    except Exception:
        pass


def available_ram_mb():
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) // 1024
    except Exception:
        pass
    return None


def md5_file(path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()


class RamGuard:
    def __init__(self):
        self.lock = threading.Lock()
        self.pause = threading.Event()
        self._tracked = {}
        self._stop = threading.Event()
        self.thread = None

    def track(self, path):
        with self.lock:
            self._tracked[str(path)] = time.time()

    def untrack(self, path):
        with self.lock:
            self._tracked.pop(str(path), None)

    def note_written(self, fd, start, end):
        try:
            os.fsync(fd)
        except Exception:
            pass
        try:
            if end > start and hasattr(os, "posix_fadvise"):
                os.posix_fadvise(fd, start, end - start, os.POSIX_FADV_DONTNEED)
        except Exception:
            pass
        self.sweep()

    def sweep(self):
        avail = available_ram_mb()
        if avail is None:
            return
        if avail < RAM_EVICT_MB:
            with self.lock:
                paths = list(self._tracked)
            for p in paths:
                drop_file_cache(p)
            gc.collect()
        if avail < RAM_FLOOR_MB:
            if not self.pause.is_set():
                self.pause.set()
                print(f"\n[ram] only {avail} MB available — pausing downloads until memory recovers "
                      f"(floor {RAM_FLOOR_MB} MB; already-written data is being evicted from cache)")
        elif self.pause.is_set():
            self.pause.clear()
            print(f"\n[ram] recovered ({avail} MB available) — downloads resumed")

    def wait(self):
        if not self.pause.is_set():
            return
        while self.pause.is_set() and not self._stop.is_set():
            time.sleep(0.5)
            try:
                self.sweep()
            except Exception:
                pass

    def _loop(self):
        while not self._stop.wait(RAM_GUARD_S):
            try:
                self.sweep()
            except Exception:
                pass

    def start(self):
        if self.thread is not None:
            return
        self._stop.clear()
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def stop(self):
        self._stop.set()
        if self.thread is not None:
            self.thread.join(timeout=RAM_GUARD_S + 2)
        self.thread = None
        self.pause.clear()


def generate_credentials():
    tag = "".join(random.choices(string.ascii_lowercase, k=4)) + "".join(random.choices(string.digits, k=8))
    return f"mc.{tag}@medcoreapp.dev", secrets.token_urlsafe(14)


_HTTPX = None


def _ensure_httpx():
    """Import httpx(+h2) for HTTP/2 multiplexed downloads; pip-install on Colab if missing.
    Returns the module, or None when unavailable."""
    global _HTTPX
    if _HTTPX is not None:
        return _HTTPX or None
    try:
        import httpx
        mod = httpx
    except Exception:
        mod = None
    if mod is None:
        try:
            print("[dl] installing httpx[http2] for high-speed HTTP/2 downloads ...")
            subprocess.run([sys.executable, "-m", "pip", "install", "-q", "httpx[http2]", "h2"],
                           capture_output=True, text=True, timeout=300)
            import httpx
            mod = httpx
        except Exception:
            mod = None
    if mod is not None:
        try:
            import h2  # noqa: F401  (httpx refuses http2=True without the h2 package)
        except Exception:
            try:
                subprocess.run([sys.executable, "-m", "pip", "install", "-q", "h2"],
                               capture_output=True, text=True, timeout=300)
                import h2  # noqa: F401
            except Exception:
                mod = None
    _HTTPX = mod if mod is not None else False
    if mod is None:
        print("[dl] HTTP/2 downloads unavailable — falling back to legacy multi-connection mode")
    return _HTTPX or None


def _ensure_pycryptodome() -> None:
    """MEGA upload crypto needs AES (pycryptodome). Colab does not ship it."""
    global AES
    if AES is not None:
        return
    print("[setup] installing pycryptodome (one-time, needed for MEGA uploads) ...")
    cmds = [[sys.executable, "-m", "pip", "install", "-q", "pycryptodome"],
            [sys.executable, "-m", "pip", "install", "-q", "--break-system-packages", "pycryptodome"],
            [sys.executable, "-m", "pip", "install", "-q", "--user", "--break-system-packages", "pycryptodome"]]
    for c in cmds:
        try:
            if subprocess.run(c, capture_output=True, text=True, timeout=900).returncode == 0:
                from Crypto.Cipher import AES as _AES
                from Crypto.Util import Counter as _Counter
                AES, Counter = _AES, _Counter
                print("[setup] pycryptodome ready")
                return
        except Exception:
            continue
    raise RuntimeError("could not install pycryptodome — run: pip install pycryptodome")


def ensure_drive_mounted() -> bool:
    if Path("/content/drive/MyDrive").exists():
        return True
    if not os.path.exists("/content"):
        return False
    try:
        from google.colab import drive
        drive.mount("/content/drive")
        return Path("/content/drive/MyDrive").exists()
    except Exception as e:
        print(f"[drive] auto-mount failed: {e}")
        print("[drive] run in a notebook cell first:  from google.colab import drive; drive.mount('/content/drive')")
        return False


def resolve_root():
    env = os.environ.get("CORDCHAIN_ROOT")
    if env:
        return Path(env), "env"
    if ensure_drive_mounted():
        return Path("/content/drive/MyDrive/Cordchain"), "drive"
    print("[drive] not mounted — falling back to /content/Cordchain (cordchain.json will NOT persist to Drive)")
    return Path("/content/Cordchain"), "local"


class FileStore:
    def __init__(self, root: Path):
        self.root = Path(root)
        self.lock = threading.RLock()
        try:
            self.root.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"[store] cannot create {self.root}: {e}")

    def _write(self, path, data) -> bool:
        payload = json.dumps(data, indent=2, ensure_ascii=False)
        tmp = Path(str(path) + ".tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
            return True
        except Exception:
            try:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(payload)
                return True
            except Exception as e:
                print(f"[store] write failed {path}: {e}")
                return False

    def _read(self, path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def load_chain(self) -> dict:
        return self._read(self.root / "cordchain.json") or {}

    def save_chain(self, data) -> bool:
        return self._write(self.root / "cordchain.json", data)

    def save_raw(self, name, data) -> bool:
        if isinstance(data, str):
            tmp = Path(str(self.root / name) + ".tmp")
            try:
                with open(tmp, "w", encoding="utf-8") as f:
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, self.root / name)
                return True
            except Exception as e:
                print(f"[store] write failed {self.root / name}: {e}")
                return False
        return self._write(self.root / name, data)

    def load_account(self, name):
        return self._read(self.root / name)

    def save_account(self, name, data) -> bool:
        return self._write(self.root / name, data)


class AuthManager:
    BASE = "https://medcore-api.gohere.in"
    HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0 Safari/537.36",
               "Accept": "*/*",
               "X-Learning-App": "medcore",
               "Content-Type": "application/json"}

    INVALID_HINTS = ("invalid", "incorrect", "wrong", "credential", "not found",
                     "unknown", "no user", "does not exist", "doesn't exist",
                     "locked", "banned", "disabled", "suspended")

    def __init__(self, email: str, password: str, retries: int = MAX_RETRIES, store=None):
        self.email = email
        self.password = password
        self.retries = retries
        self.token = None
        self.expiry = 0.0
        self.token_issued_at = 0.0
        self.user = {}
        self.trial_expiry = 0.0
        self.store = store
        self.lock = threading.RLock()
        self.rotation_lock = threading.Lock()
        self.rotated_at = 0.0

    def _absorb(self, b: dict) -> bool:
        if not b.get("token"):
            return False
        self.token = b["token"]
        self.token_issued_at = time.time()
        payload = self.token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
        self.expiry = claims.get("exp", 0)
        self.user = b.get("user", {})
        self.trial_expiry = 0.0
        raw = str(self.user.get("trialExpiresAt") or "")
        try:
            if raw:
                self.trial_expiry = datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
        except Exception:
            self.trial_expiry = 0.0
        if not self.trial_expiry and self.user.get("trialSecondsRemaining"):
            try:
                self.trial_expiry = time.time() + float(self.user["trialSecondsRemaining"])
            except (TypeError, ValueError):
                self.trial_expiry = 0.0
        return True

    def trial_seconds_left(self) -> float:
        if not self.trial_expiry:
            return 1e12
        return self.trial_expiry - time.time()

    def coordinated_rotation(self, reason: str = "") -> bool:
        with self.rotation_lock:
            elapsed = time.time() - self.rotated_at
            if elapsed < ROTATE_MIN_GAP_S and self.token:
                print(f"  [auth] account {self.email} rotated {elapsed:.0f}s ago — workers share it ({reason})")
                return True
            print(f"[medcore] creating a new temp account ({reason})")
            ok = self.fresh_account_persistent()
            self.rotated_at = time.time()
            return ok

    def _register(self) -> bool:
        local = self.email.split("@")[0].replace(".", " ").replace("mc", "MC", 1)
        name = " ".join(w.capitalize() for w in local.split() if w) or "MedCore User"
        payload = {"email": self.email, "password": self.password, "confirmPassword": self.password,
                   "name": name, "siteId": "medcore"}
        for path in ("/api/auth/register", "/api/auth/signup"):
            served = False
            for attempt in range(1, 3):
                code, b = self._request(path, "POST", payload, require_token=False)
                msg = str(b.get("message") or b.get("error") or "")
                low = msg.lower()
                if code in (200, 201):
                    self._absorb(b)
                    print(f"  [auth] registered {self.email} via {path}")
                    served = True
                    break
                if any(w in low for w in ("exist", "taken", "already")):
                    print(f"  [auth] {self.email} already registered — proceeding to login")
                    served = True
                    break
                if code in (404, 405):
                    break
                print(f"  [auth] register attempt {attempt} returned {code}: {msg or '?'}")
                time.sleep(BACKOFF_BASE * attempt + random.uniform(0, JITTER_BASE))
            if served:
                return True
        return False

    def fresh_account(self, max_tries: int = 3) -> bool:
        with self.lock:
            self.token = None
            self.expiry = 0.0
            self.trial_expiry = 0.0
            for attempt in range(1, max_tries + 1):
                email, password = generate_credentials()
                print(f"[medcore] rotating to fresh temp account {attempt}/{max_tries}: {email}")
                self.email, self.password = email, password
                self.retries = 2
                self._register()
                try:
                    if self.token or self._login():
                        print(f"[medcore] temp account active — password: {password}")
                        if self.store is not None:
                            self.store.save_account("medcore_account.json",
                                                    {"email": email, "password": password,
                                                     "updated": iso_now(), "mode": "temp"})
                        return True
                except RuntimeError as e:
                    print(f"  [auth] temp login failed: {e}")
                self.token = None
                self.expiry = 0.0
                time.sleep(BACKOFF_BASE * attempt + random.uniform(0, JITTER_BASE))
            print(f"[medcore] no working temp account after {max_tries} tries — MedCore may be "
                  f"rate-limiting new signups from this network; wait a few minutes and rerun "
                  f"(everything already chained is safe in cordchain.json)")
            return False

    def fresh_account_persistent(self) -> bool:
        for round_ in range(1, MEDCORE_ROTATE_ROUNDS + 1):
            if self.fresh_account():
                return True
            if round_ < MEDCORE_ROTATE_ROUNDS:
                wait = 20 * round_
                print(f"[medcore] signup still failing — waiting {wait}s and creating another "
                      f"account (round {round_ + 1}/{MEDCORE_ROTATE_ROUNDS})")
                time.sleep(wait)
        return False

    def _login(self) -> bool:
        codes = []
        last = "unknown error"
        for attempt in range(1, self.retries + 1):
            code, b = self._request("/api/auth/login", "POST",
                                    {"email": self.email, "password": self.password, "siteId": "medcore"},
                                    require_token=False)
            msg = str(b.get("message") or b.get("error") or "")
            if code in (200, 201) and self._absorb(b):
                return True
            last = msg or f"HTTP {code}"
            codes.append(code)
            print(f"  [auth] login attempt {attempt} returned {code}: {msg or '?'}")
            if code in (400, 401, 403) and any(h in msg.lower() for h in self.INVALID_HINTS):
                print(f"  [auth] server rejected the TEMP account {self.email} (\"{msg}\") — "
                      f"this never touches a personal MEDCORE_EMAIL account; "
                      f"switching to a fresh temp account")
                break
            time.sleep(BACKOFF_BASE * attempt + random.uniform(0, JITTER_BASE))
        self.last_error = last
        if codes and all(c in (400, 401, 403) for c in codes):
            return False
        raise RuntimeError(f"login failed after {len(codes)} attempt(s): {last}")

    def ensure_auth(self) -> str:
        with self.lock:
            if not self.token or time.time() >= (self.expiry - 60):
                if not self._login() and not self.fresh_account_persistent():
                    raise MedCoreAuthError("login rejected and fresh temp accounts failed")
                trial_note = ""
                if self.trial_expiry:
                    rem = int(self.trial_expiry - time.time())
                    trial_note = (f"; trial {'EXPIRED' if rem <= 0 else f'{rem // 60}m{rem % 60:02d}s left'}")
                print(f"  [auth] logged in as {self.user.get('name') or self.email}; "
                      f"expiry {time.strftime('%H:%M:%S', time.gmtime(self.expiry))}; "
                      f"access {self.user.get('accessStatus') or 'unknown'}{trial_note}"
                      + ("" if (self.user.get("accessStatus") in ("approved", "active"))
                         else " (pending accounts stream everything while the trial is live; "
                              "once it expires every stream 404s until a fresh account rotates in)"))
            return self.token

    def handle_reject(self, code: int = 401):
        with self.lock:
            print(f"  [auth] {code} received — forcing re-login")
            self.token = None
            self.expiry = 0.0
            if not self._login():
                if not self.coordinated_rotation(f"session rejected {code}"):
                    raise MedCoreAuthError("session rejected and fresh temp accounts failed")

    def _request(self, path: str, method: str = "GET", body=None, require_token: bool = True):
        h = dict(self.HEADERS)
        if require_token and self.token:
            h["Authorization"] = f"Bearer {self.token}"
        try:
            req = requests.request(method, self.BASE + path, headers=h, json=body, timeout=45)
            status = req.status_code
            try:
                body_json = req.json()
            except Exception:
                body_json = {}
            return status, body_json
        except Exception as e:
            return -1, {"message": str(e)}


class RateLimiter:
    def __init__(self, limit: int = RATE_LIMIT, window: int = RATE_WINDOW_S):
        self.limit = limit
        self.window = window
        self.start = time.time()
        self.remaining = limit
        self.hits = 0
        self.lock = threading.Lock()

    @staticmethod
    def _hget(headers, *names):
        for wanted in names:
            for k, v in headers.items():
                if k.lower() == wanted:
                    return v
        return None

    def note(self, headers: dict, cost: int = 1):
        with self.lock:
            self.hits += cost
            lim = self._hget(headers, "ratelimit-limit", "x-ratelimit-limit")
            rem = self._hget(headers, "ratelimit-remaining", "x-ratelimit-remaining")
            if rem is not None:
                try:
                    self.remaining = int(rem)
                except (TypeError, ValueError):
                    pass
            if lim is not None:
                try:
                    self.limit = int(lim)
                except (TypeError, ValueError):
                    pass

    def maybe_sleep(self, cost: int = 1):
        with self.lock:
            if self.remaining - cost <= 10:
                elapsed = time.time() - self.start
                sleep_for = max(0.0, self.window - elapsed) + random.uniform(0, JITTER_BASE)
                if sleep_for > 1:
                    print(f"  [rate] bucket nearly empty ({self.remaining} left, {self.hits} used) — sleeping {sleep_for:.0f}s")
                    time.sleep(sleep_for)
                self.start = time.time()
                self.remaining = self.limit
            self.remaining -= cost

    def check_429(self, headers: dict):
        with self.lock:
            reset = self._hget(headers, "ratelimit-reset", "x-ratelimit-reset")
            if reset:
                try:
                    reset = float(reset)
                    wait = reset - time.time() if reset > 1e6 else reset
                except (TypeError, ValueError):
                    wait = 0
                wait += random.uniform(1, JITTER_MAX)
                if wait > 0:
                    print(f"  [rate] 429 received — sleeping {wait:.0f}s until reset")
                    time.sleep(max(1, wait))
                self.start = time.time()
                self.remaining = self.limit


# ---------------------------------------------------------------------------
# native MEGA API client — replaces MEGAcmd entirely (no binaries, no sockets,
# constant memory: uploads stream from disk in small blocks)
# ---------------------------------------------------------------------------

try:
    from Crypto.Cipher import AES
    from Crypto.Util import Counter
except Exception:                                    # bootstrap later in main()
    AES = None
    Counter = None


class MegaApiError(RuntimeError):
    def __init__(self, message, code=0):
        super().__init__(message)
        self.code = code


MEGA_ERROR_TEXT = {
    -1: "mega internal error (EINTERNAL)",
    -2: "bad arguments (EARGS)",
    -3: "temporary mega error, retry (EAGAIN)",
    -4: "rate limited by mega (ERATELIMIT)",
    -5: "mega transfer failed (EFAILED)",
    -6: "mega too many connections (ETOOMANYCONNECTIONS)",
    -7: "mega write error (EWRITE)",
    -8: "mega read error (EREAD)",
    -9: "mega key or login error (EKEY)",
    -10: "mega data unavailable (EDATAUNAVAILABLE)",
    -11: "mega account or link blocked (EBLOCKED)",
    -12: "mega link expired (EEXPIRED)",
    -13: "mega storage full (EOVERQUOTA)",
    -14: "mega temporarily unavailable (ETEMPUNAVAIL)",
    -16: "mega write access blocked",
    -17: "mega file too big (ETOOBIG)",
    -18: "mega file too big (ETOOBIG)",
    -19: "mega too many concurrent transfers",
}


def _a32_to_str(a):
    return struct.pack(">%dI" % len(a), *a)


def _str_to_a32(b):
    if isinstance(b, str):
        b = b.encode("latin-1", "replace")
    if len(b) % 4:
        b += b"\0" * (4 - len(b) % 4)
    return struct.unpack(">%dI" % (len(b) // 4), b)


def _b64url_encode(data):
    return base64.b64encode(data).decode("ascii").replace("+", "-").replace("/", "_").replace("=", "")


def _b64url_decode(s):
    s += "=="[(2 - len(s) * 3) % 4:]
    return base64.b64decode(s.replace("-", "+").replace("_", "/").replace(",", ""))


def _a32_to_b64(a):
    return _b64url_encode(_a32_to_str(list(a)))


def _b64_to_a32(s):
    return _str_to_a32(_b64url_decode(s))


def _aes_cbc_encrypt_a32(data, key):
    c = AES.new(_a32_to_str(key), AES.MODE_CBC, b"\0" * 16)
    return _str_to_a32(c.encrypt(_a32_to_str(data)))


def _aes_cbc_decrypt_a32(data, key):
    c = AES.new(_a32_to_str(key), AES.MODE_CBC, b"\0" * 16)
    return _str_to_a32(c.decrypt(_a32_to_str(data)))


def _encrypt_key(a, key):
    out = ()
    for i in range(0, len(a), 4):
        out += tuple(_aes_cbc_encrypt_a32(list(a[i:i + 4]), key))
    return list(out)


def _decrypt_key(a, key):
    out = ()
    for i in range(0, len(a), 4):
        out += tuple(_aes_cbc_decrypt_a32(list(a[i:i + 4]), key))
    return list(out)


def _prepare_key(arr):
    pkey = [0x93C467E3, 0x7DB0C7A4, 0xD1BE3F81, 0x0152CB56]
    for _ in range(0x10000):
        for j in range(0, len(arr), 4):
            key = [0, 0, 0, 0]
            for i in range(4):
                if i + j < len(arr):
                    key[i] = arr[i + j]
            pkey = _aes_cbc_encrypt_a32(pkey, key)
    return pkey


def _stringhash(s, aeskey):
    s32 = _str_to_a32(s)
    h32 = [0, 0, 0, 0]
    for i in range(len(s32)):
        h32[i % 4] ^= s32[i]
    for _ in range(0x4000):
        h32 = _aes_cbc_encrypt_a32(h32, aeskey)
    return _a32_to_b64((h32[0], h32[2]))


def _encrypt_attr(attr, key):
    data = ("MEGA" + json.dumps(attr)).encode("utf-8")
    if len(data) % 16:
        data += b"\0" * (16 - len(data) % 16)
    return AES.new(_a32_to_str(key), AES.MODE_CBC, b"\0" * 16).encrypt(data)


def _decrypt_attr(data, key):
    try:
        raw = AES.new(_a32_to_str(key), AES.MODE_CBC, b"\0" * 16).decrypt(data)
    except Exception:
        return None
    text = raw.decode("latin-1", "replace").rstrip("\0")
    if not text.startswith("MEGA{"):
        return None
    try:
        return json.loads(text[4:])
    except Exception:
        return None


def _mpi_to_int(s):
    return int(binascii.hexlify(s[2:]), 16)


def _mega_chunks(size):
    """MEGA chunk boundaries: 128K doubling to 1MB, then 1MB steps."""
    p = 0
    s = 0x20000
    while p + s < size:
        yield (p, s)
        p += s
        if s < 0x100000:
            s += 0x20000
    yield (p, size - p)


class MegaApi:
    """Own-account MEGA client: login, node tree, streaming chunked upload,
    export, quota, delete, public download. RAM stays flat regardless of file
    size (data never sits in memory; blocks go straight from disk to wire)."""

    API_URL = "https://g.api.mega.co.nz/cs"

    def __init__(self):
        self.sid = None
        self.master_key = None
        self.email = ""
        self.password = ""
        self.seq = random.randint(0, 0xFFFFFFFF)
        self.root_handle = None
        self.nodes = {}          # handle -> {"p","t","name","size","key8"}
        self.path_to_h = {}      # "/Marrow/Psychiatry/01 - x.mp4" -> handle
        self.api_lock = threading.Lock()
        self.http = requests.Session()
        self.http.headers.update({"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) cordchain/1.0"})
        try:
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=UPLOAD_CONNECTIONS + 4, pool_maxsize=UPLOAD_CONNECTIONS + 4)
            self.http.mount("https://", adapter)
        except Exception:
            pass

    # -- api core -----------------------------------------------------------
    def api(self, payload, attempts=8):
        if not isinstance(payload, list):
            payload = [payload]
        last = "mega api unreachable"
        with self.api_lock:
            for attempt in range(1, attempts + 1):
                params = {"id": str(self.seq)}
                self.seq += 1
                if self.sid:
                    params["sid"] = self.sid
                try:
                    r = self.http.post(self.API_URL, params=params, data=json.dumps(payload),
                                       timeout=(30, 120))
                    resp = json.loads(r.text)
                except Exception as e:
                    last = f"mega api network error: {e}"
                else:
                    if isinstance(resp, int):
                        return resp
                    if isinstance(resp, list) and resp:
                        first = resp[0]
                        if isinstance(first, int):
                            if first == 0:
                                return first
                            if first == -3:
                                last = "mega api busy (EAGAIN)"
                            else:
                                raise MegaApiError(
                                    MEGA_ERROR_TEXT.get(first, f"mega api error {first}"), code=first)
                        else:
                            return first
                time.sleep(min(30, BACKOFF_BASE * attempt + random.uniform(0, JITTER_BASE)))
        raise MegaApiError(last, code=0)

    # -- login ----------------------------------------------------------------
    def login(self, email, password):
        email = (email or "").strip().lower()
        if not email or not password:
            return False
        resp0 = self.api({"a": "us0", "user": email})
        if isinstance(resp0, int):
            raise MegaApiError(MEGA_ERROR_TEXT.get(resp0, f"mega login error {resp0}"), code=resp0)
        try:
            salt = _b64_to_a32(resp0["s"])                      # v2 account
            pw_key = hashlib.pbkdf2_hmac("sha512", password.encode(),
                                         _a32_to_str(salt), 100000, 32)
            password_aes = list(_str_to_a32(pw_key[:16]))
            user_hash = _b64url_encode(pw_key[16:])
        except (KeyError, TypeError):                           # v1 account
            password_aes = _prepare_key(list(_str_to_a32(password)))
            user_hash = _stringhash(email, password_aes)
        resp = self.api({"a": "us", "user": email, "uh": user_hash})
        if isinstance(resp, int):
            raise MegaApiError(MEGA_ERROR_TEXT.get(resp, f"mega login rejected ({resp})"), code=resp)
        self._login_process(resp, password_aes)
        self.email, self.password = email, password
        return True

    def _login_process(self, resp, password_aes):
        self.master_key = _decrypt_key(_b64_to_a32(resp["k"]), password_aes)
        if "tsid" in resp:
            tsid = _b64url_decode(resp["tsid"])
            if _a32_to_str(_encrypt_key(_str_to_a32(tsid[:16]), self.master_key)) != tsid[-24:-8]:
                raise MegaApiError("mega login tsid mismatch", code=-9)
            self.sid = resp["tsid"]
        elif "csid" in resp:
            privk = _a32_to_str(_decrypt_key(_b64_to_a32(resp["privk"]), self.master_key))
            comps = [0, 0, 0, 0]
            for i in range(4):
                bitlen = (privk[0] * 256) + privk[1]
                bytelen = -(-bitlen // 8) + 2
                comps[i] = _mpi_to_int(privk[:bytelen])
                privk = privk[bytelen:]
            p, q, d = comps[0], comps[1], comps[2]
            if not p or not q or not d:
                raise MegaApiError("mega login: bad rsa private key", code=-9)
            csid_int = _mpi_to_int(_b64url_decode(resp["csid"]))
            m = pow(csid_int, d, p * q)
            raw = "%x" % m
            raw = binascii.unhexlify("0" + raw if len(raw) % 2 else raw)
            self.sid = _b64url_encode(raw[:43])
        else:
            raise MegaApiError("mega login: no session material in response", code=-9)
        if not self.sid:
            raise MegaApiError("mega login failed to derive a session id", code=-9)

    def logout(self):
        self.sid = None
        self.master_key = None
        self.email = ""
        self.password = ""

    # -- node tree --------------------------------------------------------------
    def fetch_nodes(self):
        data = self.api({"a": "f", "c": 1, "r": 1})
        self.nodes = {}
        self.path_to_h = {}
        for f in data.get("f", []):
            h, parent, t = f.get("h"), f.get("p"), f.get("t")
            entry = {"p": parent, "t": t, "name": "", "size": int(f.get("s") or 0), "key8": None}
            if t in (0, 1):
                try:
                    own = ""
                    for part in str(f.get("k") or "").split("/"):
                        if ":" in part:
                            uid, val = part.split(":", 1)
                            if uid == f.get("u"):
                                own = val
                                break
                            if not own:
                                own = val
                    if own:
                        key = _decrypt_key(_b64_to_a32(own), self.master_key)
                        entry["key8"] = key
                        k4 = tuple(key[i] ^ key[i + 4] for i in range(4)) if t == 0 else tuple(key[:4])
                        attr = _decrypt_attr(_b64url_decode(f.get("a") or ""), k4)
                        if attr and isinstance(attr.get("n"), str):
                            entry["name"] = attr["n"]
                except Exception:
                    pass
            self.nodes[h] = entry
            if t == 2:
                self.root_handle = h
        cache = {self.root_handle: "/"} if self.root_handle else {}

        def path_of(h):
            if h in cache:
                return cache[h]
            e = self.nodes.get(h)
            if not e:
                return None
            if e["t"] in (2, 3, 4):
                return None
            pp = path_of(e.get("p"))
            if pp is None or not e["name"]:
                return None
            full = pp.rstrip("/") + "/" + e["name"]
            cache[h] = full
            return full

        for h, e in self.nodes.items():
            if e["t"] in (0, 1) and e["name"]:
                pp = path_of(h)
                if pp:
                    self.path_to_h[pp] = h
        return len(self.nodes)

    # -- quota / folders ---------------------------------------------------------
    def free_bytes(self):
        d = self.api({"a": "uq", "xfer": 1, "strg": 1})
        if not isinstance(d, dict) or "mstrg" not in d:
            return -1
        try:
            total = int(d["mstrg"])
            used = int(d.get("cstrg") or 0)
            return max(0, total - used)
        except Exception:
            return -1

    def ensure_dirs(self, parts):
        if not parts:
            if self.root_handle is None:
                self.fetch_nodes()
            return self.root_handle
        cur = self.root_handle
        if cur is None:
            self.fetch_nodes()
            cur = self.root_handle
        prefix = ""
        for part in parts:
            prefix += "/" + part
            h = self.path_to_h.get(prefix)
            if h:
                cur = h
                continue
            k4 = [random.randint(0, 0xFFFFFFFF) for _ in range(4)]
            attrs = _b64url_encode(_encrypt_attr({"n": part}, k4))
            resp = self.api({"a": "p", "t": cur, "n": [{
                "h": "xxxxxxxx", "t": 1, "a": attrs,
                "k": _a32_to_b64(_encrypt_key(k4, self.master_key))}]})
            try:
                nh = resp["f"][0]["h"]
            except Exception:
                raise MegaApiError(f"mega folder create failed for {prefix}: {str(resp)[:100]}", code=-5)
            self.nodes[nh] = {"p": cur, "t": 1, "name": part, "size": 0, "key8": None}
            self.path_to_h[prefix] = nh
            cur = nh
        return cur

    # -- upload --------------------------------------------------------------
    def upload_stream(self, local_path, parent_h, name, size, progress_cb=None):
        size = int(size)
        if size <= 0:
            size = os.path.getsize(local_path)
        if size <= 0:
            raise MegaApiError("mega upload: empty file", code=-5)
        ul_url = self.api({"a": "u", "s": size})["p"]
        ul_key = [random.randint(0, 0xFFFFFFFF) for _ in range(6)]
        k4 = ul_key[:4]
        k4_str = _a32_to_str(k4)
        ctr_base = ((ul_key[4] << 32) + ul_key[5]) << 64
        iv_str = _a32_to_str([ul_key[4], ul_key[5], ul_key[4], ul_key[5]])
        mac_enc = AES.new(k4_str, AES.MODE_CBC, b"\0" * 16)

        mac_chunks = list(_mega_chunks(size))
        block_limit = UPLOAD_BLOCK_MB * 1024 * 1024
        blocks = []
        cur_off, cur_list, cur_len = None, [], 0
        for (cs, csize) in mac_chunks:
            if cur_list and cur_len + csize > block_limit:
                blocks.append((cur_off, cur_list))
                cur_off, cur_list, cur_len = None, [], 0
            if cur_off is None:
                cur_off = cs
            cur_list.append((cs, csize))
            cur_len += csize
        if cur_list:
            blocks.append((cur_off, cur_list))

        fd = os.open(str(local_path), os.O_RDONLY)
        mac_str = b"\0" * 16
        completion = ""
        pending = {}
        next_reduce = 0
        done = [0]
        errors = []
        post_timeout = (60, MEGA_PUT_TIMEOUT_S)
        try:

            def work(idx):
                off, chunks = blocks[idx]
                total_b = sum(c[1] for c in chunks)
                data = b""
                while len(data) < total_b:
                    part = os.pread(fd, total_b - len(data), off + len(data))
                    if not part:
                        raise MegaApiError("mega upload: short read on the local file", code=-5)
                    data += part
                cipher = AES.new(k4_str, AES.MODE_CTR,
                                 counter=Counter.new(128, initial_value=ctr_base + (off // 16)))
                enc = cipher.encrypt(data)
                ct_lasts = []
                for (cs, csize) in chunks:
                    chunk = data[cs - off: cs - off + csize]
                    cbc = AES.new(k4_str, AES.MODE_CBC, iv_str)
                    n = len(chunk)
                    cut = ((n - 16) // 16) * 16 if n > 16 else 0
                    if cut:
                        cbc.encrypt(chunk[:cut])
                    tail = chunk[cut:]
                    if len(tail) % 16:
                        tail += b"\0" * (16 - len(tail) % 16)
                    ct_lasts.append(cbc.encrypt(tail)[-16:])
                r = self.http.post(f"{ul_url}/{off}", data=enc, timeout=post_timeout)
                text = (r.text or "").strip()
                if r.status_code != 200 or (
                        text and not re.fullmatch(r"[0-9A-Za-z_-]+", text)):
                    raise MegaApiError(
                        f"mega upload endpoint rejected a block (HTTP {r.status_code}: {text[:60]!r})",
                        code=-5)
                return idx, ct_lasts, text, total_b

            with ThreadPoolExecutor(max_workers=UPLOAD_CONNECTIONS) as ex:
                futs = [ex.submit(work, i) for i in range(len(blocks))]
                last_finisher = {"text": ""}
                for fut in as_completed(futs):
                    try:
                        idx, ct_lasts, text_b, total_b = fut.result()
                    except Exception as e:
                        errors.append(e)
                        for f2 in futs:
                            f2.cancel()
                        break
                    last_finisher["text"] = text_b
                    pending[idx] = (ct_lasts, text_b, total_b)
                    while next_reduce in pending:
                        ct_lasts, _, total_b = pending.pop(next_reduce)
                        for ct in ct_lasts:
                            mac_str = mac_enc.encrypt(ct)
                        done[0] += total_b
                        next_reduce += 1
                        if progress_cb:
                            progress_cb(done[0], size)
            if errors:
                raise errors[0]
            completion = last_finisher["text"]
            if next_reduce != len(blocks) or not completion:
                raise MegaApiError("mega upload: no completion handle received "
                                   "(whichever request finished last must return it)", code=-5)
        finally:
            os.close(fd)

        file_mac = list(_str_to_a32(mac_str)) + [0, 0, 0, 0]
        meta_mac = (file_mac[0] ^ file_mac[1], file_mac[2] ^ file_mac[3])
        key8 = [ul_key[0] ^ ul_key[4], ul_key[1] ^ ul_key[5],
                ul_key[2] ^ meta_mac[0], ul_key[3] ^ meta_mac[1],
                ul_key[4], ul_key[5], meta_mac[0], meta_mac[1]]
        attrs = _b64url_encode(_encrypt_attr({"n": name}, k4))
        resp = self.api({"a": "p", "t": parent_h, "n": [{
            "h": completion, "t": 0, "a": attrs,
            "k": _a32_to_b64(_encrypt_key(key8, self.master_key))}]})
        try:
            node_h = resp["f"][0]["h"]
        except Exception:
            raise MegaApiError(f"mega node create failed: {str(resp)[:100]}", code=-5)
        return node_h, key8

    # -- export / delete ----------------------------------------------------------
    def export_file(self, node_h, key8):
        ph = self.api({"a": "l", "n": node_h})
        if isinstance(ph, int):
            raise MegaApiError(MEGA_ERROR_TEXT.get(ph, f"mega export error {ph}"), code=ph)
        return f"https://mega.nz/file/{ph}#{_a32_to_b64(list(key8))}"

    # -- public download (verification proof) ----------------------------------------
    def download_public(self, link, dest_dir, dest_name=None):
        m = MEGA_LINK_RE.search(link or "") or MEGA_LEGACY_LINK_RE.search(link or "")
        if not m:
            raise MegaApiError("not a mega.nz file link")
        u = m.group(0)
        if "/file/" in u:
            handle, key_s = u.split("/file/", 1)[1].split("#", 1)
        else:
            handle, key_s = u.split("/#!", 1)[1].split("!", 1)
        key8 = list(_b64_to_a32(key_s))
        d = self.api({"a": "g", "g": 1, "p": handle})
        if not isinstance(d, dict) or "g" not in d:
            raise MegaApiError("mega does not serve this link anymore")
        size = int(d["s"])
        k4 = tuple(key8[i] ^ key8[i + 4] for i in range(4))
        iv = (key8[4], key8[5])
        meta_mac = (key8[6], key8[7])
        attr = _decrypt_attr(_b64url_decode(d.get("at") or ""), k4) or {}
        name = sanitise(dest_name or attr.get("n") or handle)
        out = Path(dest_dir) / name
        k4_str = _a32_to_str(k4)
        ctr = AES.new(k4_str, AES.MODE_CTR,
                      counter=Counter.new(128, initial_value=((iv[0] << 32) + iv[1]) << 64))
        mac_enc = AES.new(k4_str, AES.MODE_CBC, b"\0" * 16)
        iv_str = _a32_to_str([iv[0], iv[1], iv[0], iv[1]])
        mac_str = b"\0" * 16
        md5 = hashlib.md5()
        written = 0
        with open(out, "wb") as fo, self.http.get(d["g"], stream=True,
                                                  timeout=(60, MEGA_GET_TIMEOUT_S)) as r:
            r.raise_for_status()
            it = r.iter_content(chunk_size=1024 * 1024)
            buf = b""
            for (cs, csize) in _mega_chunks(size):
                while len(buf) < csize:
                    try:
                        part = next(it)
                    except StopIteration:
                        break
                    if part:
                        buf += part
                data, buf = buf[:csize], buf[csize:]
                if len(data) != csize:
                    raise MegaApiError(f"mega url download truncated at {written + len(data)}/{size}")
                dec = ctr.decrypt(data)
                written += len(dec)
                md5.update(dec)
                fo.write(dec)
                cbc = AES.new(k4_str, AES.MODE_CBC, iv_str)
                n = len(dec)
                cut = ((n - 16) // 16) * 16 if n > 16 else 0
                if cut:
                    cbc.encrypt(dec[:cut])
                tail = dec[cut:]
                if len(tail) % 16:
                    tail += b"\0" * (16 - len(tail) % 16)
                mac_str = mac_enc.encrypt(cbc.encrypt(tail)[-16:])
        if written != size:
            raise MegaApiError(f"mega url download size mismatch: {written} != {size}")
        fm = list(_str_to_a32(mac_str)) + [0, 0, 0, 0]
        if (fm[0] ^ fm[1], fm[2] ^ fm[3]) != meta_mac:
            raise MegaApiError("mega url download failed the integrity (mac) check")
        return out, md5.hexdigest()


class MegaManager:
    """Own-account MEGA operations through the native MegaApi client.

    Same responsibilities as the previous MEGAcmd version (login, account
    pool, auto-generation on quota, folder tree, upload, export) but with
    zero external binaries, no socket server, and flat memory: uploads stream
    from disk in ~4MB blocks over a handful of keep-alive connections.
    """

    def __init__(self, store: FileStore):
        self.store = store
        self.api = MegaApi()
        self.logged_in = False
        self.email = None
        self.paths = set()
        self.lock = threading.RLock()
        self.gen_lock = threading.Lock()
        self.switch_count = 0
        self.default_email = MEGA_EMAIL

    @staticmethod
    def _run(args, timeout=None, cwd=None, env=None, spill=False):
        kw = {"capture_output": True, "text": True}
        if timeout:
            kw["timeout"] = timeout
        if cwd:
            kw["cwd"] = cwd
        if env:
            kw["env"] = env
        handles = []
        temps = []
        try:
            if spill:
                for tag in ("out", "err"):
                    f = tempfile.NamedTemporaryFile(prefix="mega_" + tag + "_", suffix=".log", delete=False)
                    f.close()
                    temps.append(f.name)
                kw.pop("capture_output")
                kw.pop("text")
                kw["stdout"] = open(temps[0], "wb")
                kw["stderr"] = open(temps[1], "wb")
                handles = [kw["stdout"], kw["stderr"]]
            r = subprocess.run(args, **kw)
        except subprocess.TimeoutExpired:
            r = subprocess.CompletedProcess(args, returncode=-99, stdout="", stderr="timeout")
        except FileNotFoundError:
            r = subprocess.CompletedProcess(args, returncode=-98, stdout="", stderr="required binary not installed")
        finally:
            for h in handles:
                try:
                    h.close()
                except Exception:
                    pass
        if temps:
            def _tail(p):
                try:
                    with open(p, "rb") as f:
                        f.seek(0, 2)
                        n = f.tell()
                        f.seek(max(0, n - 8192))
                        return f.read().decode("utf-8", "replace")
                except Exception:
                    return ""
            out, err = _tail(temps[0]), _tail(temps[1])
            for p in temps:
                try:
                    os.unlink(p)
                except Exception:
                    pass
            if isinstance(r, subprocess.CompletedProcess):
                return subprocess.CompletedProcess(getattr(r, "args", args), r.returncode, stdout=out, stderr=err)
        return r

    @staticmethod
    def _norm(path: str) -> str:
        return "/" + "/".join(p for p in path.strip().split("/") if p)

    def is_ready(self) -> bool:
        return self.logged_in

    def _saved_accounts(self):
        accs = []
        if MEGA_EMAIL:
            accs.append((MEGA_EMAIL, MEGA_PASSWORD or MEGA_EMAIL))
        data = self.store.load_account("mega_credentials.json") or {}
        for a in data.get("accounts") or []:
            if isinstance(a, dict) and a.get("email") and a.get("password") \
                    and a.get("status", "active") in ("active", ""):
                accs.append((a["email"], a["password"]))
        auth = self.store.load_account("mega_auth.json") or {}
        if auth.get("email"):
            accs.append((auth["email"], auth.get("password") or auth["email"]))
        seen, uniq = set(), []
        for e, p in accs:
            if e and e not in seen:
                seen.add(e)
                uniq.append((e, p))
        return uniq

    def ensure_login(self, reason: str = "MEGA login required") -> bool:
        with self.lock:
            if self.logged_in and self.api.sid:
                return True
            for email, pw in self._saved_accounts():
                if self._try_login(email, pw, "saved credentials"):
                    return True
            if MEGA_AUTOGEN:
                print("[mega] no working saved account — creating a brand-new MEGA account FIRST ...")
                for attempt in range(1, MEGA_GEN_ATTEMPTS + 1):
                    acc = self.generate_account()
                    if acc:
                        if self._try_login(acc[0], acc[1], "MEGA-Account-Generator"):
                            self.record_credential(acc[0], acc[1], "MEGA-Account-Generator")
                            return True
                        print(f"[mega-gen] VERIFICATION FAILED: {acc[0]} did not log in — account NOT counted")
                        self.mark_credential(acc[0], "unverified-login-failed")
                    print(f"[mega] account generation attempt {attempt}/{MEGA_GEN_ATTEMPTS} failed")
                    time.sleep(5 * attempt)
            return self.prompt_login(reason)

    def _try_login(self, email: str, password: str, source: str) -> bool:
        try:
            ok = self.api.login(email, password)
        except MegaApiError as e:
            print(f"[mega] login failed for {email}: {e}")
            if "MEGA-Account-Generator" in source:
                self.record_credential(email, password, source, status="unverified-login-failed")
            self.logged_in = False
            return False
        except Exception as e:
            print(f"[mega] login failed for {email}: {e}")
            self.logged_in = False
            return False
        if not ok:
            print(f"[mega] login rejected for {email}")
            if "MEGA-Account-Generator" in source:
                self.record_credential(email, password, source, status="unverified-login-failed")
            self.logged_in = False
            return False
        self.email = email
        self.logged_in = True
        try:
            self.refresh_files()
        except Exception as e:
            print(f"[mega] could not list the cloud drive after login: {e}")
        self.store.save_account("mega_auth.json", {"email": email, "password": password, "updated": iso_now()})
        self.record_credential(email, password, source)
        print(f"[mega] logged in as {email} ({source})")
        return True

    def prompt_login(self, reason: str) -> bool:
        while True:
            print(f"\n[mega] {reason}")
            hint = f" [default: {self.default_email}]" if self.default_email else ""
            email = input(f"MEGA email{hint} (Enter = default, q = stop): ").strip()
            if email.lower() in ("q", "quit", "exit"):
                return False
            if not email:
                email = self.default_email
            if not email:
                print("[mega] no email entered")
                continue
            password = email
            if self._try_login(email, password, "manual entry"):
                return True
            alt = input("Password differs from email? Enter MEGA password (Enter = use email): ").strip() or email
            if alt != password and self._try_login(email, alt, "manual entry"):
                return True
            if input("Retry? [Y/n]: ").strip().lower() in ("n", "q", "exit"):
                return False

    def load_credentials(self) -> list:
        data = self.store.load_account("mega_credentials.json") or {}
        accs = data.get("accounts")
        return accs if isinstance(accs, list) else []

    def save_credentials(self, accs):
        self.store.save_account("mega_credentials.json", {"updated": iso_now(), "accounts": accs})
        lines = [f"{'EMAIL':42}{'PASSWORD':26}STATUS", "-" * 80]
        for a in accs:
            lines.append(f"{a.get('email', ''):42}{a.get('password', ''):26}{a.get('status', '')}")
        self.store.save_raw("mega_credentials.txt", "\n".join(lines) + "\n")

    def record_credential(self, email: str, password: str, source: str, status: str = "active"):
        if not email:
            return
        with self.gen_lock:
            accs = self.load_credentials()
            entry = {"email": email, "password": password, "source": source,
                     "status": status, "created": iso_now()}
            for i, a in enumerate(accs):
                if a.get("email") == email:
                    accs[i] = {**a, **entry, "created": a.get("created") or entry["created"]}
                    break
            else:
                accs.append(entry)
            self.save_credentials(accs)
            print(f"[mega] credentials list: {len(accs)} account(s)")

    def mark_credential(self, email: str, status: str):
        if not email:
            return
        with self.gen_lock:
            accs = self.load_credentials()
            hit = False
            for a in accs:
                if a.get("email") == email:
                    a["status"] = status
                    hit = True
            if hit:
                self.save_credentials(accs)

    def generate_account(self):
        if not MEGA_AUTOGEN:
            return None
        with self.gen_lock:
            try:
                runner = ensure_mega_gen()
            except Exception as e:
                print(f"[mega-gen] auto-generation unavailable: {e}")
                return None
            pw = secrets.token_urlsafe(9)
            runs = MEGA_GEN_HOME / "runs"
            runs.mkdir(parents=True, exist_ok=True)
            workdir = Path(runs / ("run-" + time.strftime("%Y%m%d-%H%M%S") + "-" + secrets.token_hex(3)))
            workdir.mkdir(parents=True, exist_ok=True)
            env = os.environ.copy()
            env["PATH"] = str(Path(runner["megatools"]).parent) + os.pathsep + env.get("PATH", "")
            print("[mega-gen] creating a fresh MEGA account (Mail.tm temp inbox + auto verification, ~1-3 min) ...")
            r = self._run([runner["py"], runner["script"], "-n", "1", "-p", pw],
                          timeout=MEGA_GEN_TIMEOUT_S, cwd=str(workdir), env=env)
            out = ((r.stdout or "") + "\n" + (r.stderr or "")).strip()
            email, mega_pw = "", ""
            csv_path = workdir / "accounts.csv"
            if csv_path.exists():
                try:
                    rows = list(csv.reader(open(csv_path, newline="", encoding="utf-8")))
                    rows = [row for row in rows
                            if row and row[0].strip().lower() != "email" and len(row) >= 2 and "@" in row[0]]
                    if rows:
                        email = rows[-1][0].strip()
                        mega_pw = rows[-1][1].strip()
                except Exception as e:
                    print(f"[mega-gen] could not parse accounts.csv: {e}")
            if not email or not mega_pw:
                tail = out.strip().splitlines()[-3:] if out else []
                print(f"[mega-gen] generator did not produce an account (rc={r.returncode}): "
                      f"{' | '.join(tail) if tail else 'no output'}")
                print(f"[mega-gen] run directory kept for inspection: {workdir}")
                return None
            print(f"[mega-gen] generator reports account created: {email} — now verifying with a real login ...")
            shutil.rmtree(str(workdir), ignore_errors=True)
            return email, mega_pw

    def _switch_account(self, reason: str) -> bool:
        full = self.email
        if self.switch_count >= MEGA_MAX_SWITCHES:
            print(f"[mega] {self.switch_count} account switches this run — not creating more "
                  f"({reason}); rerun to continue with a fresh account")
            return False
        self.switch_count += 1
        if MEGA_AUTOGEN:
            for attempt in range(1, MEGA_GEN_ATTEMPTS + 1):
                acc = self.generate_account()
                if acc:
                    if self._try_login(acc[0], acc[1], "MEGA-Account-Generator"):
                        self.record_credential(acc[0], acc[1], "MEGA-Account-Generator")
                        self.mark_credential(full, "full")
                        return True
                    print(f"[mega-gen] VERIFICATION FAILED: {acc[0]} did not log in — account NOT counted")
                    self.mark_credential(acc[0], "unverified-login-failed")
                print(f"[mega] account generation attempt {attempt}/{MEGA_GEN_ATTEMPTS} failed")
                time.sleep(5 * attempt)
        self.mark_credential(full, "full")
        return self.prompt_login(reason)

    def refresh_files(self):
        self.api.fetch_nodes()
        self.paths = set(self.api.path_to_h)

    def path_exists(self, remote_path: str) -> bool:
        return self._norm(remote_path) in self.api.path_to_h

    def free_bytes(self) -> int:
        with self.lock:
            if not self.logged_in:
                return -1
            return self.api.free_bytes()

    def ensure_space(self, needed: int, depth: int = 0) -> bool:
        with self.lock:
            if depth > 4:
                return False
            if not self.logged_in and not self.ensure_login("MEGA login required"):
                return False
            free = self.free_bytes()
            if free < 0 or free >= needed + MEGA_MARGIN_BYTES:
                return True
            print(f"[mega] account {self.email} low on space (free {human(max(0, free))}, need "
                  f"{human(needed)}) — creating a new MEGA account")
            self.logged_in = False
            self.api.logout()
            if self._switch_account("storage full"):
                return self.ensure_space(needed, depth + 1)
            return False

    def ensure_folder(self, parts) -> str:
        return self.api.ensure_dirs([p for p in parts or [] if p])

    def rm(self, remote_path: str) -> bool:
        full = self._norm(remote_path)
        h = self.api.path_to_h.get(full)
        if not h:
            return False
        try:
            self.api.api({"a": "d", "n": h})
        except MegaApiError as e:
            print(f"  [mega] remote delete failed for {remote_path}: {e}")
            return False
        self.api.path_to_h.pop(full, None)
        self.api.nodes.pop(h, None)
        self.paths.discard(full)
        return True

    def _export_link(self, remote_path: str) -> str:
        full = self._norm(remote_path)
        h = self.api.path_to_h.get(full)
        if not h:
            raise RuntimeError(f"MEGA export failed for {remote_path}: path not found on the account")
        node = self.api.nodes.get(h) or {}
        key8 = node.get("key8")
        if not key8:
            raise RuntimeError(f"MEGA export failed for {remote_path}: node key unavailable")
        return self.api.export_file(h, key8)

    def _find_by_name(self, name: str):
        for p in sorted(self.paths):
            if p != "/" and p.rstrip("/").endswith("/" + name):
                return p
        return None

    def upload(self, local_path, mega_path: str, size_bytes: int) -> str:
        with self.lock:
            parts = [p for p in mega_path.strip("/").split("/") if p]
            name = parts[-1]
            last = None
            for attempt in range(1, MEGA_UPLOAD_RETRIES + 1):
                if not self.ensure_space(size_bytes):
                    raise UserAbort("no MEGA storage available — run the app again to add another account")
                shown = {"v": False}
                try:
                    parent_h = self.ensure_folder(parts[:-1])
                    full = "/" + "/".join(parts)
                    if self.path_exists(full):
                        print(f"  [mega] reusing existing file {name}")
                        return self._export_link(full)

                    def _progress(done, total):
                        shown["v"] = True
                        print(f"\r  [mega] {human(done)} / {human(total)} "
                              f"(x{UPLOAD_CONNECTIONS} connections)", end="")

                    size_hint = int(size_bytes or (os.path.getsize(local_path)
                                                   if os.path.exists(local_path) else 0))
                    node_h, key8 = self.api.upload_stream(local_path, parent_h, name, size_hint,
                                                          progress_cb=_progress)
                    if shown["v"]:
                        print()
                        shown["v"] = False
                    self.api.nodes[node_h] = {"p": parent_h, "t": 0, "name": name,
                                              "size": size_hint, "key8": key8}
                    self.api.path_to_h[full] = node_h
                    self.paths.add(full)
                    return self._export_link(full)
                except UserAbort:
                    raise
                except Exception as e:
                    if shown["v"]:
                        print()
                        shown["v"] = False
                    out = str(e).lower()
                    last = str(e)[:200]
                if any(k in out for k in QUOTA_KEYWORDS):
                    print(f"[mega] {self.email} is full ({last}) — creating a new MEGA account")
                    self.logged_in = False
                    self.api.logout()
                    self._switch_account("storage full")
                    continue
                if any(k in out for k in MEGA_SESSION_KEYWORDS):
                    print(f"[mega] {self.email} session is dead or blocked ({last}) — creating a "
                          f"new MEGA account")
                    self.logged_in = False
                    self.api.logout()
                    self._switch_account("mega session dead or blocked")
                    continue
                if attempt < MEGA_UPLOAD_RETRIES:
                    wait = 20 * attempt + random.uniform(0, 10)
                    print(f"  [mega] upload error ({last}) — retry {attempt}/{MEGA_UPLOAD_RETRIES} in {wait:.0f}s")
                    time.sleep(wait)
                    try:
                        self.refresh_files()
                    except Exception:
                        self.logged_in = False
                        self.api.logout()
                        if not self.ensure_login("MEGA session expired"):
                            raise UserAbort("no MEGA account available")
            raise RuntimeError(f"MEGA upload failed after {MEGA_UPLOAD_RETRIES} attempts: {last}")


def ensure_megatools() -> str:
    found = shutil.which("megatools")
    if found:
        return found
    bundled = MEGA_GEN_HOME / "bin" / "megatools"
    if bundled.exists():
        return str(bundled)
    print("[mega-gen] downloading static megatools binary ...")
    MEGA_GEN_HOME.mkdir(parents=True, exist_ok=True)
    tgz = MEGA_GEN_HOME / "megatools.tar.gz"
    r = subprocess.run(["curl", "-fsSL", "-o", str(tgz), MEGA_TOOLS_URL],
                       capture_output=True, text=True, timeout=600)
    if r.returncode != 0:
        r2 = requests.get(MEGA_TOOLS_URL, timeout=600)
        r2.raise_for_status()
        tgz.write_bytes(r2.content)
    shutil.unpack_archive(str(tgz), str(MEGA_GEN_HOME))
    tgz.unlink(missing_ok=True)
    hits = [p for p in MEGA_GEN_HOME.rglob("megatools") if p.is_file()]
    if not hits:
        raise RuntimeError("megatools binary not found inside the downloaded archive")
    binpath = MEGA_GEN_HOME / "bin"
    binpath.mkdir(exist_ok=True)
    shutil.move(str(hits[0]), str(bundled))
    os.chmod(str(bundled), 0o755)
    print(f"[mega-gen] megatools ready: {bundled}")
    return str(bundled)


def ensure_mega_gen() -> dict:
    repo = MEGA_GEN_HOME / "MEGA-Account-Generator"
    script = repo / "generate_accounts.py"
    if not script.exists():
        MEGA_GEN_HOME.mkdir(parents=True, exist_ok=True)
        print("[mega-gen] first-time setup: cloning f-o/MEGA-Account-Generator ...")
        g = subprocess.run(["git", "clone", "--depth", "1", MEGA_GEN_REPO, str(repo)],
                           capture_output=True, text=True, timeout=300)
        if not script.exists():
            raise RuntimeError(f"git clone failed: {((g.stderr or '') + (g.stdout or ''))[-200:]}")
    deps = ["pymailtm", "faker", "requests"]
    ok = subprocess.run([sys.executable, "-c",
                         "import pymailtm, faker, requests"], capture_output=True, text=True)
    if ok.returncode != 0:
        print("[mega-gen] installing Python deps (pymailtm, faker) ...")
        cmds = [[sys.executable, "-m", "pip", "install", "-q"] + deps,
                [sys.executable, "-m", "pip", "install", "-q", "--break-system-packages"] + deps,
                [sys.executable, "-m", "pip", "install", "-q", "--user", "--break-system-packages"] + deps]
        installed = False
        for c in cmds:
            r = subprocess.run(c, capture_output=True, text=True, timeout=900)
            if r.returncode == 0 and subprocess.run(
                    [sys.executable, "-c", "import pymailtm, faker, requests"],
                    capture_output=True, text=True).returncode == 0:
                installed = True
                break
        if not installed:
            raise RuntimeError("could not install pymailtm/faker (pip failed in every mode)")
    megatools = ensure_megatools()
    return {"py": sys.executable, "script": str(script), "megatools": megatools}


def probe_medcore(store: FileStore) -> AuthManager:
    if MEDCORE_EMAIL and MEDCORE_PASSWORD:
        email, password, source = MEDCORE_EMAIL, MEDCORE_PASSWORD, "env"
    else:
        saved = store.load_account("medcore_account.json") or {}
        if saved.get("email") and saved.get("password"):
            email, password, source = saved["email"], saved["password"], "saved"
        else:
            email, password = generate_credentials()
            source = "generated"
    auth = AuthManager(email, password, store=store)
    print(f"[medcore] using {source} account: {email}")
    if source == "generated":
        auth._register()
    for round_ in range(1, MEDCORE_ROTATE_ROUNDS + 2):
        try:
            auth.ensure_auth()
            break
        except MedCoreAuthError as e:
            if round_ > MEDCORE_ROTATE_ROUNDS:
                raise RuntimeError(f"MedCore login failed: {e}")
            wait = 20 * round_
            print(f"[medcore] {e} — waiting {wait}s and creating another account "
                  f"({round_}/{MEDCORE_ROTATE_ROUNDS})")
            time.sleep(wait)
    saved = store.load_account("medcore_account.json") or {}
    if saved.get("email") != auth.email:
        store.save_account("medcore_account.json", {"email": auth.email, "password": auth.password,
                                                    "updated": iso_now()})
    if auth.email != email:
        print(f"[medcore] active account is now {auth.email}")
    elif source == "generated":
        print(f"[medcore] temp account ready — password: {password}")
    return auth


def _videoish(v) -> bool:
    return isinstance(v, dict) and bool(v.get("url") or v.get("streamUrl"))


def _dex_obj(obj) -> bool:
    if isinstance(obj, list):
        return any(_videoish(v) for v in obj)
    if isinstance(obj, dict):
        if "library" in obj or "store" in obj:
            return True
        for v in obj.values():
            if isinstance(v, list) and any(_videoish(e) for e in v):
                return True
    return False


def extract_dex_json(text: str):
    s = (text or "").strip().lstrip("\ufeff")
    if s.startswith("```"):
        s = re.sub(r"^```[A-Za-z0-9_+-]*\s*", "", s)
    starts = []
    stripped = s.lstrip()
    if stripped[:1] in "{[":
        starts.append(len(s) - len(stripped))
    for pat in (r"\{", r"\["):
        for i, m in enumerate(re.finditer(pat, s)):
            if i >= JSON_EXTRACT_PROBES:
                break
            starts.append(m.start())
    dec = json.JSONDecoder()
    for pos in dict.fromkeys(starts):
        try:
            obj, _ = dec.raw_decode(s, pos)
        except Exception:
            continue
        if _dex_obj(obj):
            return obj
    return None


def parse_dex_json(data) -> list:
    items = []
    seen = set()

    def add(subject: str, folder: str, v: dict):
        if not isinstance(v, dict):
            return
        url = v.get("url") or v.get("streamUrl") or ""
        if not url:
            return
        vid = v.get("id")
        if vid is None:
            mm = UUID_RE.search(str(url))
            vid = mm.group(0).lower() if mm else "h" + hashlib.sha1(str(url).encode()).hexdigest()[:12]
        vid = str(vid)
        k = (folder, subject, vid)
        if k in seen:
            return
        seen.add(k)
        items.append({"id": vid,
                      "lectureName": v.get("lectureName") or v.get("title") or vid,
                      "sizeMb": v.get("sizeMb", 0), "url": url, "drm": v.get("drm", False),
                      "folder": sanitise(folder), "subject": sanitise(subject),
                      "section": _opt(v.get("section"))})

    if isinstance(data, dict):
        for subject, folders in (data.get("library") or {}).items():
            for folder in folders or []:
                if not folder:
                    continue
                prov = folder.get("folder") or subject
                for v in folder.get("videos", []):
                    add(subject, prov, v)
        for course, info in (data.get("store") or {}).items():
            if not info:
                continue
            for v in info.get("videos", []):
                add(course, course, v)
    entries = []
    if isinstance(data, list):
        entries = data
    elif isinstance(data, dict) and not items:
        for key in ("videos", "items", "data", "entries", "lectures"):
            v = data.get(key)
            if isinstance(v, list) and any(_videoish(e) for e in v):
                entries = v
                break
        if not entries:
            for v in data.values():
                if isinstance(v, list) and v:
                    hits = sum(1 for e in v if _videoish(e))
                    if hits and hits * 2 >= len(v) and (not entries or len(v) > len(entries)):
                        entries = v
    for e in entries:
        if not isinstance(e, dict):
            continue
        add(e.get("subject") or "Library", e.get("provider") or e.get("folder") or "MedCore", e)
    return items


def _opt(name: str) -> str:
    return sanitise(name) if name else ""


def parse_dex_md(text: str) -> list:
    items = []
    seen = set()
    lines = text.splitlines()
    h1s, h2s = set(), set()
    fence = False
    for raw in lines:
        line = raw.strip()
        if line.startswith("```"):
            fence = not fence
            continue
        if fence:
            continue
        m = HEADING_RE.match(raw.rstrip())
        if m:
            lvl = len(m.group(1))
            if lvl == 1:
                h1s.add(m.group(2).strip())
            elif lvl == 2:
                h2s.add(m.group(2).strip())
    title_mode = len(h1s) <= 1 and len(h2s) >= 2
    has_h2 = any(line.lstrip().startswith("##") for line in lines)
    h1 = h2 = h3 = h4 = ""
    in_fence = False
    for raw in lines:
        line = raw.strip()
        if line.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        m = HEADING_RE.match(raw.rstrip())
        if m:
            lvl, txt = len(m.group(1)), m.group(2).strip()
            txt = sanitise(re.sub(r"[`*_]", "", txt))
            if lvl == 1:
                h1, h2, h3, h4 = txt, "", "", ""
            elif lvl == 2:
                h2, h3, h4 = txt, "", ""
            elif lvl == 3:
                h3, h4 = txt, ""
            else:
                h4 = txt
            continue
        if not line:
            continue
        found = MD_LINK_RE.findall(line)
        if not found and "http" in line:
            found = [("", u) for u in BARE_URL_RE.findall(line)]
        for name, url in found:
            url = url.rstrip(".,;")
            if "mega.nz" in url:
                continue
            if title_mode:
                prov = h2 or "MedCore"
                subj = h3 or "Library"
                sect = h4
            elif has_h2:
                prov = h1 or "MedCore"
                subj = h2 or "Library"
                sect = h3
            else:
                prov = "MedCore"
                subj = h1 or "Library"
                sect = h3
            mm = UUID_RE.search(url)
            vid = mm.group(0).lower() if mm else "h" + hashlib.sha1(url.encode()).hexdigest()[:12]
            k = (prov, subj, vid)
            if k in seen:
                continue
            seen.add(k)
            items.append({"id": vid, "lectureName": sanitise(name) if name else vid,
                          "sizeMb": 0, "url": url, "drm": False,
                          "folder": sanitise(prov), "subject": sanitise(subj),
                          "section": _opt(sect)})
    return items


def _dex_from_url(url: str):
    for attempt in range(1, 4):
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            text = r.text
            outs = _parse_dex_text(text)
            if not outs:
                print(f"[dex] {url} fetched but held no usable video links")
                return None
            mode, items = max(outs, key=lambda o: (_dex_score(o[1]), o[0] == "dex.json"))
            print(f"[dex] fetched {mode} from url: {len(items)} video(s), "
                  f"{len(list_providers(items))} provider(s)")
            return items, text
        except Exception as e:
            print(f"[dex] fetch attempt {attempt}/3 failed: {e}")
            time.sleep(BACKOFF_BASE * attempt + random.uniform(0, JITTER_BASE))
    return None


def _parse_dex_text(text: str) -> list:
    outs = []
    obj = extract_dex_json(text)
    if obj is not None:
        try:
            ji = parse_dex_json(obj)
            if ji:
                outs.append(("dex.json", ji))
        except Exception as e:
            print(f"[dex] found a dex.json object but parsing it failed ({e}) — trying markdown")
    mi = parse_dex_md(text)
    if mi:
        outs.append(("markdown", mi))
    return outs


def _dex_score(items) -> tuple:
    return (len(list_providers(items)), len(items))


def load_dex(store: FileStore, dex_path=None):
    if dex_path:
        if str(dex_path).lower().startswith(("http://", "https://")):
            got = _dex_from_url(dex_path)
            if not got:
                print(f"[dex] --dex {dex_path} could not be fetched or parsed")
                sys.exit(2)
            return got[0], dex_path
        p = Path(dex_path)
        if not p.is_file():
            print(f"[dex] --dex {dex_path} is not a readable file")
            sys.exit(2)
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            print(f"[dex] could not read {p}: {e}")
            sys.exit(2)
        outs = _parse_dex_text(text)
        if not outs:
            print(f"[dex] {p.name} held no usable video links")
            sys.exit(2)
        mode, items = max(outs, key=lambda o: (_dex_score(o[1]), o[0] == "dex.json"))
        print(f"[dex] {p.name} parsed as {mode}: {len(items)} video(s), "
              f"{len(list_providers(items))} provider(s)")
        return items, p.name
    print(f"[dex] fetching dex from {DEX_URL} ...")
    got = _dex_from_url(DEX_URL)
    if got:
        items, text = got
        try:
            store.save_raw("dex.cache.json", text)
            print("[dex] cached to dex.cache.json (offline fallback)")
        except Exception as e:
            print(f"[dex] cache write failed: {e}")
        return items, "DEX_URL"
    print(f"[dex] fetch failed — scanning local dex files in {store.root} ...")
    best = None
    names = [n for n in ("dex.md", "DEX.md", "Dex.md", "dex.MD", "dex.json", "DEX.json", "Dex.json")
             if (store.root / n).exists()]
    try:
        for p in sorted(store.root.iterdir()):
            ln = p.name.lower()
            if (p.is_file() and ln.startswith("dex") and ln.endswith((".json", ".md"))
                    and p.name not in names):
                names.append(p.name)
    except Exception:
        pass
    for name in names:
        p = store.root / name
        if not p.exists():
            continue
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            print(f"[dex] could not read {name}: {e}")
            continue
        outs = _parse_dex_text(text)
        if not outs:
            print(f"[dex] {name}: no usable video links — checking other dex files")
            continue
        mode, items = max(outs, key=lambda o: (_dex_score(o[1]), o[0] == "dex.json"))
        print(f"[dex] {name} parsed as {mode}: {len(items)} video(s), "
              f"{len(list_providers(items))} provider(s)")
        sc = _dex_score(items)
        if best is None or sc > best[0]:
            best = (sc, items, name, mode, text)
    if best is not None:
        _, items, name, mode, text = best
        if mode == "markdown" and len(items) > 200 and len(list_providers(items)) <= 2:
            if DEX_JSON_HINT_RE.search(text):
                print(f"[dex] WARNING: {name} looks like a damaged or truncated dex.json dump — every video "
                      f"collapsed under 1 provider. Save the COMPLETE dex.json into {store.root} and rerun "
                      f"(it wins automatically) or point --dex straight at it")
            else:
                print(f"[dex] note: {len(items)} videos but only {len(list_providers(items))} provider(s) — "
                      f"if that is wrong, drop the real dex.json into {store.root} (it wins automatically) "
                      f"or run with --dex /path/to/dex.json; markdown providers come from '# Provider' "
                      f"headings or '## Provider' under a single title")
        return items, name
    raise RuntimeError(f"no dex source available — {DEX_URL} unreachable and no usable dex file in {store.root}")


def parse_args(argv=None):
    ap = argparse.ArgumentParser(
        prog="cordchain.py",
        description="MedCore -> MEGA.nz cordchain app (bare run = all providers -> cordchain.json)")
    ap.add_argument("--provider", action="append", nargs="+", default=[], metavar="NAME",
                    help="transfer only these provider(s) this run — case-insensitive, "
                         "a unique part of the name is enough, commas work; default = all providers")
    ap.add_argument("providers", nargs="*", metavar="PROVIDER",
                    help="same as --provider: bare provider names count as the filter too")
    ap.add_argument("--dex", metavar="PATH_OR_URL",
                    help="use this dex (local file or http(s) url) instead of fetching DEX_URL")
    ap.add_argument("--list", action="store_true",
                    help="list every provider with its video count and exit")
    args = ap.parse_args(argv)
    wanted = list(args.providers)
    for group in args.provider or []:
        for token in group:
            for piece in str(token).split(","):
                piece = piece.strip()
                if piece:
                    wanted.append(piece)
    args.provider = wanted
    return args


def list_providers(items) -> dict:
    counts = {}
    for it in items:
        p = it.get("folder") or "MedCore"
        counts[p] = counts.get(p, 0) + 1
    return counts


def select_providers(items, wanted):
    counts = list_providers(items)
    wanted = [w for w in (str(w).strip() for w in wanted or []) if w]
    if not wanted:
        return None, counts
    chosen = []
    for w in wanted:
        wl = w.lower()
        pool = [p for p in counts if p.lower() == wl] or [p for p in counts if wl in p.lower()]
        if len(pool) == 1:
            if pool[0] not in chosen:
                chosen.append(pool[0])
            print(f"[args] provider '{w}' -> {pool[0]} ({counts[pool[0]]} video(s))")
        elif len(pool) > 1:
            print(f"[args] provider '{w}' is ambiguous — matches: {', '.join(sorted(pool))}")
            print("[args] rerun with --list to see the exact provider names")
            sys.exit(2)
        else:
            print(f"[args] provider '{w}' not found")
            print("[args] available providers:")
            for p in sorted(counts):
                print(f"    {p} — {counts[p]} video(s)")
            sys.exit(2)
    if not chosen:
        return None, counts
    return set(chosen), counts


class Pipeline:
    def __init__(self, store: FileStore, items: list, download_dir: Path):
        self.store = store
        self.items = items
        self.auth = None
        self.mega = None
        self.rate = RateLimiter()
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.chain = []
        self.entry_by_key = {}
        self.item_by_key = {}
        self.used_paths = set()
        self.chain_lock = threading.RLock()
        self.stop_event = threading.Event()
        self.ram = RamGuard()

    def attach(self, auth: AuthManager, mega: MegaManager):
        self.auth = auth
        self.mega = mega

    @staticmethod
    def _body_message(r) -> str:
        try:
            j = r.json()
            if isinstance(j, dict):
                return str(j.get("message") or j.get("error") or "")[:160]
        except Exception:
            pass
        try:
            return (r.text or "")[:160].replace("\n", " ")
        except Exception:
            return ""

    def build_chain(self):
        saved = self.store.load_chain() or {}
        prev = {}
        for ce in saved.get("videos") or saved.get("chain") or []:
            if isinstance(ce, dict) and ce.get("key") and is_mega_link(ce.get("mega_url")):
                prev[ce["key"]] = ce
        groups = {}
        for it in self.items:
            groups.setdefault((it["folder"], it["subject"]), []).append(it)
        num = {}
        for k, rows in groups.items():
            for i, r in enumerate(rows, 1):
                num[k + (str(r["id"]),)] = i
        self.chain = []
        self.entry_by_key = {}
        self.item_by_key = {}
        self.used_paths = set()
        for seq, it in enumerate(self.items, 1):
            key = f"{it['folder']}||{it['subject']}||{it['id']}"
            e = {"seq": seq, "key": key, "provider": it["folder"], "subject": it["subject"],
                 "section": it["section"], "id": it["id"], "name": it["lectureName"],
                 "sizeMb": it.get("sizeMb", 0),
                 "num": num.get((it["folder"], it["subject"], str(it["id"])), 0),
                 "status": "pending", "mega_url": "", "mega_path": "", "mega_account": "",
                 "sourceUrl": it["url"], "uploaded": "", "verified": "", "verify": ""}
            p = prev.get(key)
            if p:
                e.update(status="chained", mega_url=p.get("mega_url", ""),
                         mega_path=p.get("mega_path", ""), mega_account=p.get("mega_account", ""),
                         uploaded=p.get("uploaded", ""), verified=p.get("verified", ""),
                         verify=p.get("verify", ""))
            if e["status"] == "chained":
                self.used_paths.add(e["mega_path"])
            else:
                base = sanitise(it.get("lectureName") or str(it["id"])) + ".mp4"
                clean = base
                if MEGA_NUMBERING and e["num"]:
                    w = max(2, len(str(e["num"])))
                    clean = f"{e['num']:0{w}d} - {base}"
                mega_path = "/".join(self._mega_parts(it) + [clean])
                if mega_path in self.used_paths:
                    mega_path = "/".join(self._mega_parts(it) + [f"{clean[:-4]} [{it['id']}].mp4"])
                self.used_paths.add(mega_path)
                e["mega_path"] = mega_path
            self.entry_by_key[key] = e
            self.item_by_key[key] = it
            self.chain.append(e)
        self.save_chain()
        chained = sum(1 for e in self.chain if e["status"] == "chained")
        print(f"[cordchain] chain built in sequence: {len(self.chain)} item(s) — "
              f"{chained} already uploaded + verified (recorded in cordchain.json), "
              f"{len(self.chain) - chained} to transfer")

    def save_chain(self):
        with self.chain_lock:
            vids = [e for e in self.chain if e["status"] == "chained" and e["mega_url"]]
            vids.sort(key=lambda e: e["seq"])
            out = []
            for e in vids:
                out.append({"seq": e["seq"], "key": e["key"], "provider": e["provider"],
                            "subject": e["subject"], "section": e["section"], "id": e["id"],
                            "name": e["name"], "sizeMb": e.get("sizeMb", 0), "num": e.get("num", 0),
                            "mega_url": e["mega_url"], "mega_path": e["mega_path"],
                            "mega_account": e["mega_account"], "uploaded": e["uploaded"],
                            "verified": e["verified"], "verify": e.get("verify", ""),
                            "sourceUrl": e.get("sourceUrl", "")})
            meta = {"kind": "cordchain", "updated": iso_now(), "videos": len(out), "source": "dex"}
            if self.store.save_chain({"_meta": meta, "videos": out}):
                if len(out) != getattr(self, "_saved_count", -1):
                    print(f"[cordchain] cordchain.json saved — {len(out)} uploaded video(s) recorded")
                self._saved_count = len(out)

    def _mark_failed(self, key: str, error: str):
        with self.chain_lock:
            e = self.entry_by_key.get(key)
            if e:
                e.update(status="failed", error=error[:200])

    def _mega_parts(self, row: dict) -> list:
        parts = [row["folder"], row["subject"]]
        if row.get("section"):
            parts.append(row["section"])
        return parts

    def _remote_name(self, row: dict, entry: dict) -> str:
        base = sanitise(row.get("lectureName") or str(row["id"])) + ".mp4"
        clean = base
        if MEGA_NUMBERING and entry.get("num"):
            w = max(2, len(str(entry.get("num"))))
            clean = f"{entry['num']:0{w}d} - {base}"
        return clean

    def _size_ok(self, local: Path, row: dict) -> bool:
        got = local.stat().st_size
        expected = float(row.get("sizeMb") or 0) * 1e6
        if not expected:
            return got > 0
        return abs(got - expected) <= expected * 0.02 + 5 * 1024 * 1024

    @staticmethod
    def _range_total(headers: dict) -> float:
        cr = RateLimiter._hget(headers, "content-range") or ""
        m = re.search(r"bytes\s+\d+-\d+/(\d+)", str(cr), re.I)
        return float(m.group(1)) if m else 0.0

    def _stream_video(self, row: dict, local: Path, key: str, expected_bytes: float = 0.0, label: str = "") -> bool:
        attempts = 0
        rejects = 0
        auth_fails = 0
        progress_shown = False
        no_multi = False
        url = row["url"]
        tag = f"  [dl:{label[:26]}]" if label else "  [dl]"
        while attempts < MAX_RETRIES:
            if self.stop_event.is_set():
                raise UserAbort("run stopped — rerun to resume")
            try:
                self.auth.ensure_auth()
                auth_fails = 0
            except UserAbort:
                raise
            except MedCoreAuthError as e:
                auth_fails += 1
                if auth_fails > AUTH_RECOVER_ATTEMPTS:
                    raise UserAbort(f"MedCore auth unavailable after {AUTH_RECOVER_ATTEMPTS} "
                                    f"recovery rounds: {e}")
                print(f"  [auth] {e} — waiting, then creating another account "
                      f"({auth_fails}/{AUTH_RECOVER_ATTEMPTS})")
                time.sleep(min(45, 10 * auth_fails))
                continue
            except RuntimeError as e:
                print(f"  [dl] auth error: {e}")
                attempts += 1
                time.sleep(BACKOFF_BASE * attempts + random.uniform(0, JITTER_BASE))
                continue
            if self.auth.trial_expiry and time.time() >= self.auth.trial_expiry - 90:
                print("  [auth] trial window ending — rotating to a fresh account "
                      "before streams go dark (expired trials 404 everything)")
                if not self.auth.coordinated_rotation("trial window ending"):
                    print("  [auth] rotation failed — continuing on the current account")
            self.rate.maybe_sleep()
            pos = local.stat().st_size if local.exists() else 0
            headers = {"Authorization": f"Bearer {self.auth.token}", "Range": f"bytes={pos}-"}
            big = False
            cr_total = 0.0
            total = 0.0
            try:
                with requests.get(url, headers=headers, stream=True, timeout=(30, 90)) as r:
                    self.rate.note(dict(r.headers))
                    code = r.status_code
                    if code == 404:
                        if progress_shown:
                            print()
                            progress_shown = False
                        msg = self._body_message(r) or "Video not found"
                        if self.auth.trial_expiry and time.time() >= self.auth.trial_expiry - 5:
                            print("  [dl] 404 but this account's trial has expired — expired trials "
                                  "404 every video, so rotating to a fresh account instead of parking")
                            attempts += 1
                            if not self.auth.coordinated_rotation("trial expired mid-run (404s)"):
                                time.sleep(min(45, 10 * attempts))
                            continue
                        self._mark_failed(key, f'server returned 404 for the dex url ("{msg}")')
                        print("  [dl] 404 on this url — parked as failed, run continues")
                        return False
                    if code in (401, 403):
                        if progress_shown:
                            print()
                            progress_shown = False
                        msg = self._body_message(r)
                        print(f"  [auth] stream rejected ({code}: {msg or 'no message'})")
                        rejects += 1
                        attempts += 1
                        if rejects == 1:
                            try:
                                self.auth.handle_reject(code)
                            except MedCoreAuthError:
                                if not self.auth.coordinated_rotation("session dead after re-login"):
                                    print("  [auth] could not create a fresh account yet — backing off")
                                    time.sleep(min(45, 10 * rejects))
                        elif rejects == 2:
                            if not self.auth.coordinated_rotation(f"stream {code} rejected twice"):
                                print("  [auth] could not create a fresh account yet — backing off")
                                time.sleep(min(45, 10 * rejects))
                        else:
                            self._mark_failed(key, f"server refuses access even with a fresh account "
                                                   f"(HTTP {code}: {msg or 'no message'})")
                            print("  [auth] fresh account also rejected — parking this video as failed")
                            return False
                        continue
                    if code == 429:
                        self.rate.check_429(dict(r.headers))
                        continue
                    if code == 416 and pos > 0:
                        if progress_shown:
                            print()
                        return True
                    r.raise_for_status()
                    h = dict(r.headers)
                    cr_total = self._range_total(h)
                    cl = RateLimiter._hget(h, "content-length")
                    try:
                        clen = float(cl) if cl else 0.0
                    except (TypeError, ValueError):
                        clen = 0.0
                    if code == 206 and cr_total:
                        total = cr_total
                    elif clen:
                        total = pos + clen if code == 206 else clen
                    else:
                        total = expected_bytes
                    big = (code == 206 and cr_total and cr_total >= MULTI_MIN_BYTES
                           and DL_CONNECTIONS > 1 and hasattr(os, "pwrite") and not no_multi
                           and not self.stop_event.is_set())
                    if not big:
                        mode = "ab" if (pos > 0 and code == 206) else "wb"
                        self.ram.track(local)
                        try:
                            with open(local, mode) as f:
                                fd = f.fileno()
                                evict_from = pos
                                wrote = 0
                                for chunk in r.iter_content(chunk_size=DL_CHUNK_SIZE):
                                    self.ram.wait()
                                    if chunk:
                                        f.write(chunk)
                                        wrote += len(chunk)
                                        if wrote >= RAM_EVICT_EVERY:
                                            cur = f.tell()
                                            self.ram.note_written(fd, evict_from, cur)
                                            evict_from = cur
                                            wrote = 0
                        finally:
                            self.ram.untrack(local)
                        got = local.stat().st_size
                        if code == 206 and total and got < total:
                            if got > pos:
                                attempts = 0
                                rejects = 0
                                progress_shown = True
                                print(f"\r{tag} {human(got)} / {human(total)}", end="")
                                continue
                            attempts += 1
                            print(f"\n{tag} no progress at {human(got)}/{human(total)} — retrying")
                            continue
                        print(f"\n{tag} {human(got)} / {human(total or got)} complete")
                        return True
            except UserAbort:
                raise
            except Exception as e:
                if progress_shown:
                    print()
                    progress_shown = False
                print(f"  [dl] error: {e}")
                attempts += 1
                time.sleep(BACKOFF_BASE * attempts + random.uniform(0, JITTER_BASE))
                continue
            if big:
                try:
                    ok = self._h2_multistream(url, local, int(cr_total), tag)
                except NoRangeError as e:
                    print(f"\n  [dl] {e} — single connection fallback")
                    no_multi = True
                    continue
                except Http1Fallback:
                    ok = False
                except UserAbort:
                    raise
                except Exception as e:
                    # stalled segment(s) etc. — the multistream path already ran
                    # recovery rounds; retry the whole download from scratch
                    # instead of parking the video
                    print(f"\n  [dl] h2 download failed ({errsig(e)}) — retrying from scratch")
                    ok = None
                if ok is False:
                    try:
                        ok = self._multistream(url, local, int(cr_total), tag)
                    except NoRangeError as e:
                        print(f"\n  [dl] {e} — single connection fallback")
                        no_multi = True
                        continue
                    except UserAbort:
                        raise
                    except Exception as e:
                        print(f"\n  [dl] parallel download failed ({errsig(e)}) — retrying from scratch")
                        ok = None
                if ok is True:
                    return True
                attempts += 1
                print(f"\n  [dl] download attempt failed — retrying ({attempts}/{MAX_RETRIES})")
                time.sleep(BACKOFF_BASE * attempts + random.uniform(0, JITTER_BASE))
        self._mark_failed(key, "download retries exhausted")
        drop_file_cache(local)
        return False

    def _h2_multistream(self, url: str, local: Path, total: int, tag: str) -> bool:
        """Download with N streams multiplexed over ONE HTTP/2 connection.

        MedCore's edge throttles by TCP connection count (2+ connections from one
        IP collapse to ~1 MB/s or hang outright) but happily serves ~3-6 MB/s per
        h2 stream on a single connection — 8 streams measured ~22 MB/s.

        Hardened after a real Colab stall (a 639 MB video lost a segment at
        exactly 20 MiB when the edge started hanging range requests mid-file):
        - each segment owns its byte position; the patience counter only counts
          CONSECUTIVE zero-progress attempts and resets the moment bytes move,
          so slow drip-feeding and many small capped responses never kill it
        - swallowed errors are printed, never silent
        - a stalled segment is retried in recovery ROUNDS on a fresh connection
          while every byte already written is kept
        Returns False when HTTP/2 is unavailable so the caller can use the
        legacy path."""
        httpx = _ensure_httpx()
        if httpx is None:
            return False
        conns = max(2, min(DL_H2_STREAMS, total // (2 * 1024 * 1024)))
        if conns < 2 or not hasattr(os, "pwrite"):
            return False
        tmp = Path(str(local) + ".m2")
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        fd = os.open(str(tmp), os.O_CREAT | os.O_RDWR, 0o644)
        replaced = False
        done = [0]
        dlock = threading.Lock()
        last_print = [time.time()]
        self.ram.track(tmp)
        bounds = segment_bounds(total, conns)
        pos = {i: bounds[i][0] for i in range(len(bounds))}
        client = None
        try:
            os.ftruncate(fd, total)

            def seg_work(i: int, a: int, b: int):
                fails = 0                # consecutive zero-progress attempts
                evict_from = pos[i]
                wrote = 0
                while pos[i] <= b:
                    if self.stop_event.is_set():
                        raise UserAbort("run stopped — rerun to resume")
                    self.ram.wait()
                    if fails >= DL_STALL_RETRIES:
                        raise RuntimeError(
                            f"segment {a}-{b} made no progress for {fails} consecutive "
                            f"attempts (stuck at byte {pos[i]}/{b + 1})")
                    try:
                        hdr = {"Authorization": f"Bearer {self.auth.ensure_auth()}",
                               "Range": f"bytes={pos[i]}-{b}"}
                        with client.stream("GET", url, headers=hdr) as r:
                            if r.http_version != "HTTP/2":
                                raise Http1Fallback("server negotiated HTTP/1.1 — h2 multiplexing unavailable")
                            self.rate.note(dict(r.headers))
                            if r.status_code == 429:
                                self.rate.check_429(dict(r.headers))
                                fails += 1
                                continue
                            if r.status_code != 206:
                                raise NoRangeError(f"server answered HTTP {r.status_code} for a byte-range request")
                            for chunk in r.iter_bytes(DL_CHUNK_SIZE):
                                if self.stop_event.is_set():
                                    raise UserAbort("run stopped — rerun to resume")
                                self.ram.wait()
                                if not chunk:
                                    continue
                                n = len(chunk)
                                if pos[i] + n > b + 1:
                                    chunk = chunk[:b + 1 - pos[i]]
                                    n = len(chunk)
                                if n <= 0:
                                    break
                                os.pwrite(fd, chunk, pos[i])
                                pos[i] += n
                                wrote += n
                                fails = 0          # bytes moved — patience resets
                                if wrote >= RAM_EVICT_EVERY:
                                    self.ram.note_written(fd, evict_from, pos[i])
                                    evict_from = pos[i]
                                    wrote = 0
                                with dlock:
                                    done[0] += n
                                    now = time.time()
                                    if now - last_print[0] >= 3:
                                        last_print[0] = now
                                        print(f"\r{tag} {human(done[0])} / {human(total)} "
                                              f"(x{conns} h2 streams, 1 connection)", end="")
                            if pos[i] > b:
                                return
                            # Response ended cleanly but the segment is unfinished —
                            # the edge sometimes caps a single range response. The
                            # position is durable, so politely re-request the rest.
                            time.sleep(0.5)
                    except UserAbort:
                        raise
                    except (NoRangeError, Http1Fallback):
                        raise
                    except MedCoreAuthError as e:
                        fails += 1
                        print(f"\n  [dl] seg {i}: auth unavailable ({errsig(e)}) — waiting, "
                              f"another account will be created by the auth layer")
                        time.sleep(min(45, 10 * fails))
                    except Exception as e:
                        fails += 1
                        print(f"\n  [dl] seg {i}: {errsig(e)} — retrying "
                              f"({fails}/{DL_STALL_RETRIES}, kept {human(pos[i] - a)}/{human(b - a + 1)})")
                        time.sleep(BACKOFF_BASE * min(fails, 5) + random.uniform(0, JITTER_BASE))

            stalled_rounds = 0
            round_no = 0
            last_err = None
            while True:
                pending = [i for i in range(len(bounds)) if pos[i] <= bounds[i][1]]
                if not pending:
                    break
                before = sum(pos.values())
                round_no += 1
                client = httpx.Client(http2=True, timeout=httpx.Timeout(30, read=60))
                try:
                    with ThreadPoolExecutor(max_workers=len(pending)) as ex:
                        futs = [ex.submit(seg_work, i, bounds[i][0], bounds[i][1]) for i in pending]
                        first = None
                        for f in as_completed(futs):
                            try:
                                f.result()
                            except (UserAbort, NoRangeError):
                                raise
                            except Http1Fallback:
                                print("\n  [dl] HTTP/2 not negotiated by the server — trying legacy mode")
                                raise
                            except Exception as e:
                                first = first or e
                    if first is not None:
                        last_err = first
                finally:
                    try:
                        client.close()
                    except Exception:
                        pass
                    client = None
                made = sum(pos.values()) - before
                pending_after = [i for i in range(len(bounds)) if pos[i] <= bounds[i][1]]
                if made > 0:
                    if round_no > 1 and pending_after:
                        print(f"\n  [dl] h2 round {round_no}: +{human(made)} recovered — "
                              f"resuming {len(pending_after)} unfinished segment(s)")
                    stalled_rounds = 0
                else:
                    stalled_rounds += 1
                    if stalled_rounds >= DL_H2_ROUNDS:
                        remaining = sum(bounds[i][1] - pos[i] + 1 for i in pending_after)
                        raise RuntimeError(
                            f"h2 download stalled: {human(remaining)} of {human(total)} undownloaded "
                            f"after {round_no} round(s) with no progress; last segment error: "
                            f"{errsig(last_err) if last_err else 'server stopped serving ranges'}")
                    print(f"\n  [dl] h2 round {round_no} made no progress "
                          f"({stalled_rounds}/{DL_H2_ROUNDS}) — fresh connection, retrying remaining bytes")
            got = os.fstat(fd).st_size
            if got != total:
                raise RuntimeError(f"h2 download size mismatch: {got} != {total}")
            print(f"\r{tag} {human(total)} / {human(total)} complete (x{conns} h2 streams, 1 connection)   ")
            os.replace(str(tmp), str(local))
            replaced = True
            return True
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass
            self.ram.untrack(tmp)
            try:
                os.close(fd)
            except Exception:
                pass
            if not replaced:
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass

    def _multistream(self, url: str, local: Path, total: int, tag: str) -> bool:
        conns = max(1, min(DL_CONNECTIONS, total // (4 * 1024 * 1024)))
        if conns < 2 or not hasattr(os, "pwrite"):
            return False
        tmp = Path(str(local) + ".m2")
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        fd = os.open(str(tmp), os.O_CREAT | os.O_RDWR, 0o644)
        replaced = False
        done = [0]
        dlock = threading.Lock()
        last_print = [time.time()]
        self.ram.track(tmp)
        try:
            os.ftruncate(fd, total)

            def seg_work(a: int, b: int):
                pos_a = a
                seg_tries = 0
                evict_from = a
                wrote = 0
                while pos_a <= b:
                    if self.stop_event.is_set():
                        raise UserAbort("run stopped — rerun to resume")
                    self.ram.wait()
                    if seg_tries >= 4:
                        raise RuntimeError(f"segment {a}-{b} stalled at {pos_a - a}/{b - a + 1} bytes after retries")
                    try:
                        hdr = {"Authorization": f"Bearer {self.auth.ensure_auth()}",
                               "Range": f"bytes={pos_a}-{b}"}
                        with requests.get(url, headers=hdr, stream=True, timeout=(30, 90)) as r:
                            self.rate.note(dict(r.headers))
                            if r.status_code == 429:
                                self.rate.check_429(dict(r.headers))
                                seg_tries += 1
                                continue
                            if r.status_code != 206:
                                raise NoRangeError(f"server answered HTTP {r.status_code} for a byte-range request")
                            for chunk in r.iter_content(chunk_size=DL_CHUNK_SIZE):
                                if self.stop_event.is_set():
                                    raise UserAbort("run stopped — rerun to resume")
                                self.ram.wait()
                                if not chunk:
                                    continue
                                n = len(chunk)
                                if pos_a + n > b + 1:
                                    chunk = chunk[:b + 1 - pos_a]
                                    n = len(chunk)
                                if n <= 0:
                                    break
                                os.pwrite(fd, chunk, pos_a)
                                pos_a += n
                                wrote += n
                                seg_tries = 0          # bytes moved — patience resets
                                if wrote >= RAM_EVICT_EVERY:
                                    self.ram.note_written(fd, evict_from, pos_a)
                                    evict_from = pos_a
                                    wrote = 0
                                with dlock:
                                    done[0] += n
                                    now = time.time()
                                    if now - last_print[0] >= 3:
                                        last_print[0] = now
                                        print(f"\r{tag} {human(done[0])} / {human(total)} (x{conns} connections)", end="")
                        if pos_a > b:
                            return
                        # response ended cleanly with the segment unfinished —
                        # the edge capped this response; re-request the remainder
                        time.sleep(0.5)
                    except UserAbort:
                        raise
                    except NoRangeError:
                        raise
                    except MedCoreAuthError as e:
                        seg_tries += 1
                        print(f"  [dl] auth unavailable mid-segment ({e}) — waiting, another "
                              f"account will be created by the auth layer")
                        time.sleep(min(45, 10 * seg_tries))
                    except Exception as e:
                        seg_tries += 1
                        print(f"\n  [dl] seg retry {seg_tries}/4 after error: {errsig(e)}")
                        time.sleep(BACKOFF_BASE * seg_tries + random.uniform(0, JITTER_BASE))

            with ThreadPoolExecutor(max_workers=conns) as ex:
                futs = [ex.submit(seg_work, a, b) for a, b in segment_bounds(total, conns)]
                first = None
                for f in as_completed(futs):
                    try:
                        f.result()
                    except (UserAbort, NoRangeError):
                        raise
                    except Exception as e:
                        first = first or e
                if first is not None:
                    raise first
            got = os.fstat(fd).st_size
            if got != total:
                raise RuntimeError(f"multi-connection download size mismatch: {got} != {total}")
            print(f"\r{tag} {human(total)} / {human(total)} complete (x{conns} connections)   ")
            os.replace(str(tmp), str(local))
            replaced = True
            return True
        finally:
            self.ram.untrack(tmp)
            try:
                os.close(fd)
            except Exception:
                pass
            if not replaced:
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass

    def verify_url(self, mega: MegaManager, link: str, local_path: Path):
        if not is_mega_link(link):
            return False, "exported url is not a valid mega.nz link format"
        want = local_path.stat().st_size
        if not VERIFY_FULL:
            state, val = mega_public_size_probe(link)
            if state == "ok":
                if int(val) == want:
                    return True, f"OK (fast check — mega.nz api reports the exact size {human(val)})"
                return False, f"fast check size mismatch: mega.nz api reports {int(val)} bytes, uploaded {want} bytes"
            if state == "dead":
                return False, f"mega.nz api does not resolve this link ({val})"
            print(f"  [proof] fast check unavailable ({val}) — falling back to a full download proof")
        td = Path(tempfile.mkdtemp(prefix="vfy_", dir=str(self.download_dir)))
        try:
            try:
                got_path, got_md5 = mega.api.download_public(link, str(td))
            except Exception as e:
                return False, f"url download failed: {str(e)[:150]}"
            got = got_path.stat().st_size
            if got != want:
                return False, f"url proof size mismatch: downloaded {got} bytes, uploaded {want} bytes"
            if VERIFY_MD5:
                a, b = got_md5, md5_file(local_path)
                if a != b:
                    return False, f"url proof md5 mismatch: {a} != {b}"
            return True, f"OK ({human(got)} downloaded back through the url)"
        finally:
            shutil.rmtree(str(td), ignore_errors=True)

    def _process_one(self, key: str, row: dict, num: int, total: int):
        entry = self.entry_by_key[key]
        if entry["status"] == "chained":
            return
        print(f"\n[{num}/{total}] {row['folder']} / {row['subject']}"
              f"{' / ' + row['section'] if row.get('section') else ''} / {row['lectureName']}")
        if row.get("drm"):
            self._mark_failed(key, "DRM-protected video — the server does not serve a direct stream for it")
            return
        expected = float(row.get("sizeMb") or 0) * 1e6
        try:
            free = shutil.disk_usage(str(self.download_dir)).free
        except Exception:
            free = None
        if free is not None and free < expected * 1.3 + 256 * 1024 * 1024:
            self._mark_failed(key, f"not enough local disk space in {self.download_dir} "
                                   f"({human(free)} free) for this video (~{human(expected)})")
            print(f"  [disk] not enough local space for this video ({human(free)} free) — skipped")
            return
        tmp = self.download_dir / f"{row['id']}.part"
        for size_attempt in range(2):
            if not self._stream_video(row, tmp, key, expected_bytes=expected,
                                      label=str(row.get("lectureName") or row["id"])):
                drop_file_cache(tmp)
                return
            if self._size_ok(tmp, row):
                break
            print("  [warn] size mismatch — re-downloading")
            drop_file_cache(tmp)
            tmp.unlink(missing_ok=True)
        else:
            self._mark_failed(key, "size mismatch after retries")
            return
        final = self.download_dir / f"{row['id']}.mp4"
        os.replace(tmp, final)
        self.ram.track(final)
        if self.stop_event.is_set():
            raise UserAbort("run stopped — rerun to resume")
        with self.chain_lock:
            mega_path = entry.get("mega_path") or "/".join(
                self._mega_parts(row) + [self._remote_name(row, entry)])
            if self.mega.path_exists("/" + mega_path):
                mega_path = f"{mega_path[:-4]} [{row['id']}].mp4"
            self.used_paths.add(mega_path)
            entry["mega_path"] = mega_path
        size_bytes = final.stat().st_size
        print(f"  [mega] uploading {final.name} ({human(size_bytes)})")
        link = ""
        info = ""
        ok = False
        try:
            for verify_attempt in range(1, VERIFY_RETRIES + 1):
                link = self.mega.upload(final, mega_path, size_bytes)
                drop_file_cache(final)
                print(f"  [proof] verifying the exported url works: {link}")
                ok, info = self.verify_url(self.mega, link, final)
                if ok:
                    print(f"  [proof] URL VERIFIED {info}")
                    break
                print(f"  [proof] url verification FAILED ({info}) — attempt "
                      f"{verify_attempt}/{VERIFY_RETRIES}: removing the remote copy and re-uploading")
                try:
                    self.mega.rm(mega_path)
                except Exception:
                    pass
        except UserAbort:
            raise
        except Exception as e:
            self._mark_failed(key, f"upload failed: {e}")
            print(f"  [error] upload failed: {e}")
            drop_file_cache(final)
            self.ram.untrack(final)
            if CLEANUP_TEMP:
                final.unlink(missing_ok=True)
            return
        if not ok:
            self._mark_failed(key, f"url verification failed after {VERIFY_RETRIES} attempts: {info}")
            drop_file_cache(final)
            self.ram.untrack(final)
            if CLEANUP_TEMP:
                final.unlink(missing_ok=True)
            return
        with self.chain_lock:
            entry.update(status="chained", mega_url=link, mega_path=mega_path,
                         mega_account=self.mega.email, uploaded=iso_now(),
                         verified=iso_now(), verify=info)
            entry.pop("error", None)
            self.save_chain()
            recorded = sum(1 for e in self.chain if e["status"] == "chained" and e["mega_url"])
        if CLEANUP_TEMP:
            drop_file_cache(final)
            final.unlink(missing_ok=True)
        self.ram.untrack(final)
        print(f"  [OK] #{entry['seq']} {row['folder']} / {row['subject']}"
              f"{' / ' + row['section'] if row.get('section') else ''} — {row['lectureName']} "
              f"({human(size_bytes)})")
        print(f"       mega url: {link}")
        print(f"       appended to cordchain.json — {recorded} video(s) recorded")

    def _process_one_safe(self, key: str, row: dict, num: int, total: int):
        if self.stop_event.is_set():
            return None
        try:
            self._process_one(key, row, num, total)
            return None
        except (UserAbort, KeyboardInterrupt) as e:
            self.stop_event.set()
            return e
        except Exception as e:
            print(f"  [error] {row.get('lectureName')}: {e}")
            traceback.print_exc()
            self._mark_failed(key, str(e))
            return None
        finally:
            gc.collect()

    def run(self, providers=None):
        global PARALLEL
        if PARALLEL > 1 and not os.environ.get("PARALLEL_FORCE"):
            print(f"\n[run] PARALLEL={PARALLEL} auto-set to 1 — MedCore's edge punishes 2+ TCP "
                  f"connections from one IP (measured: ~0.4-1 MB/s or outright stalls). The "
                  f"single-connection HTTP/2 path (x{DL_H2_STREAMS} streams, ~22 MB/s) already "
                  f"saturates the per-IP ceiling, so extra workers only cause stalls like "
                  f"'segment stalled at 20971520 bytes'. Set PARALLEL_FORCE=1 to override.")
            PARALLEL = 1
        if self.auth is None or self.mega is None:
            raise RuntimeError("pipeline not attached to auth/mega")
        targets = [(e["key"], self.item_by_key[e["key"]], e["seq"])
                   for e in self.chain
                   if e["status"] != "chained"
                   and (not SNO or e["seq"] >= SNO)
                   and (providers is None or e["provider"] in providers)]
        total = len(self.chain)
        filt = f" — providers: {', '.join(sorted(providers))}" if providers else ""
        print(f"\n{'=' * 60}\nCORDCHAIN RUN: {total} item(s) in sequence, {len(targets)} to transfer "
              f"[{'parallel x' + str(PARALLEL) if PARALLEL > 1 else 'strict one-by-one'}]"
              f"{f' — starting at global #{SNO}' if SNO else ''}{filt}\n{'=' * 60}")
        if providers:
            outside = sum(1 for e in self.chain
                          if e["status"] != "chained" and e["provider"] not in providers)
            if outside:
                print(f"[run] {outside} pending item(s) outside this provider filter stay "
                      f"pending in cordchain.json for the next run")
        if PARALLEL > 1:
            print(f"[run] PARALLEL={PARALLEL} forced via PARALLEL_FORCE=1 — expect MedCore "
                  f"connection-count throttling and mid-file stalls (downloads are fastest "
                  f"from ONE connection carrying x{DL_H2_STREAMS} h2 streams)")
        if not targets:
            if providers:
                print("[run] nothing to do for the selected providers — already chained "
                      "or outside the filter")
            else:
                print("[run] nothing to do — every item is already chained in cordchain.json")
            return None
        stopped = None
        self.ram.start()
        try:
            if PARALLEL <= 1 or len(targets) <= 1:
                for n, (k, r, _) in enumerate(targets, 1):
                    stopped = self._process_one_safe(k, r, n, len(targets))
                    if stopped:
                        break
            else:
                workers = min(PARALLEL, len(targets))
                with ThreadPoolExecutor(max_workers=workers) as ex:
                    futs = [ex.submit(self._process_one_safe, k, r, i + 1, len(targets))
                            for i, (k, r, _) in enumerate(targets)]
                    try:
                        for fut in as_completed(futs):
                            err = fut.result()
                            if isinstance(err, (UserAbort, KeyboardInterrupt)):
                                stopped = stopped or err
                                for f in futs:
                                    f.cancel()
                    except KeyboardInterrupt:
                        stopped = KeyboardInterrupt()
                        for f in futs:
                            f.cancel()
                if stopped:
                    print("\n[run] stopping workers — waiting for in-flight videos ...")
        finally:
            self.ram.stop()
        self.save_chain()
        return stopped


def main(argv=None):
    print("=" * 60)
    print(" CORDCHAIN — MedCore -> MEGA.nz cordchain app")
    print(f" dex from DEX_URL -> cordchain.json records ONLY uploaded+verified videos "
          f"(threaded x{PARALLEL}, fast url validation)")
    print("=" * 60)
    args = parse_args(argv)
    root, source = resolve_root()
    print(f"[store] project dir: {root} ({source})")
    avail = available_ram_mb()
    print(f"[ram] guard on — video cache evicted as it is written, downloads pause below "
          f"{RAM_FLOOR_MB} MB available"
          + (f" (now {avail} MB available)" if avail is not None else ""))
    store = FileStore(root)
    items, dex_name = load_dex(store, dex_path=args.dex)
    if args.list:
        counts = list_providers(items)
        print(f"\nProviders in {dex_name} ({len(counts)}):")
        for p in sorted(counts):
            print(f"  {p} — {counts[p]} video(s)")
        print(f"total: {len(counts)} provider(s), {sum(counts.values())} video(s)")
        print("\nrun a few providers:  %run cordchain.py --provider <name> [<name> ...]")
        print("run everything:       %run cordchain.py")
        return
    chosen, _ = select_providers(items, args.provider)
    if chosen:
        print(f"[args] this run transfers only {len(chosen)} provider(s): {', '.join(sorted(chosen))}")
    _ensure_pycryptodome()
    pipe = Pipeline(store, items, Path(os.environ.get(
        "DOWNLOAD_DIR",
        "/content/medcore_tmp" if Path("/content").exists() else str(store.root / "medcore_tmp"))))
    print(f"[tmp] videos download to {pipe.download_dir} (local disk — only cordchain.json and "
          f"account files live in the project dir)")
    pipe.build_chain()
    mega = MegaManager(store)
    try:
        auth = probe_medcore(store)
    except RuntimeError as e:
        print(f"[error] {e}")
        sys.exit(1)
    pipe.attach(auth, mega)
    if not any(e["status"] == "pending" and (chosen is None or e["provider"] in chosen)
               and (not SNO or e["seq"] >= SNO) for e in pipe.chain):
        print("\n[run] nothing to do for this filter — every video is already chained "
              "in cordchain.json")
        pipe.save_chain()
        return
    if not mega.ensure_login("MEGA login required"):
        print("No MEGA account available — exiting. Run the app again.")
        sys.exit(1)
    stopped = None
    try:
        stopped = pipe.run(providers=chosen)
    except KeyboardInterrupt:
        print("\nInterrupted — cordchain.json is saved and up to date.")
        stopped = "interrupted"
    except UserAbort as e:
        print(f"\nStopped: {e}")
        stopped = str(e)
    finally:
        pipe.save_chain()
    chained = sum(1 for e in pipe.chain if e["status"] == "chained")
    failed = [e for e in pipe.chain if e["status"] == "failed"]
    pending = len(pipe.chain) - chained - len(failed)
    print(f"\n{'=' * 60}\nSUMMARY: {chained}/{len(pipe.chain)} chained with verified working urls, "
          f"{pending} pending, {len(failed)} failed")
    if failed:
        print("Failed items (rerun the app to retry them in sequence):")
        for e in failed[:15]:
            print(f"  #{e['seq']} [{e['provider']} / {e['subject']}] {e['name']} — {e.get('error', '')[:120]}")
        if len(failed) > 15:
            print(f"  ... and {len(failed) - 15} more (see cordchain.json)")
    print(f"cordchain.json: {store.root / 'cordchain.json'}")
    if stopped:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except KeyboardInterrupt:
        print("\nInterrupted — progress saved.")
        sys.exit(0)


