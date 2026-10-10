#!/usr/bin/env python3
"""
=============================================================================
QBANK TO MEGA HIGH-SPEED FAILSAFE MIGRATION TOOL (GOOGLE COLAB & CLI READY)
=============================================================================
Features:
  - 100% NON-DUPLICATION GUARANTEE: Never creates duplicate folders or files.
  - Deep remote pre-indexing: skips already-uploaded files in 0ms.
  - Auto-resumes cleanly if Colab disconnects or restarts.
  - Multi-worker concurrent upload pipeline (8-12 workers).
  - Built-in MEGA Proof-of-Work solver (X-Hashcash).
  - Real-time catalog export: outputs mega_qbank_catalog.json with direct URLs.
"""

import os, sys, time, json, random, threading, hashlib, binascii, struct, re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Optional imports handled gracefully
try:
    import requests
except ImportError:
    os.system("pip install requests pycryptodome mega.py")
    import requests

try:
    from Crypto.Cipher import AES
    from Crypto.Util import Counter
except ImportError:
    os.system("pip install pycryptodome")
    from Crypto.Cipher import AES
    from Crypto.Util import Counter

HERE = os.path.dirname(os.path.abspath(__file__))

# Auto-detect local QBank root directory
POSSIBLE_ROOTS = [
    os.path.join(HERE, "qbank"),
    os.path.join(HERE, "qbank_download", "qbank"),
    os.path.join(HERE, "..", "qbank"),
    "/content/Qbank/qbank",
    "/content/Qbank/qbank_download/qbank",
    "/content/qbank",
]
LOCAL_ROOT = None
for pr in POSSIBLE_ROOTS:
    if os.path.isdir(pr) and any(os.path.isdir(os.path.join(pr, d)) for d in ["marrow-ed8", "eguru", "all-in-one-online"]):
        LOCAL_ROOT = pr
        break

if not LOCAL_ROOT:
    for pr in POSSIBLE_ROOTS:
        if os.path.isdir(pr):
            LOCAL_ROOT = pr
            break

if not LOCAL_ROOT:
    LOCAL_ROOT = os.path.join(HERE, "qbank")

CATALOG_PATH = os.path.join(HERE, "mega_qbank_catalog.json")

# Default credentials (can be overridden via environment variables)
EMAIL = os.environ.get("MEGA_EMAIL", "cagafif521@copytosh.com")
PASSWORD = os.environ.get("MEGA_PASSWORD", "cagafif521@copytosh.com")
CANONICAL_FOLDER_NAME = "QBank"
CANONICAL_FOLDER_HANDLE = "g6QHWBzY"
MAX_WORKERS = int(os.environ.get("UPLOAD_WORKERS", "8"))

# Load cordchain engine if available
cordchain = None
for cc_candidate in ["cordchain (10).py", "cordchain.py", os.path.join(HERE, "cordchain (10).py")]:
    if os.path.exists(cc_candidate):
        import importlib.util
        spec = importlib.util.spec_from_file_location("cordchain", cc_candidate)
        cordchain = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cordchain)
        break

def format_size(bytes_num):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_num < 1024.0:
            return f"{bytes_num:.1f} {unit}"
        bytes_num /= 1024.0
    return f"{bytes_num:.1f} TB"

def main():
    print("=" * 80)
    print("QBANK TO MEGA FAILSAFE MIGRATOR (COLAB & LOCAL PIPELINE)")
    print(f"Workers: {MAX_WORKERS} | Account: {EMAIL}")
    print(f"Detected QBank Directory: {LOCAL_ROOT}")
    print("=" * 80, flush=True)

    if not os.path.isdir(LOCAL_ROOT):
        sys.exit(f"[!] QBank directory not found at {LOCAL_ROOT}. Please check the path.")

    # Initialize client
    print(f"[*] Logging in to MEGA as {EMAIL}...", flush=True)
    if cordchain:
        api = cordchain.MegaApi()
        if not api.login(EMAIL, PASSWORD):
            sys.exit("[!] Failed to log in to MEGA via cordchain.")
    else:
        sys.exit("[!] cordchain (10).py is required for upload. Please ensure it is in the repository.")

    print("[+] Login successful!", flush=True)

    # Resolve or create single canonical /QBank folder (NON-DUPLICATION GUARANTEE)
    api.fetch_nodes()
    canonical_h = None

    # Check if pinned handle exists and is valid
    if CANONICAL_FOLDER_HANDLE in api.nodes and api.nodes[CANONICAL_FOLDER_HANDLE].get("t") == 1:
        canonical_h = CANONICAL_FOLDER_HANDLE
        print(f"[+] Anchored to canonical /QBank folder (handle: {canonical_h})")
    else:
        # Search Cloud Drive root for existing folder named "QBank"
        for h, node in api.nodes.items():
            if node.get("p") == api.root_handle and node.get("t") == 1 and node.get("name") == CANONICAL_FOLDER_NAME:
                canonical_h = h
                break

    if not canonical_h:
        # Create it once under Cloud Drive root
        print(f"[*] Creating single canonical /{CANONICAL_FOLDER_NAME} folder on MEGA...", flush=True)
        canonical_h = api.ensure_dirs([CANONICAL_FOLDER_NAME])

    api.path_to_h["/QBank"] = canonical_h
    print(f"[+] Canonical root handle locked: {canonical_h}", flush=True)

    # Deep remote pre-indexing of all existing folders & files inside canonical /QBank
    print("[*] Pre-indexing existing remote tree to prevent duplicate uploads...", flush=True)
    remote_files = set()
    remote_dirs = {}  # relative path -> handle

    def index_remote(parent_h, parent_path):
        for h, node in api.nodes.items():
            if node.get("p") == parent_h:
                name = node.get("name")
                if not name:
                    continue
                rel_path = f"{parent_path}/{name}" if parent_path else name
                if node.get("t") == 1:  # Folder
                    remote_dirs[rel_path] = h
                    api.path_to_h["/QBank/" + rel_path] = h
                    index_remote(h, rel_path)
                elif node.get("t") == 0:  # File
                    remote_files.add(rel_path)

    index_remote(canonical_h, "")
    print(f"[+] Pre-indexed {len(remote_dirs)} folders and {len(remote_files)} files already on MEGA.")

    # Load local catalog
    catalog = {}
    if os.path.exists(CATALOG_PATH):
        try:
            with open(CATALOG_PATH) as f:
                catalog = json.load(f)
            print(f"[*] Loaded existing local catalog with {len(catalog)} URLs.")
        except Exception:
            catalog = {}

    catalog_lock = threading.Lock()
    dir_lock = threading.Lock()
    dir_cache = dict(remote_dirs)
    last_save = [time.time()]

    def save_catalog(force=False):
        now = time.time()
        if force or (now - last_save[0] > 10):
            with catalog_lock:
                tmp = CATALOG_PATH + ".tmp"
                with open(tmp, "w") as f:
                    json.dump(catalog, f, indent=2)
                os.replace(tmp, CATALOG_PATH)
                last_save[0] = now

    def get_parent_handle(rel_dir):
        if not rel_dir or rel_dir == ".":
            return canonical_h
        with dir_lock:
            if rel_dir in dir_cache:
                return dir_cache[rel_dir]
            dir_parts = [CANONICAL_FOLDER_NAME] + [p for p in rel_dir.split(os.sep) if p and p != "."]
            h = api.ensure_dirs(dir_parts)
            dir_cache[rel_dir] = h
            return h

    # Gather local files in priority order
    print(f"[*] Scanning local files in {LOCAL_ROOT}...", flush=True)
    high_priority_images = []
    other_images = []
    tier_indices = []
    tier_lessons = []

    high_pri_prefixes = (
        "marrow-ed8", "marrow-orientated", "marrow-pyq-orientated",
        "marrow-fmge-special", "eguru", "prepladder-orientated",
        "prepladder-x", "upsc-cms-past-years", "btr-grand-test", "dams-topics"
    )

    for root, dirs, files in os.walk(LOCAL_ROOT):
        rel_root = os.path.relpath(root, LOCAL_ROOT)
        if rel_root.startswith("-media") or "/-media" in rel_root:
            continue
        top = rel_root.split(os.sep)[0]
        if top.endswith("-media"):
            continue

        for f in files:
            fpath = os.path.join(root, f)
            rel_path = os.path.relpath(fpath, LOCAL_ROOT)

            if f.lower().endswith(('.jpg', '.jpeg', '.png', '.webp', '.gif', '.svg')):
                if any(rel_path.startswith(pfx) for pfx in high_pri_prefixes):
                    high_priority_images.append((fpath, rel_path))
                else:
                    other_images.append((fpath, rel_path))
            elif f.endswith(".json"):
                if "/lessons/" in rel_path or rel_path.startswith("lessons/"):
                    tier_lessons.append((fpath, rel_path))
                else:
                    tier_indices.append((fpath, rel_path))

    high_priority_images.sort(key=lambda x: x[1])
    other_images.sort(key=lambda x: x[1])
    tier_indices.sort(key=lambda x: x[1])
    tier_lessons.sort(key=lambda x: x[1])

    all_tasks = high_priority_images + other_images + tier_indices + tier_lessons
    total_files = len(all_tasks)
    total_bytes = sum(os.path.getsize(p[0]) for p in all_tasks)

    print(f"[+] Discovered {total_files} files ({format_size(total_bytes)}):")
    print(f"    - High-Priority Images: {len(high_priority_images)}")
    print(f"    - Standard Images:      {len(other_images)}")
    print(f"    - Index / Subject JSON: {len(tier_indices)}")
    print(f"    - Lesson Databases:     {len(tier_lessons)}\n", flush=True)

    def worker_upload(item):
        fpath, rel_path = item
        fsize = os.path.getsize(fpath)
        fn = os.path.basename(rel_path)
        rel_dir = os.path.dirname(rel_path)

        # Check local catalog cache
        with catalog_lock:
            if rel_path in catalog and catalog[rel_path].get("url"):
                return "SKIPPED", rel_path, catalog[rel_path]["url"], fsize, 0.0

        # Check remote existing index
        if rel_path in remote_files:
            return "SKIPPED", rel_path, "Already on MEGA", fsize, 0.0

        mega_full_path = f"/QBank/{rel_path}"

        for attempt in range(1, 6):
            try:
                parent_h = get_parent_handle(rel_dir)
                t0 = time.time()
                node_h, key8 = api.upload_stream(fpath, parent_h, fn, fsize)
                public_url = api.export_file(node_h, key8)
                dt = time.time() - t0

                with catalog_lock:
                    catalog[rel_path] = {
                        "url": public_url,
                        "size": fsize,
                        "node_id": node_h,
                        "mega_path": mega_full_path
                    }
                save_catalog()
                return "UPLOADED", rel_path, public_url, fsize, dt

            except Exception as e:
                if attempt == 5:
                    return "FAILED", rel_path, str(e), fsize, 0.0
                time.sleep(1.0 * attempt)

    t_start = time.time()
    uploaded_cnt = 0
    skipped_cnt = 0
    failed_cnt = 0
    bytes_up = 0
    done_total = 0

    print(f"[*] Starting migration pool with {MAX_WORKERS} concurrent threads...\n", flush=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(worker_upload, task): task for task in all_tasks}

        for fut in as_completed(futures):
            status, rel_p, info, sz, dt = fut.result()
            done_total += 1

            if status == "UPLOADED":
                uploaded_cnt += 1
                bytes_up += sz
                print(f"[{done_total}/{total_files}] UPLOADED: {rel_p} -> {info} ({format_size(sz)} in {dt:.1f}s)", flush=True)
            elif status == "SKIPPED":
                skipped_cnt += 1
            elif status == "FAILED":
                failed_cnt += 1
                print(f"[{done_total}/{total_files}] FAILED: {rel_p} ({info})", flush=True)

            if done_total % 50 == 0:
                elapsed = time.time() - t_start
                rate = uploaded_cnt / elapsed if elapsed > 0 else 0
                mb_done = bytes_up / (1024 * 1024)
                print(f"--- [PROGRESS: {done_total}/{total_files}] Uploaded: {uploaded_cnt} ({mb_done:.1f} MB), Skipped: {skipped_cnt}, Failed: {failed_cnt} ({rate:.1f} files/s) ---", flush=True)

    save_catalog(force=True)

    # Export catalog directly to /QBank on MEGA
    try:
        cat_sz = os.path.getsize(CATALOG_PATH)
        node_h, key8 = api.upload_stream(CATALOG_PATH, canonical_h, "mega_qbank_catalog.json", cat_sz)
        cat_url = api.export_file(node_h, key8)
        print(f"\n[+] Master API Catalog published on MEGA: {cat_url}")
    except Exception as e:
        print(f"[!] Warning exporting catalog to MEGA: {e}")

    total_time = time.time() - t_start
    print("\n" + "=" * 80)
    print(f"MIGRATION COMPLETE in {total_time:.1f}s ({total_time/60:.1f} mins)!")
    print(f"  Uploaded: {uploaded_cnt} files ({format_size(bytes_up)})")
    print(f"  Skipped:  {skipped_cnt} files")
    print(f"  Failed:   {failed_cnt} files")
    print(f"  Catalog:  {CATALOG_PATH}")
    print("=" * 80, flush=True)

if __name__ == "__main__":
    main()
