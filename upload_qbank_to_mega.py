#!/usr/bin/env python3
"""
Upload the complete QBank database to MEGA using multi-threaded cordchain engine.
Prioritizes images first (Marrow, Egurukul, and all subjects) so they appear immediately on MEGA.
Outputs:
    - mega_qbank_catalog.json: Master registry mapping local paths to public MEGA URLs
    - QBANK_API_DOCUMENTATION.md: Full integration guide and API documentation for agents/apps
"""

import os, sys, time, json, importlib.util
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Load cordchain module
spec = importlib.util.spec_from_file_location("cordchain", "cordchain (10).py")
cordchain = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cordchain)

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "qbank_download", "qbank")
CATALOG_PATH = os.path.join(HERE, "mega_qbank_catalog.json")

EMAIL = os.environ.get("MEGA_EMAIL", "cagafif521@copytosh.com")
PASSWORD = os.environ.get("MEGA_PASSWORD", "cagafif521@copytosh.com")
DEST_ROOT = "QBank"
MAX_WORKERS = int(os.environ.get("UPLOAD_WORKERS", "6"))

def format_size(bytes_num):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_num < 1024.0:
            return f"{bytes_num:.1f} {unit}"
        bytes_num /= 1024.0
    return f"{bytes_num:.1f} TB"

def main():
    print("=" * 75)
    print("CORDCHAIN HIGH-SPEED MULTI-THREADED MEGA QBANK UPLOADER")
    print(f"Concurrency: {MAX_WORKERS} workers | Target Account: {EMAIL}")
    print("=" * 75, flush=True)

    api = cordchain.MegaApi()
    print(f"[*] Logging into MEGA as {EMAIL}...", flush=True)
    if not api.login(EMAIL, PASSWORD):
        sys.exit("[!] Failed to log in to MEGA.")
    print("[+] Login successful!")

    # Check storage quota
    d = api.api({"a": "uq", "xfer": 1, "strg": 1})
    if isinstance(d, dict) and "mstrg" in d:
        cap_gb = int(d["mstrg"]) / (1024**3)
        used_mb = int(d.get("cstrg", 0)) / (1024**2)
        print(f"[+] MEGA Account Quota: {used_mb:.1f} MB used of {cap_gb:.1f} GB total capacity.")

    print("[*] Fetching existing nodes on MEGA Cloud Drive...", flush=True)
    api.fetch_nodes()
    print(f"[+] Loaded {len(api.nodes)} nodes. Existing paths indexed: {len(api.path_to_h)}")

    # Load existing catalog
    catalog = {}
    if os.path.exists(CATALOG_PATH):
        try:
            with open(CATALOG_PATH) as f:
                catalog = json.load(f)
            print(f"[*] Loaded existing catalog with {len(catalog)} URLs.")
        except Exception as e:
            print(f"[!] Warning reading catalog: {e}")
            catalog = {}

    # Canonical root handle
    CANONICAL_ROOT_H = "g6QHWBzY"
    api.path_to_h["/QBank"] = CANONICAL_ROOT_H
    print(f"[+] Anchored to canonical /QBank folder handle: {CANONICAL_ROOT_H}")

    # Pre-index existing folders in canonical /QBank using mega client
    try:
        from mega import Mega
        _m = Mega()
        _m.login(EMAIL, PASSWORD)
        _files = _m.get_files()
        def _build_paths(parent_h, parent_path):
            for h, f in _files.items():
                if f.get('p') == parent_h and f.get('t') == 1:
                    name = f.get('a', {}).get('n', '')
                    if name:
                        full_p = parent_path + "/" + name
                        api.path_to_h[full_p] = h
                        _build_paths(h, full_p)
        _build_paths(CANONICAL_ROOT_H, "/QBank")
        print(f"[+] Pre-indexed {len(api.path_to_h)} existing folder paths inside /QBank.")
    except Exception as e:
        print(f"[!] Warning pre-indexing folders: {e}")

    # Thread safety objects
    catalog_lock = threading.Lock()
    dir_cache = {}
    dir_lock = threading.Lock()
    last_save_time = [time.time()]

    def get_parent_handle(rel_dir):
        with dir_lock:
            if rel_dir in dir_cache:
                return dir_cache[rel_dir]
            dir_parts = [DEST_ROOT] + [p for p in rel_dir.split(os.sep) if p and p != "."]
            h = api.ensure_dirs(dir_parts)
            dir_cache[rel_dir] = h
            return h

    def save_catalog_if_needed(force=False):
        now = time.time()
        if force or (now - last_save_time[0] > 10):
            with catalog_lock:
                tmp_path = CATALOG_PATH + ".tmp"
                with open(tmp_path, "w") as f:
                    json.dump(catalog, f, indent=2)
                os.replace(tmp_path, CATALOG_PATH)
                last_save_time[0] = now

    # Scan all files
    print("[*] Scanning local QBank directory structure...", flush=True)
    
    high_priority_images = []
    other_images = []
    tier_indices = []
    tier_lessons = []

    high_pri_prefixes = (
        "marrow-ed8", "marrow-orientated", "marrow-pyq-orientated", 
        "marrow-fmge-special", "eguru", "prepladder-orientated", 
        "prepladder-x", "upsc-cms-past-years", "btr-grand-test", "dams-topics"
    )

    for root, dirs, files in os.walk(ROOT):
        rel_root = os.path.relpath(root, ROOT)
        if rel_root.startswith("-media") or "/-media" in rel_root or "-media/" in rel_root:
            continue
        top = rel_root.split(os.sep)[0]
        if top.endswith("-media"):
            continue

        for f in files:
            fpath = os.path.join(root, f)
            rel_path = os.path.relpath(fpath, ROOT)
            
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

    # Priority Sort Order:
    # 1. High priority images (Marrow, Egurukul, Prepladder X/orientated, etc.)
    # 2. All remaining images (All-in-one, Cerebellum, DAMS, Prepladder-test)
    # 3. Core Indices (index.json, subject.json)
    # 4. Lesson Question JSONs
    high_priority_images.sort(key=lambda x: x[1])
    other_images.sort(key=lambda x: x[1])
    tier_indices.sort(key=lambda x: x[1])
    tier_lessons.sort(key=lambda x: x[1])

    all_tasks = high_priority_images + other_images + tier_indices + tier_lessons
    total_files = len(all_tasks)
    total_bytes = sum(os.path.getsize(p[0]) for p in all_tasks)

    print(f"[+] Scan complete:")
    print(f"    - High-Priority Images (Marrow, Eguru, etc.): {len(high_priority_images)} files")
    print(f"    - Standard Images (All-in-one, Cerebellum, etc.): {len(other_images)} files")
    print(f"    - Core Indices & Subjects: {len(tier_indices)} files")
    print(f"    - Lesson Question Databases: {len(tier_lessons)} files")
    print(f"    - TOTAL: {total_files} files ({format_size(total_bytes)})\n", flush=True)

    def worker_upload(task_item):
        local_path, rel_path = task_item
        rel_dir = os.path.dirname(rel_path)
        filename = os.path.basename(rel_path)
        file_size = os.path.getsize(local_path)

        # Check if already in catalog
        with catalog_lock:
            if rel_path in catalog and catalog[rel_path].get("url"):
                return "SKIPPED", rel_path, catalog[rel_path]["url"], file_size, 0.0

        dir_parts = [DEST_ROOT] + [p for p in rel_dir.split(os.sep) if p and p != "."]
        mega_full_path = "/" + "/".join(dir_parts) + "/" + filename

        # Retry loop
        for attempt in range(1, 5):
            try:
                parent_h = get_parent_handle(rel_dir)
                t0 = time.time()
                node_h, key8 = api.upload_stream(local_path, parent_h, filename, file_size)
                public_url = api.export_file(node_h, key8)
                dt = time.time() - t0

                with catalog_lock:
                    catalog[rel_path] = {
                        "url": public_url,
                        "size": file_size,
                        "node_id": node_h,
                        "mega_path": mega_full_path
                    }
                save_catalog_if_needed()
                return "UPLOADED", rel_path, public_url, file_size, dt
            except Exception as e:
                err_str = str(e)
                if attempt == 4:
                    return "FAILED", rel_path, err_str, file_size, 0.0
                time.sleep(1.0 * attempt)

    t_start = time.time()
    uploaded_count = 0
    skipped_count = 0
    failed_count = 0
    bytes_uploaded = 0
    completed_total = 0

    print(f"[*] Starting upload pool with {MAX_WORKERS} workers...\n", flush=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(worker_upload, task): task for task in all_tasks}

        for fut in as_completed(futures):
            status, rel_path, result_info, file_sz, dt = fut.result()
            completed_total += 1

            if status == "UPLOADED":
                uploaded_count += 1
                bytes_uploaded += file_sz
                print(f"[{completed_total}/{total_files}] UPLOADED: {rel_path} -> {result_info} ({format_size(file_sz)} in {dt:.1f}s)", flush=True)
            elif status == "SKIPPED":
                skipped_count += 1
            elif status == "FAILED":
                failed_count += 1
                print(f"[{completed_total}/{total_files}] FAILED: {rel_path} ({result_info})", flush=True)

            if completed_total % 25 == 0:
                elapsed = time.time() - t_start
                rate = uploaded_count / elapsed if elapsed > 0 else 0
                mb_up = bytes_uploaded / (1024 * 1024)
                print(f"--- [PROGRESS: {completed_total}/{total_files}] Uploaded: {uploaded_count} files ({mb_up:.1f} MB), Skipped: {skipped_count}, Failed: {failed_count} ({rate:.1f} files/s) ---", flush=True)

    # Save final catalog
    save_catalog_if_needed(force=True)

    # Upload master catalog file to root of QBank
    try:
        qbank_h = get_parent_handle("")
        cat_sz = os.path.getsize(CATALOG_PATH)
        cat_node, cat_key = api.upload_stream(CATALOG_PATH, qbank_h, "mega_qbank_catalog.json", cat_sz)
        cat_url = api.export_file(cat_node, cat_key)
        print(f"\n[+] MASTER API CATALOG UPLOADED TO MEGA:")
        print(f"    {cat_url}")
    except Exception as e:
        print(f"[!] Note: could not export catalog file: {e}")

    total_time = time.time() - t_start
    print("\n" + "=" * 75)
    print(f"MEGA UPLOAD COMPLETED in {total_time:.1f}s ({total_time/60:.1f} mins)!")
    print(f"  Uploaded: {uploaded_count}")
    print(f"  Skipped (already synced): {skipped_count}")
    print(f"  Failed: {failed_count}")
    print(f"  Data Uploaded: {format_size(bytes_uploaded)}")
    print(f"  Master Catalog saved to: {CATALOG_PATH}")
    print("=" * 75, flush=True)

if __name__ == "__main__":
    main()
