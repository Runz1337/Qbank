#!/usr/bin/env python3
"""
QBank Client SDK:
Allows your AI agent, backend, or script to easily query sets, subjects, lessons,
and fetch direct MEGA public URLs or local offline files.
"""

import os, json

HERE = os.path.dirname(os.path.abspath(__file__))
LOCAL_QBANK = os.path.join(HERE, "qbank_download", "qbank")
CATALOG_PATH = os.path.join(HERE, "mega_qbank_catalog.json")

class QBankClient:
    def __init__(self, qbank_dir=LOCAL_QBANK, catalog_path=CATALOG_PATH):
        self.qbank_dir = qbank_dir
        self.catalog_path = catalog_path
        self.catalog = {}
        if os.path.exists(self.catalog_path):
            try:
                self.catalog = json.load(open(self.catalog_path))
            except Exception:
                self.catalog = {}

    def list_sets(self):
        """List all available question bank sets."""
        if not os.path.exists(self.qbank_dir):
            return []
        return sorted([
            d for d in os.listdir(self.qbank_dir)
            if os.path.isdir(os.path.join(self.qbank_dir, d)) and not d.endswith('-media')
        ])

    def get_set_index(self, set_id):
        """Get the index.json metadata for a specific set."""
        idx_file = os.path.join(self.qbank_dir, set_id, "index.json")
        if os.path.exists(idx_file):
            return json.load(open(idx_file, encoding="utf-8", errors="ignore"))
        return None

    def get_subjects(self, set_id):
        """Get all subjects within a set."""
        idx = self.get_set_index(set_id)
        if idx and "subjects" in idx:
            return idx["subjects"]
        return {}

    def get_subject_data(self, set_id, subject_slug):
        """Get subject details and lessons list (e.g. anatomy.json)."""
        fpath = os.path.join(self.qbank_dir, set_id, f"{subject_slug}.json")
        if os.path.exists(fpath):
            return json.load(open(fpath, encoding="utf-8", errors="ignore"))
        return None

    def get_lesson(self, set_id, subject_slug, lesson_filename):
        """Get full question payload for a lesson."""
        fpath = os.path.join(self.qbank_dir, set_id, "lessons", subject_slug, lesson_filename)
        if os.path.exists(fpath):
            return json.load(open(fpath, encoding="utf-8", errors="ignore"))
        return None

    def get_mega_url(self, rel_path):
        """Get the direct public MEGA URL for any file (e.g. 'all-in-one-online/anatomy.json')."""
        clean_rel = rel_path.lstrip("/ ")
        entry = self.catalog.get(clean_rel)
        if entry:
            return entry.get("url")
        return None

if __name__ == "__main__":
    client = QBankClient()
    print("Available QBank Sets:")
    for s in client.list_sets():
        idx = client.get_set_index(s)
        total_q = idx.get("totalQuestions", "N/A") if idx else "N/A"
        print(f"  - {s:30} ({total_q} questions)")
