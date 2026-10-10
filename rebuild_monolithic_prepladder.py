#!/usr/bin/env python3
"""
Rebuilds the monolithic 188 MB prepladder-tests.json from individual lazy-lesson JSON files.
Run anytime you need a single consolidated file instead of individual lesson files.
"""

import os, json

ROOT = os.path.dirname(os.path.abspath(__file__))
PREP_DIR = os.path.join(ROOT, "qbank", "prepladder-test-orientated")
INDEX_FILE = os.path.join(PREP_DIR, "prepladder-tests.json")
OUTPUT_FILE = os.path.join(PREP_DIR, "prepladder-tests.monolithic.json")

def main():
    if not os.path.exists(INDEX_FILE):
        print(f"[!] Index not found: {INDEX_FILE}")
        return

    with open(INDEX_FILE, "r", encoding="utf-8") as f:
        meta = json.load(f)

    lessons = []
    print(f"[*] Rebuilding from {len(meta.get('lessons', []))} individual lesson files...")
    for item in meta.get("lessons", []):
        rel_path = item["questionsFile"].lstrip("/").replace("qbank/prepladder-test-orientated/", "")
        fpath = os.path.join(PREP_DIR, rel_path)
        if os.path.exists(fpath):
            with open(fpath, "r", encoding="utf-8") as lf:
                ld = json.load(lf)
                lessons.append({
                    "lessonNo": item["lessonNo"],
                    "title": item["title"],
                    "questionCount": len(ld.get("questions", [])),
                    "questions": ld.get("questions", [])
                })

    rebuilt = {
        "subject": meta.get("subject", "Prepladder Tests"),
        "sourceTitle": meta.get("sourceTitle", "Prepladder Test Orientated"),
        "totalLessons": len(lessons),
        "totalQuestions": sum(l["questionCount"] for l in lessons),
        "lessons": lessons
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        json.dump(rebuilt, out, indent=2)

    sz_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"[+] Successfully rebuilt {OUTPUT_FILE} ({sz_mb:.1f} MB)!")

if __name__ == "__main__":
    main()
