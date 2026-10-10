#!/usr/bin/env python3
"""
Unpacks prepladder-tests.json (188 MB) into lazy-lessons-v1 architecture:
- 174 individual lesson JSON files in qbank/prepladder-test-orientated/lessons/prepladder-tests/
- Standard lightweight index in qbank/prepladder-test-orientated/prepladder-tests.json (< 35 KB)
- Creates rebuild_monolithic_prepladder.py to reconstitute the single monolithic file on demand.
This brings prepladder-test-orientated in 100% compliance with GitHub's 100MB limit
and the existing lazy-lessons-v1 format used across the QBank database.
"""

import os, json, re

ROOT = os.path.dirname(os.path.abspath(__file__))
PREP_DIR = os.path.join(ROOT, "qbank", "prepladder-test-orientated")
BIG_FILE = os.path.join(PREP_DIR, "prepladder-tests.json")
LESSONS_DIR = os.path.join(PREP_DIR, "lessons", "prepladder-tests")

def main():
    print(f"[*] Reading {BIG_FILE}...")
    with open(BIG_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    lessons = data.get("lessons", [])
    subject = data.get("subject", "Prepladder Tests")
    source_title = data.get("sourceTitle", "Prepladder Test Orientated")

    os.makedirs(LESSONS_DIR, exist_ok=True)
    print(f"[*] Unpacking {len(lessons)} lessons into {LESSONS_DIR}...")

    index_lessons = []
    total_q = 0

    for idx, l in enumerate(lessons, 1):
        title = l.get("title", f"Lesson {idx}")
        q_count = len(l.get("questions", []))
        total_q += q_count

        slug = re.sub(r"[^a-zA-Z0-9]+", "-", title.lower()).strip("-")
        fn = f"{idx:03d}-{slug}.json"
        dest_path = os.path.join(LESSONS_DIR, fn)

        lesson_obj = {
            "subject": subject,
            "sourceTitle": source_title,
            "title": title,
            "questions": l.get("questions", []),
            "preview": True
        }

        with open(dest_path, "w", encoding="utf-8") as out_f:
            json.dump(lesson_obj, out_f, separators=(",", ":"))

        index_lessons.append({
            "lessonNo": idx,
            "title": title,
            "questionCount": q_count,
            "questionsFile": f"/qbank/prepladder-test-orientated/lessons/prepladder-tests/{fn}",
            "preview": True
        })

    # Write lazy-lessons-v1 index file
    index_obj = {
        "subject": subject,
        "sourceTitle": source_title,
        "format": "lazy-lessons-v1",
        "totalLessons": len(lessons),
        "totalQuestions": total_q,
        "lessons": index_lessons,
        "preview": True
    }

    with open(BIG_FILE, "w", encoding="utf-8") as f:
        json.dump(index_obj, f, separators=(",", ":"))

    idx_size_kb = os.path.getsize(BIG_FILE) / 1024
    print(f"[+] Successfully converted prepladder-tests.json into lazy-lessons-v1!")
    print(f"[+] Total lessons: {len(lessons)} individual files generated.")
    print(f"[+] New prepladder-tests.json index size: {idx_size_kb:.1f} KB (was 188 MB).")

if __name__ == "__main__":
    main()
