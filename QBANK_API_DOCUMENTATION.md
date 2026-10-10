# QBank Database API & Integration Documentation

This document explains the full architecture, schema, folder sequence, and direct MEGA API integration for the QBank database so your AI agent or application can query, fetch, and display any question, explanation, or visual.

---

## 1. Storage & Remote Structure

On your MEGA account, the entire database is mapped to `/QBank/`:

```text
/QBank/
├── <set_id>/                                    # e.g., all-in-one-online, marrow-ed8, eguru
│   ├── index.json                               # Set metadata & subject registry
│   ├── <subject>.json                           # Subject lessons & preview question
│   └── lessons/
│       └── <subject>/                           # e.g., lessons/anatomy/
│           ├── <lesson_id>.json                 # Full question payload
│           └── images/                          # Visuals & question diagrams
│               └── <image_id>.webp (or .jpg)
```

---

## 2. JSON Schemas

### A. Set Index Schema (`<set>/index.json`)
```json
{
  "sourceTitle": "All In One Online",
  "totalSubjects": 20,
  "totalQuestions": 65740,
  "subjects": {
    "Anatomy": {
      "file": "/qbank/all-in-one-online/anatomy.json",
      "totalLessons": 58,
      "totalQuestions": 1405
    }
  }
}
```

### B. Subject Schema (`<set>/<subject>.json`)
```json
{
  "subject": "Anatomy",
  "sourceTitle": "All In One Online",
  "totalLessons": 58,
  "lessons": [
    {
      "lessonNo": 1,
      "title": "Abdomen & Pelvis",
      "questionCount": 25,
      "questionsFile": "/qbank/all-in-one-online/lessons/anatomy/001-abdomen-pelvis.json",
      "preview": true
    }
  ]
}
```

### C. Lesson & Question Schema (`<set>/lessons/<subject>/<lesson_id>.json`)
```json
{
  "subject": "Anatomy",
  "lessonNo": 1,
  "title": "Abdomen & Pelvis",
  "questions": [
    {
      "id": "q-101",
      "number": 1,
      "stem": "Which artery is the principal blood supply to the stomach?",
      "stemHtml": "<p>Which artery is the principal blood supply to the stomach?</p><img src=\"images/diagram_01.jpg\" />",
      "options": [
        {"label": "A", "text": "Left gastric artery"},
        {"label": "B", "text": "Right gastro-omental artery"},
        {"label": "C", "text": "Splenic artery"},
        {"label": "D", "text": "Celiac trunk"}
      ],
      "answer": "A",
      "answerText": "Left gastric artery",
      "solutionHtml": "<p><strong>Left gastric artery</strong> arises directly from the celiac axis...</p>",
      "tags": ["GI", "Arterial Supply"]
    }
  ]
}
```

---

## 3. Resolving Image URLs in your App

Every image reference inside `stemHtml` or `solutionHtml` uses a clean relative path:
- **Inside lesson JSONs:** `src="images/<file>"` points to the `images/` directory in the **same folder** as the lesson JSON.
- **Inside standalone subject JSONs:** `src="<subject>/images/<file>"` points to the `<subject>/images/` subdirectory.

---

## 4. Programmatic Python SDK / Client (`qbank_client.py`)

A client is provided in the workspace to allow your agent or frontend to query questions locally or via MEGA:

```python
from qbank_client import QBankClient

client = QBankClient()

# 1. List all available sets
sets = client.list_sets()
print("Available Sets:", sets)

# 2. Get subjects for Marrow ED8
subjects = client.get_subjects("marrow-ed8")

# 3. Get questions for a specific lesson
lesson = client.get_lesson("all-in-one-online", "anatomy", "eguru-5401038-egurukul-anatomy-neet-pg-2023.json")
for q in lesson["questions"]:
    print(f"Q{q['number']}: {q['stem']}")
    print(f"Answer: {q['answer']} ({q['answerText']})")
```

---

## 5. Running the REST API Server (`serve_qbank_api.py`)

You can run the built-in HTTP REST API server on your backend:

```bash
python3 serve_qbank_api.py --port 8080
```

### Endpoints:
- `GET /api/sets` → Array of all available sets
- `GET /api/sets/:set/subjects` → Subject list with lesson counts
- `GET /api/sets/:set/subjects/:subject` → Lessons list
- `GET /api/sets/:set/lessons/:subject/:lessonFile` → Complete question payload
- `GET /api/sets/:set/images/:subject/:filename` → Direct image streaming
- `GET /api/mega/catalog` → Live JSON dictionary of all direct public `https://mega.nz/file/...` URLs
