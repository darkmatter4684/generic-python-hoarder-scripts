# Entity Hoarder

Interactive single-file CLI to store and manage researched "entities" (people, websites, features, etc.)

Location
- Script: `entity-hoarder/entity-hoarder.py`
- DB: `entity-hoarder/entities.db` (created automatically)

Usage
```
python3 entity-hoarder/entity-hoarder.py
```

Notes
- The script detects FTS5 and JSON1 support in your Python's SQLite build and uses them when available.
- If `rapidfuzz` is installed, it will be used for better fuzzy matching. Install with:

```
uv install rapidfuzz
```

- All changes are committed immediately to reduce data loss risk.

Exit
- Type `quit`, `exit`, or `:q`, press Ctrl-D (EOF), or confirm exit on Ctrl-C.
