#!/usr/bin/env python3
"""Entity Hoarder - single-file interactive CLI for storing researched entities.

Location: entity-hoarder/entities.db (created automatically)

Features:
- SQLite backend with immediate commits and WAL + synchronous=NORMAL for durability
- FTS5 and JSON1 detected at runtime (FTS used when available)
- Optional `rapidfuzz` use for fuzzy ranking; falls back to `difflib`
- Guided key/value metadata prompts with optional raw JSON editing
- Interactive-only CLI with stable exit commands and signal handling
"""
from __future__ import annotations

import sqlite3
import json
import os
import sys
import signal
from pathlib import Path
from datetime import datetime
import difflib
import textwrap
from typing import Optional, List, Dict, Any, Tuple

try:
    from rapidfuzz import process as rf_process
    from rapidfuzz import fuzz as rf_fuzz
    HAVE_RAPIDFUZZ = True
except Exception:
    rf_process = None
    rf_fuzz = None
    HAVE_RAPIDFUZZ = False

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "entities.db"

EXIT_COMMANDS = {"quit", "exit", ":q"}


def slugify(name: str) -> str:
    s = name.lower()
    s = ''.join(c if c.isalnum() else '-' for c in s)
    while '--' in s:
        s = s.replace('--', '-')
    return s.strip('-')[:200]


def open_conn(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    # Durability / performance pragmas
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def db_supports_fts(conn: sqlite3.Connection) -> bool:
    try:
        cur = conn.execute("PRAGMA compile_options;")
        opts = [row[0].upper() for row in cur.fetchall()]
        for o in opts:
            if 'ENABLE_FTS5' in o or 'FTS5' in o:
                return True
        # Fallback: try creating a temporary fts5 table and drop it
        conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS __fts_test USING fts5(content);")
        conn.execute("DROP TABLE IF EXISTS __fts_test;")
        return True
    except Exception:
        return False


def db_supports_json1(conn: sqlite3.Connection) -> bool:
    try:
        conn.execute("SELECT json('{}');")
        return True
    except Exception:
        return False


def init_db(conn: sqlite3.Connection) -> dict:
    fts_ok = db_supports_fts(conn)
    json_ok = db_supports_json1(conn)

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS entities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            name TEXT NOT NULL,
            slug TEXT UNIQUE,
            description TEXT,
            tags TEXT,
            metadata TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(type);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_entities_name ON entities(name);")
    conn.commit()

    if fts_ok:
        try:
            # content='entities' means fts table will reference the content table
            conn.execute(
                "CREATE VIRTUAL TABLE IF NOT EXISTS entities_fts USING fts5(name, description, tags, content='entities', content_rowid='id');"
            )
            # Triggers to keep FTS in sync
            conn.executescript(
                """
                CREATE TRIGGER IF NOT EXISTS entities_ai AFTER INSERT ON entities BEGIN
                  INSERT INTO entities_fts(rowid, name, description, tags) VALUES (new.id, new.name, new.description, new.tags);
                END;
                CREATE TRIGGER IF NOT EXISTS entities_ad AFTER DELETE ON entities BEGIN
                  INSERT INTO entities_fts(entities_fts, rowid, name, description, tags) VALUES('delete', old.id, old.name, old.description, old.tags);
                END;
                CREATE TRIGGER IF NOT EXISTS entities_au AFTER UPDATE ON entities BEGIN
                  INSERT INTO entities_fts(entities_fts, rowid, name, description, tags) VALUES('delete', old.id, old.name, old.description, old.tags);
                  INSERT INTO entities_fts(rowid, name, description, tags) VALUES (new.id, new.name, new.description, new.tags);
                END;
                """
            )
            conn.commit()
        except Exception:
            # If anything fails, mark fts as unavailable for runtime behavior
            fts_ok = False

    return {"fts": fts_ok, "json": json_ok}


def now_iso() -> str:
    return datetime.utcnow().isoformat() + 'Z'


def add_entity(conn: sqlite3.Connection, fields: Dict[str, Any]) -> int:
    slug = fields.get('slug') or slugify(fields.get('name', ''))
    created = now_iso()
    updated = created
    metadata_text = json.dumps(fields.get('metadata') or {})
    cur = conn.execute(
        """
        INSERT INTO entities (type, name, slug, description, tags, metadata, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            fields.get('type'),
            fields.get('name'),
            slug,
            fields.get('description'),
            fields.get('tags'),
            metadata_text,
            created,
            updated,
        ),
    )
    conn.commit()
    return cur.lastrowid


def update_entity(conn: sqlite3.Connection, entity_id: int, fields: Dict[str, Any]) -> None:
    # build set clause dynamically
    parts = []
    vals: List[Any] = []
    for k in ('type', 'name', 'slug', 'description', 'tags'):
        if k in fields and fields[k] is not None:
            parts.append(f"{k} = ?")
            vals.append(fields[k])
    if 'metadata' in fields and fields['metadata'] is not None:
        parts.append('metadata = ?')
        vals.append(json.dumps(fields['metadata']))
    parts.append('updated_at = ?')
    vals.append(now_iso())
    vals.append(entity_id)
    sql = f"UPDATE entities SET {', '.join(parts)} WHERE id = ?"
    conn.execute(sql, tuple(vals))
    conn.commit()


def delete_entity(conn: sqlite3.Connection, entity_id: int) -> None:
    conn.execute("DELETE FROM entities WHERE id = ?", (entity_id,))
    conn.commit()


def fetch_entity(conn: sqlite3.Connection, entity_id: int) -> Optional[sqlite3.Row]:
    cur = conn.execute("SELECT * FROM entities WHERE id = ?", (entity_id,))
    return cur.fetchone()


def search_entities(conn: sqlite3.Connection, query: str, fts: bool, limit: int = 25) -> List[sqlite3.Row]:
    q = query.strip()
    if not q:
        cur = conn.execute("SELECT * FROM entities ORDER BY updated_at DESC LIMIT ?", (limit,))
        return cur.fetchall()

    if fts:
        try:
            # Use FTS5 match; match against name, description, tags
            cur = conn.execute(
                "SELECT e.* FROM entities_fts f JOIN entities e ON f.rowid = e.id WHERE entities_fts MATCH ? LIMIT ?",
                (q, limit),
            )
            rows = cur.fetchall()
            if rows:
                return rows
        except Exception:
            # fallback to LIKE
            pass

    # Fallback: simple LIKE matching across name, description, tags
    like_q = f"%{q}%"
    cur = conn.execute(
        "SELECT * FROM entities WHERE name LIKE ? OR description LIKE ? OR tags LIKE ? ORDER BY updated_at DESC LIMIT ?",
        (like_q, like_q, like_q, limit),
    )
    return cur.fetchall()


def rank_candidates(query: str, rows: List[sqlite3.Row], top_n: int = 10) -> List[Tuple[sqlite3.Row, float]]:
    choices = []
    for r in rows:
        text = ' '.join(filter(None, [r['name'] or '', r['description'] or '']))
        choices.append((r, text))

    scored: List[Tuple[sqlite3.Row, float]] = []
    if HAVE_RAPIDFUZZ:
        texts = [t for (_, t) in choices]
        # use process.extract to score quickly
        results = rf_process.extract(query, texts, scorer=rf_fuzz.WRatio, limit=len(texts))
        # results: list of tuples (match, score, index)
        # build mapping index -> score
        idx_to_score = {res[2]: res[1] for res in results}
        for idx, (r, _) in enumerate(choices):
            scored.append((r, float(idx_to_score.get(idx, 0))))
    else:
        for r, text in choices:
            name_score = difflib.SequenceMatcher(None, query.lower(), (r['name'] or '').lower()).ratio()
            desc_score = difflib.SequenceMatcher(None, query.lower(), (r['description'] or '').lower()).ratio()
            score = max(name_score * 1.2, desc_score)
            scored.append((r, float(score * 100)))

    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:top_n]


def format_row_short(r: sqlite3.Row) -> str:
    tid = r['id']
    t = r['type'] or ''
    name = r['name'] or ''
    desc = (r['description'] or '').split('\n', 1)[0]
    return f"[{tid}] ({t}) {name} — {desc}"


def prompt(prompt_text: str) -> str:
    try:
        return input(prompt_text)
    except EOFError:
        # Treat EOF as exit
        print()
        raise


def prompt_fields(existing: Optional[sqlite3.Row] = None) -> Dict[str, Any]:
    # Guided prompts for type, name, description, tags, metadata
    out: Dict[str, Any] = {}
    def get(field, label, default=''):
        curv = existing[field] if existing is not None and field in existing.keys() else default
        s = prompt(f"{label} [{curv}]: ")
        return s.strip() or curv

    out['type'] = get('type', 'Type (person/website/feature/etc)')
    out['name'] = get('name', 'Name')
    out['slug'] = slugify(out['name'])
    out['description'] = get('description', 'Description')
    tags = get('tags', 'Tags (comma-separated)')
    out['tags'] = tags

    # Metadata guided entry
    existing_meta = {}
    if existing is not None:
        try:
            existing_meta = json.loads(existing['metadata'] or '{}')
        except Exception:
            existing_meta = {}

    meta = dict(existing_meta)
    print("Enter metadata as key/value pairs. Leave key blank to finish. Type ':raw' to edit raw JSON.")
    while True:
        k = prompt('meta key: ').strip()
        if not k:
            break
        if k == ':raw':
            cur_text = json.dumps(meta, indent=2)
            print("Enter raw JSON (single line or multi-line). End with an empty line on its own.")
            lines = []
            while True:
                try:
                    l = input()
                except EOFError:
                    break
                if l == '':
                    break
                lines.append(l)
            txt = '\n'.join(lines).strip()
            try:
                meta = json.loads(txt)
            except Exception as e:
                print(f"Invalid JSON: {e}")
            break
        v = prompt(f'value for {k}: ')
        meta[k] = v

    out['metadata'] = meta
    return out


def view_entity(conn: sqlite3.Connection, entity_id: int) -> None:
    r = fetch_entity(conn, entity_id)
    if not r:
        print('Not found')
        return
    print('\n' + '=' * 60)
    print(f"ID: {r['id']}")
    print(f"Type: {r['type']}")
    print(f"Name: {r['name']}")
    print(f"Slug: {r['slug']}")
    print('Description:')
    print(textwrap.indent((r['description'] or '').strip(), '  '))
    print(f"Tags: {r['tags']}")
    try:
        meta = json.loads(r['metadata'] or '{}')
        print('Metadata:')
        print(textwrap.indent(json.dumps(meta, indent=2), '  '))
    except Exception:
        print('Metadata: (invalid JSON)')
        print(r['metadata'])
    print(f"Created: {r['created_at']}")
    print(f"Updated: {r['updated_at']}")
    print('=' * 60 + '\n')


def fuzzy_select_loop(conn: sqlite3.Connection, scored: List[Tuple[sqlite3.Row, float]]) -> Optional[int]:
    if not scored:
        return None
    for idx, (r, score) in enumerate(scored, start=1):
        print(f"{idx}. {format_row_short(r)} (score={score:.2f})")
    print('a) Add new entity')
    print('s) Search again')
    print('q) Quit')
    while True:
        cmd = prompt('Choose number to open, or command: ').strip()
        if not cmd:
            continue
        if cmd.lower() in ('a', 'add'):
            return -1
        if cmd.lower() in ('s', 'search'):
            return None
        if cmd.lower() in ('q',) or cmd.lower() in EXIT_COMMANDS:
            raise EOFError
        if cmd.isdigit():
            n = int(cmd)
            if 1 <= n <= len(scored):
                return scored[n-1][0]['id']
        print('Invalid choice')


def main_loop(conn: sqlite3.Connection, features: dict) -> None:
    fts = features.get('fts', False)

    def cleanup_and_exit():
        try:
            conn.close()
        except Exception:
            pass
        print('Goodbye')
        sys.exit(0)

    def sigint_handler(signum, frame):
        try:
            ans = input('\nReceived interrupt. Exit? [y/N]: ').strip().lower()
        except EOFError:
            ans = 'n'
        if ans == 'y':
            cleanup_and_exit()

    signal.signal(signal.SIGINT, sigint_handler)

    while True:
        try:
            q = prompt('\nSearch> ').strip()
        except EOFError:
            # treat EOF as exit
            cleanup_and_exit()

        if not q:
            print('Type a search term, or `add` to create a new entity, or `quit` to exit.')
            continue

        if q.lower() in EXIT_COMMANDS:
            cleanup_and_exit()

        if q.lower() in ('add', 'new'):
            fields = prompt_fields(None)
            eid = add_entity(conn, fields)
            print(f'Created entity {eid}')
            continue

        rows = search_entities(conn, q, fts=fts, limit=50)
        if not rows:
            yn = prompt('No matches found. Create new entity with this name? [Y/n]: ').strip().lower()
            if yn in ('', 'y', 'yes'):
                fields = prompt_fields({'name': q}) if False else prompt_fields(None)
                # prefill name
                fields['name'] = fields.get('name') or q
                eid = add_entity(conn, fields)
                print(f'Created entity {eid}')
            continue

        scored = rank_candidates(q, rows, top_n=20)
        sel = fuzzy_select_loop(conn, scored)
        if sel is None:
            continue
        if sel == -1:
            fields = prompt_fields(None)
            eid = add_entity(conn, fields)
            print(f'Created entity {eid}')
            continue

        # show selected entity
        try:
            view_entity(conn, sel)
        except Exception as e:
            print(f'Error fetching entity: {e}')
            continue

        # actions: edit, delete, back
        while True:
            act = prompt('[v]iew [e]dit [d]elete [b]ack: ').strip().lower()
            if act in ('v', 'view', ''):
                view_entity(conn, sel)
            elif act in ('e', 'edit'):
                existing = fetch_entity(conn, sel)
                upd = prompt_fields(existing)
                update_entity(conn, sel, upd)
                print('Updated.')
            elif act in ('d', 'delete'):
                yn = prompt('Confirm delete? This cannot be undone. [y/N]: ').strip().lower()
                if yn == 'y':
                    delete_entity(conn, sel)
                    print('Deleted.')
                    break
            elif act in ('b', 'back'):
                break
            elif act in EXIT_COMMANDS:
                cleanup_and_exit()
            else:
                print('Unknown action')


if __name__ == '__main__':
    conn = open_conn(DB_PATH)
    features = init_db(conn)
    print('Entity Hoarder')
    print(f"DB: {DB_PATH}")
    if HAVE_RAPIDFUZZ:
        print('Using rapidfuzz for fuzzy matching')
    else:
        print('rapidfuzz not found; falling back to difflib (pip install rapidfuzz to improve matching)')
    if features.get('fts'):
        print('FTS5 support: enabled')
    else:
        print('FTS5 support: not available; using LIKE fallback')
    try:
        main_loop(conn, features)
    except EOFError:
        try:
            conn.close()
        except Exception:
            pass
        print('Exiting')
