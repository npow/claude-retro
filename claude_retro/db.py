"""SQLite database — thin wrapper around agenttrace.db.

All connection management, WAL mode, and base schema (raw_entries, sessions,
session_features, progress_entries, etc.) live in agenttrace.db.

This module re-exports those functions and applies the additional
claude-retro-specific schema extensions (extra columns on synthesis and
session_judgments) on top of the base schema.

DB_PATH is determined by the CLAUDE_RETRO_DB env var (defaulted in config.py
before any agenttrace import can read it).
"""

import sqlite3

# config.py must be imported first — it sets CLAUDE_RETRO_DB default so that
# agenttrace.config reads the correct path when it first imports.
from .config import DB_PATH  # noqa: F401 — re-exported for __main__.py reset command

from agenttrace.db import (
    get_writer as _at_get_writer,
    get_reader,
    get_conn,
    execute_write,
    execute_read,
    rebuild_fts_index,
)

__all__ = [
    "DB_PATH",
    "get_writer",
    "get_reader",
    "get_conn",
    "execute_write",
    "execute_read",
    "rebuild_fts_index",
]


def _migrate_add_columns(conn: sqlite3.Connection, table: str, columns: list):
    """Add columns to table if they don't exist (safe migration)."""
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for col_name, col_type in columns:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")


def _init_extra_schema(conn: sqlite3.Connection):
    """Apply claude-retro-specific schema extensions on top of the agenttrace base."""
    # synthesis has extra columns in claude-retro not present in agenttrace
    _migrate_add_columns(conn, "synthesis", [
        ("workflow_prompts", "TEXT"),
        ("features_to_try", "TEXT"),
    ])
    # session_judgments has extra columns in claude-retro
    _migrate_add_columns(conn, "session_judgments", [
        ("friction_categories", "TEXT"),
        ("estimated_cost_usd", "REAL"),
    ])
    conn.commit()


_extra_initialized = False


def get_writer() -> sqlite3.Connection:
    """Get the serialized writer connection with full schema.

    Calls agenttrace's get_writer (which creates the base schema), then
    applies claude-retro-specific schema extensions once per process.
    """
    global _extra_initialized
    conn = _at_get_writer()
    if not _extra_initialized:
        _init_extra_schema(conn)
        _extra_initialized = True
    return conn
