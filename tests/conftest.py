"""Shared fixtures for tests — uses an in-memory DuckDB so nothing touches the real DB."""

import pytest

from claude_retro import db


@pytest.fixture(autouse=True)
def isolated_db(monkeypatch, tmp_path):
    """Redirect all DB access to a temp file for test isolation."""
    test_db = tmp_path / "test.duckdb"
    monkeypatch.setattr(db, "DB_PATH", test_db)
    # Clear thread-local connection cache so each test gets a fresh connection
    if hasattr(db._local, "conn"):
        del db._local.conn
    yield test_db
    if hasattr(db._local, "conn"):
        db._local.conn.close()
        del db._local.conn


@pytest.fixture
def conn(isolated_db):
    """Return a DuckDB connection with schema initialized."""
    return db.get_conn()


@pytest.fixture
def seed_entries(conn):
    """Insert minimal raw_entries for two sessions — one good, one single-entry."""
    from datetime import datetime, timedelta

    base = datetime(2026, 1, 15, 10, 0, 0)

    entries = [
        # Session A: 3 entries (user prompt, assistant tools, user prompt)
        (
            "e1",
            "sess-a",
            "proj-x",
            "user",
            base,
            None,
            False,
            "Implement auth flow",
            20,
            False,
            False,
            None,
            [],
            [],
            "",
            0,
            0,
            0,
            None,
            0,
            None,
            None,
        ),
        (
            "e2",
            "sess-a",
            "proj-x",
            "assistant",
            base + timedelta(seconds=10),
            None,
            False,
            None,
            0,
            False,
            False,
            "claude-sonnet",
            [],
            ["Edit", "Write"],
            "done",
            100,
            500,
            200,
            None,
            0,
            None,
            None,
        ),
        (
            "e3",
            "sess-a",
            "proj-x",
            "user",
            base + timedelta(seconds=30),
            None,
            False,
            "Actually use JWT not sessions",
            30,
            False,
            False,
            None,
            [],
            [],
            "",
            0,
            0,
            0,
            None,
            0,
            None,
            None,
        ),
        # Session B: 4 entries (user, assistant with error, tool result error, user correction)
        (
            "e4",
            "sess-b",
            "proj-y",
            "user",
            base + timedelta(minutes=5),
            None,
            False,
            "Fix the deploy bug",
            18,
            False,
            False,
            None,
            [],
            [],
            "",
            0,
            0,
            0,
            None,
            0,
            None,
            None,
        ),
        (
            "e5",
            "sess-b",
            "proj-y",
            "assistant",
            base + timedelta(minutes=5, seconds=10),
            None,
            False,
            None,
            0,
            False,
            False,
            "claude-sonnet",
            [],
            ["Bash", "Read"],
            "running",
            50,
            300,
            100,
            None,
            0,
            None,
            None,
        ),
        (
            "e6",
            "sess-b",
            "proj-y",
            "user",
            base + timedelta(minutes=5, seconds=15),
            None,
            False,
            None,
            0,
            True,
            True,
            None,
            [],
            [],
            "",
            0,
            0,
            0,
            None,
            0,
            None,
            None,
        ),
        (
            "e7",
            "sess-b",
            "proj-y",
            "user",
            base + timedelta(minutes=5, seconds=30),
            None,
            False,
            "Wrong file, try src/deploy.py",
            28,
            False,
            False,
            None,
            [],
            [],
            "",
            0,
            0,
            0,
            None,
            0,
            None,
            None,
        ),
        # Session C: single entry (should be excluded from sessions, HAVING COUNT >= 2)
        (
            "e8",
            "sess-c",
            "proj-x",
            "user",
            base + timedelta(minutes=10),
            None,
            False,
            "Hello",
            5,
            False,
            False,
            None,
            [],
            [],
            "",
            0,
            0,
            0,
            None,
            0,
            None,
            None,
        ),
    ]

    for e in entries:
        conn.execute(
            """
            INSERT INTO raw_entries (
                entry_id, session_id, project_name, entry_type, timestamp_utc,
                parent_uuid, is_sidechain, user_text, user_text_length,
                is_tool_result, tool_result_error, model, content_types,
                tool_names, text_content, text_length, input_tokens,
                output_tokens, system_subtype, duration_ms, git_branch, cwd
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            list(e),
        )

    return conn
