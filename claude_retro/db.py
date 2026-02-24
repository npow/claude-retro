"""DuckDB connection and schema initialization with separate read/write pools."""

import subprocess
import sys
import threading
import time

import duckdb
from .config import DB_PATH

_local = threading.local()
_writer_lock = threading.Lock()
_writer_conn = None

# Maximum seconds to wait for the DuckDB lock before giving up
LOCK_TIMEOUT = 30


def _find_lock_holder() -> str:
    """Return a human-readable string describing what holds the DB lock."""
    try:
        result = subprocess.run(
            ["lsof", str(DB_PATH)],
            capture_output=True,
            text=True,
            timeout=5,
        )
        for line in result.stdout.strip().splitlines()[1:]:  # skip header
            parts = line.split()
            if len(parts) >= 2:
                return f"{parts[0]} (PID {parts[1]})"
    except Exception:
        pass
    return "unknown process"


def get_writer() -> duckdb.DuckDBPyConnection:
    """Get the serialized writer connection for writes/DDL.

    All write operations should use this connection with _writer_lock held.
    Only one writer exists per process.
    """
    global _writer_conn
    if _writer_conn is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _writer_conn = _connect_with_retry()
        init_schema(_writer_conn)
    return _writer_conn


def get_reader() -> duckdb.DuckDBPyConnection:
    """Get a read-only connection for this thread.

    Each thread gets its own reader connection for concurrent reads.
    Readers do not block each other or the writer.
    """
    if not hasattr(_local, "reader"):
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _local.reader = _connect_with_retry()
    return _local.reader


def get_conn() -> duckdb.DuckDBPyConnection:
    """Legacy API: returns reader connection by default.

    Use get_writer() for writes or get_reader() explicitly for reads.
    """
    return get_reader()


def _connect_with_retry() -> duckdb.DuckDBPyConnection:
    """Connect to DuckDB with exponential backoff on lock contention."""
    deadline = time.monotonic() + LOCK_TIMEOUT
    attempt = 0
    while True:
        try:
            return duckdb.connect(str(DB_PATH))
        except duckdb.IOException as e:
            if time.monotonic() >= deadline:
                holder = _find_lock_holder()
                raise duckdb.IOException(
                    f"Could not acquire DuckDB lock after {LOCK_TIMEOUT}s. "
                    f"Lock held by: {holder}. "
                    f"Kill that process or wait for it to finish.\n"
                    f"Original error: {e}"
                ) from None
            attempt += 1
            wait = min(
                1.0, 0.2 * attempt
            )  # backoff: 0.2, 0.4, 0.6, 0.8, 1.0, 1.0, ...
            print(
                f"[db] Waiting for DuckDB lock ({_find_lock_holder()})... "
                f"retry in {wait:.1f}s",
                file=sys.stderr,
            )
            time.sleep(wait)


def init_schema(conn: duckdb.DuckDBPyConnection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_entries (
            entry_id VARCHAR PRIMARY KEY,
            session_id VARCHAR,
            project_name VARCHAR,
            entry_type VARCHAR,
            timestamp_utc TIMESTAMP,
            parent_uuid VARCHAR,
            is_sidechain BOOLEAN DEFAULT FALSE,
            user_text TEXT,
            user_text_length INTEGER DEFAULT 0,
            is_tool_result BOOLEAN DEFAULT FALSE,
            tool_result_error BOOLEAN DEFAULT FALSE,
            model VARCHAR,
            content_types VARCHAR[],
            tool_names VARCHAR[],
            text_content TEXT,
            text_length INTEGER DEFAULT 0,
            input_tokens INTEGER DEFAULT 0,
            output_tokens INTEGER DEFAULT 0,
            system_subtype VARCHAR,
            duration_ms INTEGER DEFAULT 0,
            git_branch VARCHAR,
            cwd VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_id VARCHAR PRIMARY KEY,
            project_name VARCHAR,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            duration_seconds INTEGER DEFAULT 0,
            user_prompt_count INTEGER DEFAULT 0,
            assistant_msg_count INTEGER DEFAULT 0,
            tool_use_count INTEGER DEFAULT 0,
            tool_error_count INTEGER DEFAULT 0,
            turn_count INTEGER DEFAULT 0,
            first_prompt TEXT,
            intent VARCHAR DEFAULT 'unknown',
            trajectory VARCHAR DEFAULT 'unknown',
            convergence_score DOUBLE DEFAULT 0.0,
            drift_score DOUBLE DEFAULT 0.0,
            thrash_score DOUBLE DEFAULT 0.0
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS session_features (
            session_id VARCHAR PRIMARY KEY,
            avg_prompt_length DOUBLE DEFAULT 0,
            prompt_length_trend DOUBLE DEFAULT 0,
            max_prompt_length INTEGER DEFAULT 0,
            avg_response_length DOUBLE DEFAULT 0,
            response_length_trend DOUBLE DEFAULT 0,
            response_length_cv DOUBLE DEFAULT 0,
            total_input_tokens BIGINT DEFAULT 0,
            total_output_tokens BIGINT DEFAULT 0,
            edit_write_ratio DOUBLE DEFAULT 0,
            read_grep_ratio DOUBLE DEFAULT 0,
            bash_ratio DOUBLE DEFAULT 0,
            task_ratio DOUBLE DEFAULT 0,
            web_ratio DOUBLE DEFAULT 0,
            unique_tools_used INTEGER DEFAULT 0,
            avg_turn_duration_ms DOUBLE DEFAULT 0,
            hour_of_day INTEGER DEFAULT 0,
            day_of_week INTEGER DEFAULT 0,
            correction_count INTEGER DEFAULT 0,
            correction_rate DOUBLE DEFAULT 0,
            rephrasing_count INTEGER DEFAULT 0,
            decision_marker_count INTEGER DEFAULT 0,
            topic_keyword_entropy DOUBLE DEFAULT 0,
            sidechain_count INTEGER DEFAULT 0,
            sidechain_ratio DOUBLE DEFAULT 0,
            abandoned BOOLEAN DEFAULT FALSE,
            has_pr_link BOOLEAN DEFAULT FALSE,
            branch_switch_count INTEGER DEFAULT 0,
            prompt_length_oscillation DOUBLE DEFAULT 0,
            api_error_count INTEGER DEFAULT 0
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS session_tool_usage (
            session_id VARCHAR,
            tool_name VARCHAR,
            use_count INTEGER DEFAULT 0,
            error_count INTEGER DEFAULT 0,
            PRIMARY KEY (session_id, tool_name)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS baselines (
            id INTEGER PRIMARY KEY,
            window_size INTEGER,
            computed_at TIMESTAMP,
            avg_convergence DOUBLE,
            avg_drift DOUBLE,
            avg_thrash DOUBLE,
            avg_duration DOUBLE,
            avg_turns DOUBLE,
            avg_tool_errors DOUBLE,
            avg_correction_rate DOUBLE,
            session_count INTEGER
        )
    """)

    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS prescription_seq START 1
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS prescriptions (
            id INTEGER DEFAULT nextval('prescription_seq') PRIMARY KEY,
            category VARCHAR,
            title VARCHAR,
            description TEXT,
            evidence TEXT,
            confidence DOUBLE,
            dismissed BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS session_judgments (
            session_id VARCHAR PRIMARY KEY,
            outcome VARCHAR,
            outcome_confidence DOUBLE DEFAULT 0.0,
            outcome_reasoning TEXT,
            prompt_clarity DOUBLE DEFAULT 0.0,
            prompt_completeness DOUBLE DEFAULT 0.0,
            prompt_missing TEXT,
            prompt_summary TEXT,
            trajectory_summary TEXT,
            underspecified_parts TEXT,
            misalignment_count INTEGER DEFAULT 0,
            misalignments TEXT,
            correction_count INTEGER DEFAULT 0,
            corrections TEXT,
            productive_turns INTEGER DEFAULT 0,
            waste_turns INTEGER DEFAULT 0,
            productivity_ratio DOUBLE DEFAULT 0.0,
            waste_breakdown TEXT,
            raw_analysis_1 TEXT,
            raw_analysis_2 TEXT,
            judged_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_log (
            file_path VARCHAR PRIMARY KEY,
            mtime DOUBLE,
            entry_count INTEGER DEFAULT 0,
            ingested_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS skip_cache (
            file_path VARCHAR PRIMARY KEY,
            mtime DOUBLE,
            error_type VARCHAR,
            error_message TEXT,
            skip_until TIMESTAMP,
            cached_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS session_skills (
            session_id VARCHAR PRIMARY KEY,
            d1_level INTEGER DEFAULT 0,
            d1_opportunity INTEGER DEFAULT 0,
            d2_level INTEGER DEFAULT 0,
            d2_opportunity INTEGER DEFAULT 0,
            d3_level INTEGER DEFAULT 0,
            d3_opportunity INTEGER DEFAULT 0,
            d4_level INTEGER DEFAULT 0,
            d4_opportunity INTEGER DEFAULT 0,
            d5_level INTEGER DEFAULT 0,
            d5_opportunity INTEGER DEFAULT 0,
            d6_level INTEGER DEFAULT 0,
            d6_opportunity INTEGER DEFAULT 0,
            d7_level INTEGER DEFAULT 0,
            d7_opportunity INTEGER DEFAULT 0,
            d8_level INTEGER DEFAULT 0,
            d8_opportunity INTEGER DEFAULT 0,
            d9_level INTEGER DEFAULT 0,
            d9_opportunity INTEGER DEFAULT 0,
            d10_level INTEGER DEFAULT 0,
            d10_opportunity INTEGER DEFAULT 0,
            detection_confidence DOUBLE DEFAULT 0.0,
            assessed_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS skill_profile (
            id INTEGER PRIMARY KEY DEFAULT 1,
            d1_score DOUBLE DEFAULT 0.0,
            d2_score DOUBLE DEFAULT 0.0,
            d3_score DOUBLE DEFAULT 0.0,
            d4_score DOUBLE DEFAULT 0.0,
            d5_score DOUBLE DEFAULT 0.0,
            d6_score DOUBLE DEFAULT 0.0,
            d7_score DOUBLE DEFAULT 0.0,
            d8_score DOUBLE DEFAULT 0.0,
            d9_score DOUBLE DEFAULT 0.0,
            d10_score DOUBLE DEFAULT 0.0,
            gap_1 VARCHAR,
            gap_2 VARCHAR,
            gap_3 VARCHAR,
            session_count INTEGER DEFAULT 0,
            computed_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS skill_nudge_seq START 1
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS skill_nudges (
            id INTEGER DEFAULT nextval('skill_nudge_seq') PRIMARY KEY,
            dimension VARCHAR,
            current_level INTEGER DEFAULT 0,
            target_level INTEGER DEFAULT 0,
            nudge_text TEXT,
            evidence TEXT,
            frequency INTEGER DEFAULT 1,
            dismissed BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT current_timestamp
        )
    """)


def execute_write(sql: str, params=None):
    """Execute a write query with proper locking.

    Use this for INSERT, UPDATE, DELETE, or DDL statements.
    """
    with _writer_lock:
        writer = get_writer()
        if params:
            return writer.execute(sql, params)
        return writer.execute(sql)


def execute_read(sql: str, params=None):
    """Execute a read query using a reader connection.

    Use this for SELECT queries that don't modify data.
    """
    reader = get_reader()
    if params:
        return reader.execute(sql, params)
    return reader.execute(sql)
