"""DuckDB connection and schema initialization."""

import subprocess
import sys
import threading
import time

import duckdb
from .config import DB_PATH

_local = threading.local()

# Maximum seconds to wait for the DuckDB lock before giving up
LOCK_TIMEOUT = 30


def _find_lock_holder() -> str:
    """Return a human-readable string describing what holds the DB lock."""
    try:
        result = subprocess.run(
            ["lsof", str(DB_PATH)],
            capture_output=True, text=True, timeout=5,
        )
        for line in result.stdout.strip().splitlines()[1:]:  # skip header
            parts = line.split()
            if len(parts) >= 2:
                return f"{parts[0]} (PID {parts[1]})"
    except Exception:
        pass
    return "unknown process"


def get_conn() -> duckdb.DuckDBPyConnection:
    if not hasattr(_local, 'conn'):
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + LOCK_TIMEOUT
        last_err = None
        attempt = 0
        while True:
            try:
                _local.conn = duckdb.connect(str(DB_PATH))
                break
            except duckdb.IOException as e:
                last_err = e
                if time.monotonic() >= deadline:
                    holder = _find_lock_holder()
                    raise duckdb.IOException(
                        f"Could not acquire DuckDB lock after {LOCK_TIMEOUT}s. "
                        f"Lock held by: {holder}. "
                        f"Kill that process or wait for it to finish.\n"
                        f"Original error: {e}"
                    ) from None
                attempt += 1
                wait = min(1.0, 0.2 * attempt)  # backoff: 0.2, 0.4, 0.6, 0.8, 1.0, 1.0, ...
                print(
                    f"[db] Waiting for DuckDB lock ({_find_lock_holder()})... "
                    f"retry in {wait:.1f}s",
                    file=sys.stderr,
                )
                time.sleep(wait)

        init_schema(_local.conn)
    return _local.conn


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
