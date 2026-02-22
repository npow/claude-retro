"""Populate sessions table from raw_entries."""

from .db import get_conn


def build_sessions():
    """Aggregate raw_entries into session-level rows.

    Uses BEGIN/COMMIT so the DELETE only takes effect if the INSERT succeeds,
    preventing data loss on query errors.
    """
    conn = get_conn()

    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("DELETE FROM sessions")

        conn.execute("""
            INSERT INTO sessions (
                session_id, project_name, started_at, ended_at, duration_seconds,
                user_prompt_count, assistant_msg_count, tool_use_count, tool_error_count,
                turn_count, first_prompt
            )
            SELECT
                agg.session_id,
                agg.project_name,
                agg.started_at,
                agg.ended_at,
                agg.duration_seconds,
                agg.user_prompt_count,
                agg.assistant_msg_count,
                agg.tool_use_count,
                agg.tool_error_count,
                agg.turn_count,
                fp.user_text as first_prompt
            FROM (
                SELECT
                    session_id,
                    mode(project_name) as project_name,
                    MIN(timestamp_utc) as started_at,
                    MAX(timestamp_utc) as ended_at,
                    EXTRACT(EPOCH FROM (MAX(timestamp_utc) - MIN(timestamp_utc)))::INTEGER as duration_seconds,
                    COUNT(*) FILTER (WHERE entry_type = 'user' AND NOT is_tool_result AND user_text_length > 0) as user_prompt_count,
                    COUNT(*) FILTER (WHERE entry_type = 'assistant') as assistant_msg_count,
                    COALESCE(SUM(len(tool_names)), 0)::INTEGER as tool_use_count,
                    COUNT(*) FILTER (WHERE tool_result_error = TRUE) as tool_error_count,
                    COUNT(*) FILTER (WHERE entry_type = 'system' AND system_subtype = 'turn_duration') as turn_count
                FROM raw_entries
                WHERE session_id IS NOT NULL
                GROUP BY session_id
                HAVING COUNT(*) >= 2
            ) agg
            LEFT JOIN (
                SELECT DISTINCT ON (session_id) session_id, user_text
                FROM raw_entries
                WHERE entry_type = 'user' AND NOT is_tool_result AND user_text_length > 0
                ORDER BY session_id, timestamp_utc ASC
            ) fp ON agg.session_id = fp.session_id
        """)

        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    count = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
    return count


def build_tool_usage():
    """Aggregate tool usage per session."""
    conn = get_conn()

    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("DELETE FROM session_tool_usage")

        conn.execute("""
            INSERT INTO session_tool_usage (session_id, tool_name, use_count, error_count)
            SELECT session_id, tool_name, COUNT(*) as use_count, 0 as error_count
            FROM (
                SELECT session_id, UNNEST(tool_names) as tool_name
                FROM raw_entries
                WHERE session_id IS NOT NULL
                  AND len(tool_names) > 0
                  AND entry_type = 'assistant'
            )
            GROUP BY session_id, tool_name
        """)

        # Update error counts from tool_result entries
        conn.execute("""
            UPDATE session_tool_usage stu
            SET error_count = COALESCE((
                SELECT COUNT(*)
                FROM raw_entries re
                WHERE re.session_id = stu.session_id
                  AND re.tool_result_error = TRUE
                  AND re.entry_type = 'user'
            ), 0)
            WHERE tool_name IN (
                SELECT DISTINCT tool_name FROM session_tool_usage
                WHERE session_id = stu.session_id
                LIMIT 1
            )
        """)

        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    count = conn.execute("SELECT COUNT(*) FROM session_tool_usage").fetchone()[0]
    return count
