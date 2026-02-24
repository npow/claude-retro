"""Flask REST API."""

import json
import sys
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory, Response

from .db import get_conn
from .events import get_broadcaster, format_sse
from .version import get_version_info
from .export import generate_export_html

if getattr(sys, "frozen", False):
    _static = str(Path(sys._MEIPASS) / "static")
else:
    _static = str(Path(__file__).parent / "static")

app = Flask(__name__, static_folder=_static)

# Set by app.py / __main__.py so /api/status can read worker state
_worker = None


def set_worker(worker):
    global _worker
    _worker = worker


@app.route("/api/status")
def api_status():
    if _worker is None:
        return jsonify({"state": "idle", "step": "", "ready": True})
    return jsonify(_worker.status)


@app.route("/api/events")
def api_events():
    """Server-Sent Events (SSE) stream for real-time updates.

    Streams status updates, progress events, and completion notifications
    as they happen. Replaces the need for polling /api/status.
    """

    def generate():
        broadcaster = get_broadcaster()
        q = broadcaster.subscribe()
        try:
            # Send initial status
            if _worker:
                yield format_sse("status", _worker.status)

            # Stream events as they arrive
            while True:
                try:
                    event = q.get(timeout=30)  # 30s keepalive
                    yield format_sse(event["event"], event["data"])
                except Exception:
                    # Timeout - send keepalive comment
                    yield ": keepalive\n\n"
        finally:
            broadcaster.unsubscribe(q)

    return Response(generate(), mimetype="text/event-stream")


def _row_to_dict(row, columns):
    return {col: _serialize(val) for col, val in zip(columns, row)}


def _serialize(val):
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return val


@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/version")
def api_version():
    return jsonify(get_version_info())


@app.route("/api/export")
def api_export():
    """Export verdict and prescriptions as standalone HTML."""
    from datetime import datetime

    html = generate_export_html()
    filename = f"claude-retro-{datetime.now().strftime('%Y%m%d')}.html"

    return Response(
        html,
        mimetype="text/html",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/api/overview")
def api_overview():
    conn = get_conn()

    stats = conn.execute("""
        SELECT
            COUNT(*) as total_sessions,
            AVG(convergence_score) as avg_convergence,
            AVG(drift_score) as avg_drift,
            AVG(thrash_score) as avg_thrash,
            SUM(duration_seconds) / 3600.0 as total_hours,
            COUNT(DISTINCT project_name) as total_projects,
            AVG(turn_count) as avg_turns
        FROM sessions
    """).fetchone()

    trajectory_dist = conn.execute("""
        SELECT trajectory, COUNT(*) as count
        FROM sessions
        GROUP BY trajectory
        ORDER BY count DESC
    """).fetchall()

    baselines = conn.execute("""
        SELECT * FROM baselines ORDER BY window_size
    """).fetchall()
    baseline_cols = [d[0] for d in conn.description]

    return jsonify(
        {
            "total_sessions": stats[0],
            "avg_convergence": round(stats[1] or 0, 3),
            "avg_drift": round(stats[2] or 0, 3),
            "avg_thrash": round(stats[3] or 0, 3),
            "total_hours": round(stats[4] or 0, 1),
            "total_projects": stats[5],
            "avg_turns": round(stats[6] or 0, 1),
            "trajectory_distribution": {t: c for t, c in trajectory_dist},
            "baselines": [_row_to_dict(b, baseline_cols) for b in baselines],
        }
    )


@app.route("/api/sessions")
def api_sessions():
    conn = get_conn()
    project = request.args.get("project")
    intent = request.args.get("intent")
    trajectory = request.args.get("trajectory")
    search = request.args.get("search")
    limit = int(request.args.get("limit", 50))
    offset = int(request.args.get("offset", 0))
    sort = request.args.get("sort", "started_at DESC")

    # Whitelist sort columns
    allowed_sorts = {
        "started_at": "s.started_at",
        "convergence": "s.convergence_score",
        "drift": "s.drift_score",
        "thrash": "s.thrash_score",
        "duration": "s.duration_seconds",
        "turns": "s.turn_count",
        "misalignments": "COALESCE(j.misalignment_count, 0)",
        "productivity": "COALESCE(j.productivity_ratio, 0)",
    }
    sort_parts = sort.split()
    sort_col = allowed_sorts.get(sort_parts[0], "s.started_at")
    sort_dir = (
        "DESC" if len(sort_parts) < 2 or sort_parts[1].upper() == "DESC" else "ASC"
    )

    conditions = []
    params = []

    if project:
        conditions.append("s.project_name = ?")
        params.append(project)
    if intent:
        conditions.append("s.intent = ?")
        params.append(intent)
    if trajectory:
        conditions.append("s.trajectory = ?")
        params.append(trajectory)
    if search:
        conditions.append("(s.first_prompt ILIKE ? OR s.session_id ILIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    total = conn.execute(f"SELECT COUNT(*) FROM sessions s {where}", params).fetchone()[
        0
    ]

    rows = conn.execute(
        f"""
        SELECT s.session_id, s.project_name, s.started_at, s.ended_at, s.duration_seconds,
               s.user_prompt_count, s.assistant_msg_count, s.tool_use_count, s.tool_error_count,
               s.turn_count, s.first_prompt, s.intent, s.trajectory,
               s.convergence_score, s.drift_score, s.thrash_score,
               j.outcome, j.misalignment_count, j.productivity_ratio
        FROM sessions s
        LEFT JOIN session_judgments j ON s.session_id = j.session_id
        {where}
        ORDER BY {sort_col} {sort_dir}
        LIMIT ? OFFSET ?
    """,
        params + [limit, offset],
    ).fetchall()

    cols = [
        "session_id",
        "project_name",
        "started_at",
        "ended_at",
        "duration_seconds",
        "user_prompt_count",
        "assistant_msg_count",
        "tool_use_count",
        "tool_error_count",
        "turn_count",
        "first_prompt",
        "intent",
        "trajectory",
        "convergence_score",
        "drift_score",
        "thrash_score",
        "judgment_outcome",
        "misalignment_count",
        "productivity_ratio",
    ]

    return jsonify(
        {
            "total": total,
            "sessions": [_row_to_dict(r, cols) for r in rows],
        }
    )


@app.route("/api/sessions/<session_id>")
def api_session_detail(session_id):
    conn = get_conn()

    session = conn.execute(
        """
        SELECT * FROM sessions WHERE session_id = ?
    """,
        [session_id],
    ).fetchone()

    if not session:
        return jsonify({"error": "Session not found"}), 404

    session_cols = [d[0] for d in conn.description]

    features = conn.execute(
        """
        SELECT * FROM session_features WHERE session_id = ?
    """,
        [session_id],
    ).fetchone()
    feature_cols = [d[0] for d in conn.description] if features else []

    tools = conn.execute(
        """
        SELECT tool_name, use_count, error_count
        FROM session_tool_usage WHERE session_id = ?
        ORDER BY use_count DESC
    """,
        [session_id],
    ).fetchall()

    judgment = conn.execute(
        """
        SELECT * FROM session_judgments WHERE session_id = ?
    """,
        [session_id],
    ).fetchone()
    judgment_cols = [d[0] for d in conn.description] if judgment else []

    result = {
        "session": _row_to_dict(session, session_cols),
        "features": _row_to_dict(features, feature_cols) if features else {},
        "tools": [
            {"tool_name": t[0], "use_count": t[1], "error_count": t[2]} for t in tools
        ],
    }
    if judgment:
        jd = _row_to_dict(judgment, judgment_cols)
        # Parse JSON string fields for the frontend
        for field in (
            "prompt_missing",
            "underspecified_parts",
            "misalignments",
            "corrections",
            "waste_breakdown",
        ):
            if jd.get(field) and isinstance(jd[field], str):
                try:
                    jd[field] = json.loads(jd[field])
                except (json.JSONDecodeError, ValueError):
                    pass
        result["judgment"] = jd
    else:
        result["judgment"] = None

    return jsonify(result)


@app.route("/api/sessions/<session_id>/timeline")
def api_session_timeline(session_id):
    conn = get_conn()

    entries = conn.execute(
        """
        SELECT entry_id, entry_type, timestamp_utc, user_text_length,
               text_length, tool_names, is_tool_result, tool_result_error,
               system_subtype, duration_ms,
               CASE WHEN user_text_length > 0 THEN LEFT(user_text, 200) ELSE LEFT(text_content, 200) END as preview
        FROM raw_entries
        WHERE session_id = ? AND NOT is_sidechain
        ORDER BY timestamp_utc
    """,
        [session_id],
    ).fetchall()

    cols = [
        "entry_id",
        "entry_type",
        "timestamp_utc",
        "user_text_length",
        "text_length",
        "tool_names",
        "is_tool_result",
        "tool_result_error",
        "system_subtype",
        "duration_ms",
        "preview",
    ]

    return jsonify(
        {
            "timeline": [_row_to_dict(e, cols) for e in entries],
        }
    )


@app.route("/api/intents")
def api_intents():
    conn = get_conn()

    rows = conn.execute("""
        SELECT
            intent,
            COUNT(*) as count,
            AVG(convergence_score) as avg_convergence,
            AVG(drift_score) as avg_drift,
            AVG(thrash_score) as avg_thrash,
            AVG(duration_seconds) as avg_duration,
            AVG(turn_count) as avg_turns
        FROM sessions
        GROUP BY intent
        ORDER BY count DESC
    """).fetchall()

    cols = [
        "intent",
        "count",
        "avg_convergence",
        "avg_drift",
        "avg_thrash",
        "avg_duration",
        "avg_turns",
    ]

    return jsonify(
        {
            "intents": [_row_to_dict(r, cols) for r in rows],
        }
    )


@app.route("/api/trends")
def api_trends():
    conn = get_conn()
    days = int(request.args.get("days", 30))

    rows = conn.execute(
        """
        SELECT
            CAST(started_at AS DATE) as day,
            COUNT(*) as sessions,
            AVG(convergence_score) as avg_convergence,
            AVG(drift_score) as avg_drift,
            AVG(thrash_score) as avg_thrash,
            SUM(duration_seconds) / 3600.0 as hours
        FROM sessions
        WHERE started_at >= current_date - INTERVAL '? days'
        GROUP BY CAST(started_at AS DATE)
        ORDER BY day
    """.replace("?", str(int(days)))
    ).fetchall()

    cols = ["day", "sessions", "avg_convergence", "avg_drift", "avg_thrash", "hours"]

    return jsonify(
        {
            "trends": [_row_to_dict(r, cols) for r in rows],
        }
    )


@app.route("/api/actions")
def api_actions():
    from .prescriptions import generate_actions

    actions = generate_actions()
    return jsonify({"actions": actions})


@app.route("/api/prescriptions")
def api_prescriptions():
    conn = get_conn()

    rows = conn.execute("""
        SELECT id, category, title, description, evidence, confidence, dismissed, created_at
        FROM prescriptions
        WHERE dismissed = FALSE
        ORDER BY confidence DESC
    """).fetchall()

    cols = [
        "id",
        "category",
        "title",
        "description",
        "evidence",
        "confidence",
        "dismissed",
        "created_at",
    ]

    return jsonify(
        {
            "prescriptions": [_row_to_dict(r, cols) for r in rows],
        }
    )


@app.route("/api/prescriptions/<int:pid>/dismiss", methods=["POST"])
def api_dismiss_prescription(pid):
    conn = get_conn()
    conn.execute("UPDATE prescriptions SET dismissed = TRUE WHERE id = ?", [pid])
    return jsonify({"ok": True})


@app.route("/api/tools")
def api_tools():
    conn = get_conn()

    rows = conn.execute("""
        SELECT tool_name, SUM(use_count) as total_uses, SUM(error_count) as total_errors
        FROM session_tool_usage
        GROUP BY tool_name
        ORDER BY total_uses DESC
    """).fetchall()

    return jsonify(
        {
            "tools": [
                {"tool_name": r[0], "total_uses": r[1], "total_errors": r[2]}
                for r in rows
            ],
        }
    )


@app.route("/api/projects")
def api_projects():
    conn = get_conn()

    rows = conn.execute("""
        SELECT
            s.project_name,
            COUNT(*) as session_count,
            AVG(s.convergence_score) as avg_convergence,
            AVG(s.drift_score) as avg_drift,
            AVG(s.thrash_score) as avg_thrash,
            SUM(s.duration_seconds) / 3600.0 as total_hours,
            MAX(s.started_at) as last_active,
            SUM(s.tool_error_count) as total_errors,
            COALESCE(SUM(f.total_input_tokens), 0) as total_input_tokens,
            COALESCE(SUM(f.total_output_tokens), 0) as total_output_tokens,
            AVG(j.productivity_ratio) as avg_productivity,
            SUM(CASE WHEN j.outcome = 'completed' THEN 1.0 ELSE 0.0 END)
                / NULLIF(SUM(CASE WHEN j.outcome IS NOT NULL THEN 1 ELSE 0 END), 0) as completion_rate,
            AVG(j.misalignment_count) as avg_misalignments
        FROM sessions s
        LEFT JOIN session_features f ON s.session_id = f.session_id
        LEFT JOIN session_judgments j ON s.session_id = j.session_id
        GROUP BY s.project_name
        ORDER BY session_count DESC
    """).fetchall()

    cols = [
        "project_name",
        "session_count",
        "avg_convergence",
        "avg_drift",
        "avg_thrash",
        "total_hours",
        "last_active",
        "total_errors",
        "total_input_tokens",
        "total_output_tokens",
        "avg_productivity",
        "completion_rate",
        "avg_misalignments",
    ]

    return jsonify(
        {
            "projects": [_row_to_dict(r, cols) for r in rows],
        }
    )


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """Trigger a full refresh (ingest + LLM judge) in the background.

    Non-blocking — returns immediately. Poll /api/status for progress.
    Accepts optional JSON body: {"concurrency": 12}
    """
    if _worker is None:
        return jsonify({"error": "No background worker available"}), 500

    if _worker.is_busy:
        # Queue it — the worker will pick it up after the current run finishes
        _worker.request_refresh(
            concurrency=max(
                1,
                min(
                    32,
                    int((request.get_json(silent=True) or {}).get("concurrency", 12)),
                ),
            )
        )
        return jsonify({"ok": True, "queued": True, "concurrency": 12})

    body = request.get_json(silent=True) or {}
    concurrency = body.get("concurrency", 12)
    concurrency = max(1, min(32, int(concurrency)))

    _worker.request_refresh(concurrency=concurrency)
    return jsonify({"ok": True, "concurrency": concurrency})


@app.route("/api/judgments/stats")
def api_judgment_stats():
    conn = get_conn()

    total = conn.execute("SELECT COUNT(*) FROM session_judgments").fetchone()[0]
    if total == 0:
        return jsonify({"total_judged": 0})

    outcome_dist = conn.execute("""
        SELECT outcome, COUNT(*) as count
        FROM session_judgments
        GROUP BY outcome ORDER BY count DESC
    """).fetchall()

    avgs = conn.execute("""
        SELECT
            AVG(prompt_clarity) as avg_clarity,
            AVG(prompt_completeness) as avg_completeness,
            AVG(productivity_ratio) as avg_productivity,
            AVG(misalignment_count) as avg_misalignments,
            SUM(CASE WHEN misalignment_count > 0 THEN 1 ELSE 0 END) as sessions_with_misalignment
        FROM session_judgments
    """).fetchone()

    return jsonify(
        {
            "total_judged": total,
            "outcome_distribution": {r[0]: r[1] for r in outcome_dist},
            "avg_clarity": round(avgs[0] or 0, 3),
            "avg_completeness": round(avgs[1] or 0, 3),
            "avg_productivity": round(avgs[2] or 0, 3),
            "avg_misalignments": round(avgs[3] or 0, 2),
            "misalignment_rate": round((avgs[4] or 0) / total, 3) if total else 0,
        }
    )


@app.route("/api/patterns")
def api_patterns():
    conn = get_conn()

    # --- Prompt gap clustering ---
    gap_rows = conn.execute("""
        SELECT prompt_missing FROM session_judgments
        WHERE prompt_missing IS NOT NULL AND prompt_missing != '[]'
    """).fetchall()

    GAP_CATEGORIES = {
        "context": [
            "repo",
            "codebase",
            "file",
            "directory",
            "structure",
            "existing",
            "path",
            "folder",
            "project",
        ],
        "requirements": [
            "expected",
            "behavior",
            "output",
            "format",
            "specific",
            "requirement",
            "result",
            "goal",
        ],
        "constraints": [
            "environment",
            "version",
            "dependency",
            "platform",
            "setup",
            "config",
            "os",
            "runtime",
        ],
        "error_details": [
            "error",
            "message",
            "stack",
            "trace",
            "log",
            "exception",
            "warning",
            "failure",
        ],
        "scope": [
            "which",
            "where",
            "boundary",
            "limit",
            "priority",
            "scope",
            "range",
            "subset",
        ],
    }

    gap_counts = {cat: {"count": 0, "examples": []} for cat in GAP_CATEGORIES}
    total_gap_items = 0
    for (raw,) in gap_rows:
        try:
            items = json.loads(raw) if isinstance(raw, str) else raw
            for item in items:
                text = str(item).lower()
                total_gap_items += 1
                matched = False
                for cat, keywords in GAP_CATEGORIES.items():
                    if any(kw in text for kw in keywords):
                        gap_counts[cat]["count"] += 1
                        if len(gap_counts[cat]["examples"]) < 3:
                            gap_counts[cat]["examples"].append(str(item))
                        matched = True
                        break
                if not matched:
                    # Assign to "other" implicitly by not counting
                    pass
        except (json.JSONDecodeError, TypeError):
            continue

    prompt_gaps = sorted(
        [
            {
                "category": cat,
                "examples": info["examples"],
                "count": info["count"],
                "pct": round(info["count"] / total_gap_items, 2)
                if total_gap_items
                else 0,
            }
            for cat, info in gap_counts.items()
            if info["count"] > 0
        ],
        key=lambda x: -x["count"],
    )

    # --- Misalignment theme clustering ---
    mis_rows = conn.execute("""
        SELECT misalignments FROM session_judgments
        WHERE misalignments IS NOT NULL AND misalignments != '[]'
    """).fetchall()

    THEME_KEYWORDS = {
        "tool_overuse": ["tool", "unnecessary", "redundant", "excessive", "repeated"],
        "wrong_approach": [
            "wrong",
            "incorrect",
            "different approach",
            "should have",
            "instead of",
        ],
        "scope_drift": [
            "scope",
            "beyond",
            "unrelated",
            "off-topic",
            "tangent",
            "extra",
        ],
        "format_mismatch": ["format", "style", "convention", "naming", "pattern"],
        "misunderstood_intent": [
            "misunderstood",
            "misinterpret",
            "not what",
            "didn't ask",
            "intent",
        ],
        "ignored_feedback": [
            "ignored",
            "repeated",
            "already said",
            "told you",
            "again",
        ],
    }

    theme_counts = {
        t: {"count": 0, "example": "", "sessions": []} for t in THEME_KEYWORDS
    }
    for (raw,) in mis_rows:
        try:
            items = json.loads(raw) if isinstance(raw, str) else raw
            for item in items:
                desc = (
                    item.get("description", str(item))
                    if isinstance(item, dict)
                    else str(item)
                ).lower()
                for theme, keywords in THEME_KEYWORDS.items():
                    if any(kw in desc for kw in keywords):
                        theme_counts[theme]["count"] += 1
                        if not theme_counts[theme]["example"]:
                            theme_counts[theme]["example"] = (
                                item.get("description", str(item))
                                if isinstance(item, dict)
                                else str(item)
                            )
                        break
        except (json.JSONDecodeError, TypeError):
            continue

    misalignment_themes = sorted(
        [
            {
                "theme": theme.replace("_", " "),
                "count": info["count"],
                "example": info["example"],
            }
            for theme, info in theme_counts.items()
            if info["count"] > 0
        ],
        key=lambda x: -x["count"],
    )

    # --- Behavioral correlations ---
    correlations = []

    # Prompt length vs productivity
    prompt_bins = conn.execute("""
        SELECT
            CASE
                WHEN LENGTH(s.first_prompt) < 100 THEN 'short (<100 chars)'
                WHEN LENGTH(s.first_prompt) < 500 THEN 'medium (100-500 chars)'
                ELSE 'long (>500 chars)'
            END as bin,
            AVG(j.productivity_ratio) as avg_prod,
            COUNT(*) as n
        FROM sessions s
        JOIN session_judgments j ON s.session_id = j.session_id
        GROUP BY bin
        HAVING n >= 3
        ORDER BY avg_prod DESC
    """).fetchall()
    if len(prompt_bins) >= 2:
        parts = [
            f"{b[0]}: {b[1]:.0%} productivity ({b[2]} sessions)" for b in prompt_bins
        ]
        correlations.append(
            {
                "factor": "First prompt length",
                "insight": ". ".join(parts) + ".",
                "type": "tip",
            }
        )

    # Corrections vs completion
    corr_bins = conn.execute("""
        SELECT
            CASE WHEN f.correction_count = 0 THEN 'zero corrections' ELSE 'has corrections' END as bin,
            SUM(CASE WHEN j.outcome = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as completion_pct,
            AVG(j.productivity_ratio) as avg_prod,
            COUNT(*) as n
        FROM sessions s
        JOIN session_features f ON s.session_id = f.session_id
        JOIN session_judgments j ON s.session_id = j.session_id
        GROUP BY bin
        HAVING n >= 3
    """).fetchall()
    if len(corr_bins) >= 2:
        parts = [
            f"{b[0]}: {b[1]:.0f}% completion rate, {b[2]:.0%} productivity ({b[3]} sessions)"
            for b in corr_bins
        ]
        correlations.append(
            {
                "factor": "Corrections impact",
                "insight": ". ".join(parts) + ".",
                "type": "tip",
            }
        )

    # Unique tools vs productivity
    tool_bins = conn.execute("""
        SELECT
            CASE WHEN f.unique_tools_used < 5 THEN 'focused (<5 tools)' ELSE 'broad (5+ tools)' END as bin,
            AVG(j.productivity_ratio) as avg_prod,
            COUNT(*) as n
        FROM sessions s
        JOIN session_features f ON s.session_id = f.session_id
        JOIN session_judgments j ON s.session_id = j.session_id
        GROUP BY bin
        HAVING n >= 3
    """).fetchall()
    if len(tool_bins) >= 2:
        parts = [
            f"{b[0]}: {b[1]:.0%} productivity ({b[2]} sessions)" for b in tool_bins
        ]
        correlations.append(
            {
                "factor": "Tool breadth",
                "insight": ". ".join(parts) + ".",
                "type": "tip",
            }
        )

    # --- Worst sessions ---
    worst = conn.execute("""
        SELECT j.session_id, j.misalignment_count, j.outcome,
               LEFT(s.first_prompt, 120) as prompt_preview
        FROM session_judgments j
        JOIN sessions s ON j.session_id = s.session_id
        WHERE j.misalignment_count > 0
        ORDER BY j.misalignment_count DESC
        LIMIT 5
    """).fetchall()

    worst_sessions = [
        {
            "session_id": w[0],
            "misalignments": w[1],
            "outcome": w[2],
            "prompt_preview": w[3],
        }
        for w in worst
    ]

    return jsonify(
        {
            "prompt_gaps": prompt_gaps,
            "misalignment_themes": misalignment_themes,
            "behavioral_correlations": correlations,
            "worst_sessions": worst_sessions,
        }
    )


@app.route("/api/skills/dimensions")
def api_skill_dimensions():
    from .config import SKILL_DIMENSIONS

    dims = []
    for dim_id in sorted(SKILL_DIMENSIONS.keys(), key=lambda x: int(x[1:])):
        d = SKILL_DIMENSIONS[dim_id]
        dims.append(
            {
                "id": dim_id,
                "name": d["name"],
                "short": d["short"],
                "weight": d["weight"],
                "color": d["color"],
            }
        )
    return jsonify({"dimensions": dims})


@app.route("/api/skills/profile")
def api_skill_profile():
    conn = get_conn()

    profile = conn.execute("SELECT * FROM skill_profile WHERE id = 1").fetchone()
    if not profile:
        return jsonify({"profile": None})

    cols = [d[0] for d in conn.description]
    p = _row_to_dict(profile, cols)
    return jsonify({"profile": p})


@app.route("/api/skills/session/<session_id>")
def api_skill_session(session_id):
    conn = get_conn()

    row = conn.execute(
        "SELECT * FROM session_skills WHERE session_id = ?", [session_id]
    ).fetchone()
    if not row:
        return jsonify({"skills": None})

    cols = [d[0] for d in conn.description]
    return jsonify({"skills": _row_to_dict(row, cols)})


@app.route("/api/skills/nudges")
def api_skill_nudges():
    from .config import SKILL_DIMENSIONS

    conn = get_conn()
    rows = conn.execute("""
        SELECT id, dimension, current_level, target_level, nudge_text,
               evidence, frequency, dismissed, created_at
        FROM skill_nudges
        WHERE dismissed = FALSE
        ORDER BY created_at DESC
    """).fetchall()

    cols = [
        "id",
        "dimension",
        "current_level",
        "target_level",
        "nudge_text",
        "evidence",
        "frequency",
        "dismissed",
        "created_at",
    ]

    nudges = []
    for r in rows:
        nd = _row_to_dict(r, cols)
        dim_id = nd.get("dimension", "")
        dim_info = SKILL_DIMENSIONS.get(dim_id, {})
        nd["dimension_name"] = dim_info.get("name", dim_id)
        nd["dimension_color"] = dim_info.get("color", "#8b8fa3")
        nudges.append(nd)

    return jsonify({"nudges": nudges})


@app.route("/api/skills/nudges/<int:nid>/dismiss", methods=["POST"])
def api_dismiss_skill_nudge(nid):
    conn = get_conn()
    conn.execute("UPDATE skill_nudges SET dismissed = TRUE WHERE id = ?", [nid])
    return jsonify({"ok": True})


@app.route("/api/heatmap")
def api_heatmap():
    conn = get_conn()

    rows = conn.execute("""
        SELECT
            f.day_of_week,
            f.hour_of_day,
            COUNT(*) as count,
            AVG(s.convergence_score) as avg_convergence
        FROM sessions s
        JOIN session_features f ON s.session_id = f.session_id
        GROUP BY f.day_of_week, f.hour_of_day
        ORDER BY f.day_of_week, f.hour_of_day
    """).fetchall()

    return jsonify(
        {
            "heatmap": [
                {
                    "day": r[0],
                    "hour": r[1],
                    "count": r[2],
                    "avg_convergence": round(r[3], 3),
                }
                for r in rows
            ],
        }
    )
