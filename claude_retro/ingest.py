"""JSONL parsing → raw_entries table with incremental ingestion."""

import json
import os
from pathlib import Path

from .config import CLAUDE_PROJECTS_DIR
from .db import get_conn


def find_jsonl_files() -> list[tuple[Path, str]]:
    """Find all JSONL files and their project names."""
    results = []
    if not CLAUDE_PROJECTS_DIR.exists():
        return results
    for project_dir in sorted(CLAUDE_PROJECTS_DIR.iterdir()):
        if not project_dir.is_dir():
            continue
        project_name = project_dir.name
        for jsonl_file in sorted(project_dir.glob("*.jsonl")):
            results.append((jsonl_file, project_name))
    return results


def needs_ingestion(file_path: Path, conn) -> bool:
    """Check if file needs (re-)ingestion based on mtime."""
    mtime = os.path.getmtime(file_path)
    result = conn.execute(
        "SELECT mtime FROM ingestion_log WHERE file_path = ?", [str(file_path)]
    ).fetchone()
    if result is None:
        return True
    return mtime > result[0]


def parse_entry(line: str, project_name: str) -> dict | None:
    """Parse a single JSONL line into a raw_entry dict."""
    try:
        d = json.loads(line)
    except json.JSONDecodeError:
        return None

    entry_type = d.get("type")
    if entry_type == "progress":
        return None
    if entry_type == "file-history-snapshot":
        return None

    entry_id = d.get("uuid")
    if not entry_id:
        return None

    session_id = d.get("sessionId")
    timestamp = d.get("timestamp")
    parent_uuid = d.get("parentUuid")
    is_sidechain = d.get("isSidechain", False)
    git_branch = d.get("gitBranch")
    cwd = d.get("cwd")

    msg = d.get("message", {})
    model = msg.get("model")
    usage = msg.get("usage", {})
    input_tokens = usage.get("input_tokens", 0)
    output_tokens = usage.get("output_tokens", 0)

    # System entries
    system_subtype = d.get("subtype")
    duration_ms = d.get("durationMs", 0)

    # Parse content
    content = msg.get("content", "")
    user_text = ""
    user_text_length = 0
    is_tool_result = False
    tool_result_error = False
    content_types = []
    tool_names = []
    text_content = ""
    text_length = 0

    if isinstance(content, str):
        if entry_type == "user":
            user_text = content
            user_text_length = len(content)
        elif entry_type == "assistant":
            text_content = content
            text_length = len(content)
        content_types = ["text"]
    elif isinstance(content, list):
        text_parts = []
        user_text_parts = []
        for block in content:
            btype = block.get("type", "")
            content_types.append(btype)
            if btype == "text":
                t = block.get("text", "")
                if entry_type == "user":
                    user_text_parts.append(t)
                else:
                    text_parts.append(t)
            elif btype == "tool_use":
                tool_names.append(block.get("name", ""))
            elif btype == "tool_result":
                is_tool_result = True
                if block.get("is_error"):
                    tool_result_error = True
            elif btype == "thinking":
                pass  # skip thinking content to save space

        if entry_type == "user":
            user_text = "\n".join(user_text_parts)
            user_text_length = len(user_text)
        text_content = "\n".join(text_parts)
        text_length = len(text_content)

    # Deduplicate content_types
    content_types = list(dict.fromkeys(content_types))

    return {
        "entry_id": entry_id,
        "session_id": session_id,
        "project_name": project_name,
        "entry_type": entry_type,
        "timestamp_utc": timestamp,
        "parent_uuid": parent_uuid,
        "is_sidechain": is_sidechain,
        "user_text": user_text,
        "user_text_length": user_text_length,
        "is_tool_result": is_tool_result,
        "tool_result_error": tool_result_error,
        "model": model,
        "content_types": content_types,
        "tool_names": tool_names,
        "text_content": text_content,
        "text_length": text_length,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "system_subtype": system_subtype,
        "duration_ms": duration_ms or 0,
        "git_branch": git_branch,
        "cwd": cwd,
    }


def ingest_file(file_path: Path, project_name: str, conn) -> int:
    """Ingest a single JSONL file. Returns count of entries added."""
    # Remove old entries for this file's sessions
    # We identify by checking existing entries from this file
    entries = []
    with open(file_path, "r", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            entry = parse_entry(line, project_name)
            if entry:
                entries.append(entry)

    if not entries:
        return 0

    # Batch insert using INSERT OR REPLACE
    for entry in entries:
        conn.execute(
            """
            INSERT OR REPLACE INTO raw_entries VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """,
            [
                entry["entry_id"],
                entry["session_id"],
                entry["project_name"],
                entry["entry_type"],
                entry["timestamp_utc"],
                entry["parent_uuid"],
                entry["is_sidechain"],
                entry["user_text"],
                entry["user_text_length"],
                entry["is_tool_result"],
                entry["tool_result_error"],
                entry["model"],
                entry["content_types"],
                entry["tool_names"],
                entry["text_content"],
                entry["text_length"],
                entry["input_tokens"],
                entry["output_tokens"],
                entry["system_subtype"],
                entry["duration_ms"],
                entry["git_branch"],
                entry["cwd"],
            ],
        )

    # Update ingestion log
    mtime = os.path.getmtime(file_path)
    conn.execute(
        """
        INSERT OR REPLACE INTO ingestion_log VALUES (?, ?, ?, current_timestamp)
    """,
        [str(file_path), mtime, len(entries)],
    )

    return len(entries)


def run_ingest() -> dict:
    """Run full incremental ingestion. Returns stats."""
    conn = get_conn()
    files = find_jsonl_files()

    stats = {
        "total_files": len(files),
        "ingested_files": 0,
        "total_entries": 0,
        "skipped_files": 0,
    }

    for file_path, project_name in files:
        if not needs_ingestion(file_path, conn):
            stats["skipped_files"] += 1
            continue
        count = ingest_file(file_path, project_name, conn)
        stats["ingested_files"] += 1
        stats["total_entries"] += count

    # Get total counts
    stats["total_entries_in_db"] = conn.execute(
        "SELECT COUNT(*) FROM raw_entries"
    ).fetchone()[0]
    stats["total_sessions_found"] = conn.execute(
        "SELECT COUNT(DISTINCT session_id) FROM raw_entries WHERE session_id IS NOT NULL"
    ).fetchone()[0]
    stats["total_projects"] = conn.execute(
        "SELECT COUNT(DISTINCT project_name) FROM raw_entries"
    ).fetchone()[0]

    return stats
