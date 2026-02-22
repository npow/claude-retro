"""Configuration: paths, thresholds, scoring weights, keyword lists."""

import os
from pathlib import Path

# Paths
CLAUDE_PROJECTS_DIR = Path.home() / ".claude" / "projects"
DB_PATH = Path(
    os.environ.get("CLAUDE_RETRO_DB", Path.home() / ".claude" / "retro.duckdb")
)
SERVER_PORT = int(os.environ.get("CLAUDE_RETRO_PORT", "8420"))

# Scoring weights
CONVERGENCE_WEIGHTS = {
    "prompt_length_decrease": 0.25,
    "decision_markers": 0.20,
    "low_correction_rate": 0.20,
    "low_tool_error_rate": 0.15,
    "has_pr": 0.10,
    "stable_response_length": 0.10,
}

DRIFT_WEIGHTS = {
    "keyword_entropy": 0.25,
    "increasing_prompt_length": 0.20,
    "branch_switches": 0.15,
    "sidechain_ratio": 0.15,
    "no_decisions": 0.15,
    "long_session": 0.10,
}

THRASH_WEIGHTS = {
    "correction_rate": 0.30,
    "tool_error_rate": 0.25,
    "rephrasing": 0.20,
    "oscillating_lengths": 0.15,
    "api_errors": 0.10,
}

# Trajectory thresholds
TRAJECTORY_THRESHOLDS = {
    "converged": {"convergence_min": 0.6, "drift_max": 0.3, "thrash_max": 0.3},
    "drifted": {"drift_min": 0.5, "convergence_max": 0.4},
    "thrashed": {"thrash_min": 0.5, "convergence_max": 0.4},
}

# Baseline windows
BASELINE_WINDOWS = [14, 60]

# Intent keywords (keyword -> intent, weight)
INTENT_KEYWORDS = {
    "debug": [
        "bug",
        "error",
        "fix",
        "broken",
        "crash",
        "fail",
        "issue",
        "wrong",
        "traceback",
        "exception",
        "debug",
        "stack trace",
    ],
    "implement": [
        "add",
        "create",
        "build",
        "implement",
        "new feature",
        "write",
        "make",
        "generate",
    ],
    "refactor": [
        "refactor",
        "clean up",
        "restructure",
        "reorganize",
        "rename",
        "extract",
        "simplify",
        "move",
    ],
    "research": [
        "how does",
        "what is",
        "explain",
        "understand",
        "look at",
        "find",
        "search",
        "where is",
        "show me",
    ],
    "brainstorm": [
        "idea",
        "think about",
        "consider",
        "brainstorm",
        "design",
        "plan",
        "approach",
        "strategy",
        "option",
    ],
    "review": [
        "review",
        "check",
        "audit",
        "look over",
        "examine",
        "inspect",
        "pr",
        "pull request",
    ],
    "prototype": [
        "prototype",
        "proof of concept",
        "poc",
        "experiment",
        "try",
        "test",
        "spike",
        "explore",
    ],
}

# Decision markers
DECISION_MARKERS = [
    "let's go with",
    "i'll use",
    "decided to",
    "going with",
    "chosen",
    "the approach",
    "final",
    "commit",
    "ship",
    "merge",
    "lgtm",
    "looks good",
    "that works",
    "perfect",
    "done",
    "complete",
]

# Correction indicators
CORRECTION_MARKERS = [
    "actually",
    "wait",
    "no,",
    "sorry",
    "wrong",
    "instead",
    "not that",
    "undo",
    "revert",
    "go back",
    "try again",
    "that's not",
    "that didn't",
    "doesn't work",
    "not working",
]

# Rephrasing indicators
REPHRASING_MARKERS = [
    "i mean",
    "what i meant",
    "to clarify",
    "in other words",
    "let me rephrase",
    "more specifically",
    "to be clear",
]

# Tool categories for analysis
TOOL_CATEGORIES = {
    "edit_write": ["Edit", "Write", "NotebookEdit"],
    "read_grep": ["Read", "Grep", "Glob"],
    "bash": ["Bash"],
    "task": ["Task"],
    "web": ["WebFetch", "WebSearch"],
}
