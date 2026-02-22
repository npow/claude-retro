# Claude Retro

[![CI](https://github.com/npow/claude-retro/actions/workflows/ci.yml/badge.svg)](https://github.com/npow/claude-retro/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

You spend hours in Claude Code every day. Are those sessions actually working? Which prompting habits lead to clean completions, and which ones send Claude into loops?

Claude Retro is a sprint retro for your AI coding sessions. It reads your local Claude Code history, scores every session, and tells you exactly what to change.

**What you get:**
- **Session scores** -- convergence (on track), drift (wandering), thrash (stuck in loops) for every session
- **Prompt quality grades** -- an LLM judge rates your prompts on clarity and completeness, flags what was missing
- **Waste analysis** -- which turns were productive vs. wasted on corrections, rework, and misalignment
- **Pattern detection** -- recurring prompt gaps, misalignment themes, behavioral correlations across all your sessions
- **Actionable prescriptions** -- specific recommendations based on your data, not generic advice

All analysis runs locally against `~/.claude/projects/`. Nothing leaves your machine except the LLM judging calls (which use your own `claude` CLI).

## Architecture

```
~/.claude/projects/**/*.jsonl          ~/.claude/retro.duckdb
         |                                      ^
         v                                      |
 +-----------------+    every 30s    +----------+----------+
 | IngestionWorker |--------------->| ingest -> sessions  |
 |  (daemon thread)|                | -> features         |
 +-----------------+                | -> scores           |
                                    | -> intents          |
                                    | -> baselines        |
                                    | -> prescriptions    |
                                    +---------------------+
                                              ^
                                              |  on "Refresh Data"
                                    +---------------------+
                                    |    LLM Judge        |
                                    | (claude -p calls)   |
                                    +---------------------+

 +-------------------+
 |   Flask server    |<--- REST API ---> Browser / pywebview
 |  (daemon thread)  |
 +-------------------+
```

**Desktop mode** (`claude-retro app`): pywebview window on the main thread, Flask + IngestionWorker as daemon threads. Closing the window exits everything.

**Server mode** (`claude-retro`): Flask runs on the main thread, IngestionWorker as a daemon thread, browser opens automatically.

## Quick start

```bash
pip install -e .
claude-retro
```

Opens in your browser. Sessions auto-refresh every 30 seconds. Hit "Refresh Data" to run LLM judging on new sessions.

## Desktop app

```bash
claude-retro app
```

Or build a standalone macOS `.app`:

```bash
bash build_macos.sh
```

## Commands

| Command | Description |
|---------|-------------|
| `claude-retro` | Start server + open browser (default) |
| `claude-retro app` | Launch native desktop window |
| `claude-retro ingest` | Run full pipeline including LLM judging |
| `claude-retro digest` | Print a weekly summary to stdout |
| `claude-retro reset` | Delete the database and start fresh |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CLAUDE_RETRO_DB` | `~/.claude/retro.duckdb` | Database path |
| `CLAUDE_RETRO_PORT` | `8420` | Server port |

## Requirements

- Python 3.10+
- `claude` CLI on PATH (for LLM judging)
