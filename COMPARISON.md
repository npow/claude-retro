# agentsview vs claude-retro: Architecture Comparison

## Overview

**agentsview**: Session browser/viewer with full-text search and analytics
**claude-retro**: Retrospective analysis tool with LLM-powered insights

## Architecture Comparison

### agentsview (Go + Svelte)
```
Go Server (single binary)
├── SQLite + FTS5 (full-text search)
├── File Watcher (fsnotify)
├── SSE for live updates
├── Embedded Svelte 5 SPA
└── Export to HTML/Gist
```

**Lines of code**: ~34k (Go backend + Svelte frontend)

### claude-retro (Python + Vanilla JS)
```
Python Flask Server
├── DuckDB (sessions, features, scores)
├── Background IngestionWorker (daemon thread)
├── LLM Judge (Anthropic SDK via claude-relay)
├── Single-file HTML/JS/CSS
└── pywebview for desktop app
```

**Lines of code**: ~5.3k Python

## Key Differences

| Feature | agentsview | claude-retro |
|---------|-----------|--------------|
| **Language** | Go + Svelte 5 | Python + Vanilla JS |
| **Database** | SQLite + FTS5 | DuckDB |
| **Frontend** | Svelte SPA (multi-file) | Single HTML file |
| **Real-time** | SSE events | 30s polling |
| **Build** | Compiled binary | PyInstaller .app |
| **Dependencies** | Node.js for build | Python 3.10+ |
| **Purpose** | Browse/search sessions | Analyze/improve patterns |

## What We Can Steal

### 1. **SSE for Real-Time Updates** ⭐⭐⭐
**Why**: Better than polling, instant feedback during ingestion

agentsview implementation:
```go
// internal/server/events.go
func (s *Server) handleWatchSession(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "text/event-stream")
    // Stream session updates as they happen
}
```

**What to steal**:
- Replace 30s polling with SSE endpoint
- Stream ingestion progress in real-time
- Update UI instantly when LLM judge completes

**Priority**: HIGH - Much better UX than polling

---

### 2. **File Watcher for Auto-Sync** ⭐⭐⭐
**Why**: Detect new sessions immediately instead of polling

agentsview uses `fsnotify` to watch `~/.claude/projects/`:
```go
// internal/sync/watcher.go
watcher, _ := fsnotify.NewWatcher()
watcher.Add(claudeDir)
```

**What to steal**:
- Use `watchdog` (Python equivalent of fsnotify)
- Trigger ingestion on new .jsonl files
- Remove 30s polling loop

**Priority**: HIGH - Better performance, instant updates

---

### 3. **Separate Read/Write DB Connections** ⭐⭐
**Why**: Better concurrency, no lock contention

agentsview pattern:
```go
type DB struct {
    writer *sql.DB  // serialized writes
    reader *sql.DB  // concurrent reads
    mu     sync.Mutex
}
```

**What to steal**:
- Open DuckDB with separate read/write connections
- Use writer for ingestion, reader for API queries
- Remove "Waiting for DuckDB lock" issues

**Priority**: MEDIUM - Would fix the crash we just had!

---

### 4. **Progressive Frontend Build** ⭐⭐
**Why**: Modern tooling, better dev experience

agentsview:
- Svelte 5 with TypeScript
- Vite dev server with HMR
- Embedded in binary at build time

claude-retro:
- Single HTML file with inline JS/CSS
- No build step, no tooling

**What to steal**:
- Consider Svelte or Vue for better componentization
- Keep single-file option for simplicity
- Add HMR for faster iteration

**Priority**: LOW - Current approach works, but this would scale better

---

### 5. **Export to HTML/Gist** ⭐
**Why**: Share insights with team

agentsview exports sessions as standalone HTML:
```go
// internal/server/export.go
func (s *Server) handleExportSession(w http.ResponseWriter, r *http.Request)
```

**What to steal**:
- Export verdict + prescriptions as shareable HTML
- Add "Share to Gist" button for top 3 changes
- Generate weekly digest as standalone report

**Priority**: MEDIUM - Nice to have for team sharing

---

### 6. **Version Info in Binary** ⭐
**Why**: Better debugging, release tracking

agentsview embeds git version at build time:
```go
LDFLAGS := -X main.version=$(VERSION) \
           -X main.commit=$(COMMIT) \
           -X main.buildDate=$(BUILD_DATE)
```

**What to steal**:
- Show version in UI footer
- Include in bug reports
- Auto-update checker (they have this!)

**Priority**: LOW - Nice polish

---

### 7. **Skip Cache for Failed Parses** ⭐⭐
**Why**: Avoid re-processing bad files

agentsview pattern:
```go
// internal/sync/engine.go
skipCache map[string]int64  // path -> mtime
```

**What to steal**:
- Cache files that fail parsing or LLM judging
- Retry only when mtime changes
- Speeds up re-ingestion

**Priority**: MEDIUM - Performance optimization

---

### 8. **Keyboard Shortcuts** ⭐
**Why**: Power user efficiency

agentsview has vim-style navigation:
- `j`/`k` for next/prev message
- `]`/`[` for next/prev session
- `Cmd+K` for search

**What to steal**:
- Add keyboard nav to session list
- `r` to trigger LLM judge
- `e` to export verdict

**Priority**: LOW - Nice UX polish

---

### 9. **Makefile for Build Tasks** ⭐⭐
**Why**: Standardized build process

agentsview has clean Makefile:
```makefile
build: frontend
	CGO_ENABLED=1 go build -tags fts5 ...

frontend:
	cd frontend && npm run build
```

**What to steal**:
- Replace `build_macos.sh` with Makefile
- Add `make dev`, `make test`, `make install`
- Document all build targets in `make help`

**Priority**: MEDIUM - Better dev experience

---

### 10. **CI/CD with GitHub Actions** ⭐
**Why**: Automated testing and releases

agentsview has:
- Automated tests on PR
- Release builds for multiple platforms
- Auto-deploy to GitHub Releases

claude-retro has:
- Basic CI tests
- Manual releases

**What to steal**:
- Auto-build .app on tags
- Upload to GitHub Releases
- Generate release notes from commits

**Priority**: LOW - Would automate releases

---

## What NOT to Steal

### 1. Go + CGO Complexity
- Go is fast but adds build complexity (CGO for SQLite)
- Python is fine for this use case
- PyInstaller works well for macOS .app

### 2. Multi-Agent Support
- agentsview supports Claude, Codex, Gemini
- claude-retro is Claude-specific, that's fine
- Focus beats breadth here

### 3. Full-Text Search
- agentsview has FTS5 for searching message content
- claude-retro doesn't need this (analyzing, not searching)
- DuckDB has FTS if we need it later

---

## Immediate Wins (This Week)

1. **Add SSE for real-time updates** (replace polling)
2. **Add file watcher** (replace background thread polling)
3. **Separate read/write DB connections** (fix lock issues)
4. **Add skip cache** (performance)

These 4 changes would make claude-retro feel much snappier and more reliable.

---

## Nice to Have (Next Month)

5. **Export verdict to HTML**
6. **Makefile for build tasks**
7. **Version info in UI**
8. **Keyboard shortcuts**

---

## Code Structure Lessons

### agentsview strengths:
- Clean separation of concerns (config, db, parser, server, sync)
- Table-driven tests everywhere
- Embedded frontend in binary (single artifact)
- Proper error handling with context

### claude-retro strengths:
- Simple single-file frontend (no build step needed)
- Fast development cycle (Python)
- LLM-powered analysis (unique value prop)
- Good enough for now

---

## Recommendation

**Steal these in order**:

1. SSE (1-2 hours) - Replace `/api/status` polling
2. File watcher (1 hour) - Replace background thread
3. Read/write DB split (30 min) - Fix lock contention
4. Skip cache (30 min) - Performance boost

Total time investment: ~4 hours for major UX/reliability improvements.

The rest can wait until we have users complaining about specific gaps.
