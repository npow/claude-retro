"""File system watcher for automatic session discovery."""

import logging
import os
import threading
import time
from pathlib import Path

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent, FileCreatedEvent

from .config import CLAUDE_PROJECTS_DIR

logger = logging.getLogger(__name__)


class SessionFileHandler(FileSystemEventHandler):
    """Handler for session JSONL file changes."""

    def __init__(self, on_change_callback):
        self.on_change = on_change_callback
        self._debounce_lock = threading.Lock()
        self._pending_files = set()
        self._debounce_timer = None

    def on_modified(self, event):
        if isinstance(event, FileModifiedEvent) and event.src_path.endswith(".jsonl"):
            self._queue_change(event.src_path)

    def on_created(self, event):
        if isinstance(event, FileCreatedEvent) and event.src_path.endswith(".jsonl"):
            self._queue_change(event.src_path)

    def _queue_change(self, path: str):
        """Queue a file change with debouncing.

        Multiple changes to the same file within 2 seconds are coalesced
        into a single callback invocation.
        """
        with self._debounce_lock:
            self._pending_files.add(path)

            # Cancel existing timer if any
            if self._debounce_timer:
                self._debounce_timer.cancel()

            # Set new timer to fire callback after 2s of quiet
            self._debounce_timer = threading.Timer(2.0, self._fire_callback)
            self._debounce_timer.start()

    def _fire_callback(self):
        """Fire the callback with accumulated file paths."""
        with self._debounce_lock:
            if self._pending_files:
                files = list(self._pending_files)
                self._pending_files.clear()
                try:
                    self.on_change(files)
                except Exception as e:
                    logger.error(f"Error in file change callback: {e}")


class FileWatcher:
    """Watch ~/.claude/projects/ for new or modified JSONL files.

    Replaces the polling-based change detection in IngestionWorker.
    """

    def __init__(self, on_change_callback):
        """Initialize the file watcher.

        Args:
            on_change_callback: Function called with list of changed file paths
                when .jsonl files are created or modified.
        """
        self.on_change = on_change_callback
        self.observer = None
        self.handler = SessionFileHandler(on_change_callback)
        self._started = False

    def start(self):
        """Start watching the session directory."""
        if self._started:
            return

        if not CLAUDE_PROJECTS_DIR.exists():
            logger.warning(
                f"Session directory does not exist: {CLAUDE_PROJECTS_DIR}"
            )
            return

        self.observer = Observer()
        self.observer.schedule(
            self.handler, str(CLAUDE_PROJECTS_DIR), recursive=True
        )
        self.observer.start()
        self._started = True
        logger.info(f"Watching for session changes: {CLAUDE_PROJECTS_DIR}")

    def stop(self):
        """Stop watching the session directory."""
        if self.observer:
            self.observer.stop()
            self.observer.join(timeout=5)
            self._started = False
            logger.info("File watcher stopped")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
