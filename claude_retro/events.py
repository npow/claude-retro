"""Server-Sent Events (SSE) broadcaster for real-time updates."""

import json
import queue
import threading
import time
from typing import Any


class EventBroadcaster:
    """Thread-safe broadcaster for SSE events.

    Supports multiple concurrent listeners. Events are queued per listener
    with a max size to prevent memory leaks from slow/dead connections.
    """

    def __init__(self, max_queue_size: int = 100):
        self._listeners: list[queue.Queue] = []
        self._lock = threading.Lock()
        self._max_queue_size = max_queue_size

    def subscribe(self) -> queue.Queue:
        """Subscribe to events. Returns a queue that will receive events."""
        q = queue.Queue(maxsize=self._max_queue_size)
        with self._lock:
            self._listeners.append(q)
        return q

    def unsubscribe(self, q: queue.Queue):
        """Unsubscribe from events."""
        with self._lock:
            if q in self._listeners:
                self._listeners.remove(q)

    def emit(self, event: str, data: Any):
        """Broadcast an event to all listeners.

        Args:
            event: Event type (e.g., "status", "progress", "complete")
            data: Event data (will be JSON-serialized)
        """
        message = {
            "event": event,
            "data": data,
            "timestamp": time.time(),
        }

        with self._lock:
            dead_queues = []
            for q in self._listeners:
                try:
                    q.put_nowait(message)
                except queue.Full:
                    # Drop this event for slow consumers
                    pass
                except Exception:
                    dead_queues.append(q)

            # Remove dead queues
            for q in dead_queues:
                self._listeners.remove(q)

    @property
    def listener_count(self) -> int:
        """Return the number of active listeners."""
        with self._lock:
            return len(self._listeners)


# Global broadcaster instance
_broadcaster = EventBroadcaster()


def get_broadcaster() -> EventBroadcaster:
    """Get the global event broadcaster."""
    return _broadcaster


def emit_event(event: str, data: Any):
    """Emit an event to all listeners (convenience function)."""
    _broadcaster.emit(event, data)


def format_sse(event: str, data: dict) -> str:
    """Format an event as SSE protocol message."""
    lines = []
    if event:
        lines.append(f"event: {event}")
    lines.append(f"data: {json.dumps(data)}")
    lines.append("")  # Empty line terminates message
    return "\n".join(lines) + "\n"
