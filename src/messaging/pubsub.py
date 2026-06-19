"""In-memory Pub/Sub event bus for simulation.

In production, this would be backed by Apache Kafka / RabbitMQ
with partitioned topics, consumer groups, and at-least-once delivery.
"""

import asyncio
import enum
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional


class EventTopic(str, enum.Enum):
    SCHEDULER_BACKUP_DUE = "scheduler.backup.due"
    BACKUP_JOB_STARTED = "backup.job.started"
    BACKUP_JOB_COMPLETED = "backup.job.completed"
    BACKUP_JOB_FAILED = "backup.job.failed"
    BACKUP_CHUNK_STORED = "backup.chunk.stored"
    RESTORE_JOB_STARTED = "restore.job.started"
    RESTORE_JOB_COMPLETED = "restore.job.completed"
    RESTORE_JOB_FAILED = "restore.job.failed"
    STORAGE_NODE_HEARTBEAT = "storage.node.heartbeat"
    STORAGE_NODE_FAILED = "storage.node.failed"
    REPLICATION_SYNC_REQUIRED = "replication.sync.required"
    MONITORING_ALERT = "monitoring.alert"


@dataclass
class Event:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    topic: str = ""
    event_type: str = ""
    payload: dict = field(default_factory=dict)
    producer: str = ""
    timestamp: float = field(default_factory=time.time)


class PubSubBus:
    """Simple in-memory pub/sub bus. Async subscribers."""

    def __init__(self):
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._history: List[Event] = []

    def subscribe(self, topic: str, callback: Callable) -> None:
        self._subscribers[topic].append(callback)

    def unsubscribe(self, topic: str, callback: Callable) -> None:
        self._subscribers[topic] = [
            cb for cb in self._subscribers[topic] if cb is not callback
        ]

    async def publish(self, event: Event) -> None:
        self._history.append(event)
        for cb in self._subscribers.get(event.topic, []):
            try:
                if asyncio.iscoroutinefunction(cb):
                    await cb(event)
                else:
                    cb(event)
            except Exception as e:
                print(f"[PubSub] Error in subscriber for {event.topic}: {e}")

    def get_history(self, topic: Optional[str] = None) -> List[Event]:
        if topic:
            return [e for e in self._history if e.topic == topic]
        return self._history

    def clear(self) -> None:
        self._history.clear()
        self._subscribers.clear()
