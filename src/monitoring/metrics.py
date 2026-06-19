"""Monitoring system — collects metrics and generates alerts."""

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from src.messaging.pubsub import PubSubBus, Event, EventTopic


@dataclass
class MetricSample:
    name: str
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


@dataclass
class Alert:
    severity: str  # info, warning, critical
    message: str
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


class MetricsCollector:
    """Collects and aggregates metrics from Pub/Sub events."""

    def __init__(self, pubsub: PubSubBus):
        self.pubsub = pubsub
        self._samples: List[MetricSample] = []
        self._alerts: List[Alert] = []
        self._counters: Dict[str, int] = defaultdict(int)
        self._latencies: Dict[str, List[float]] = defaultdict(list)

        # Subscribe to all events
        pubsub.subscribe(EventTopic.BACKUP_JOB_COMPLETED,
                         self._on_backup_complete)
        pubsub.subscribe(EventTopic.BACKUP_JOB_FAILED,
                         self._on_backup_failed)
        pubsub.subscribe(EventTopic.RESTORE_JOB_COMPLETED,
                         self._on_restore_complete)
        pubsub.subscribe(EventTopic.RESTORE_JOB_FAILED,
                         self._on_restore_failed)
        pubsub.subscribe(EventTopic.STORAGE_NODE_HEARTBEAT,
                         self._on_heartbeat)

    async def _on_backup_complete(self, event: Event) -> None:
        self._counters["backup.completed"] += 1
        self._samples.append(MetricSample(
            name="backup.bytes",
            value=event.payload.get("bytes", 0),
            labels={"policy_id": event.payload.get("policy_id", "")},
        ))
        self._samples.append(MetricSample(
            name="backup.dedup_savings",
            value=event.payload.get("dedup_savings", 0),
            labels={"policy_id": event.payload.get("policy_id", "")},
        ))

    async def _on_backup_failed(self, event: Event) -> None:
        self._counters["backup.failed"] += 1
        self._alerts.append(Alert(
            severity="critical",
            message=f"Backup failed: {event.payload.get('error', '')}",
            labels={"job_id": event.payload.get("job_id", "")},
        ))

    async def _on_restore_complete(self, event: Event) -> None:
        self._counters["restore.completed"] += 1

    async def _on_restore_failed(self, event: Event) -> None:
        self._counters["restore.failed"] += 1
        self._alerts.append(Alert(
            severity="critical",
            message=f"Restore failed: {event.payload.get('error', '')}",
            labels={"job_id": event.payload.get("job_id", "")},
        ))

    async def _on_heartbeat(self, event: Event) -> None:
        node_id = event.payload.get("node_id", "")
        region = event.payload.get("region", "")
        self._samples.append(MetricSample(
            name="storage.node.heartbeat",
            value=1,
            labels={"node_id": node_id, "region": region},
        ))

    def get_counter(self, name: str) -> int:
        return self._counters.get(name, 0)

    def get_alerts(self, severity: Optional[str] = None) -> List[Alert]:
        if severity:
            return [a for a in self._alerts if a.severity == severity]
        return self._alerts

    def summary(self) -> Dict:
        return {
            "backups_completed": self._counters.get("backup.completed", 0),
            "backups_failed": self._counters.get("backup.failed", 0),
            "restores_completed": self._counters.get("restore.completed", 0),
            "restores_failed": self._counters.get("restore.failed", 0),
            "critical_alerts": len(self.get_alerts("critical")),
            "total_samples": len(self._samples),
        }
