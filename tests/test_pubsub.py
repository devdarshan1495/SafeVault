"""Tests for the Pub/Sub event bus."""

import pytest
from src.messaging.pubsub import PubSubBus, Event, EventTopic


@pytest.mark.asyncio
async def test_publish_subscribe():
    bus = PubSubBus()
    received = []

    async def handler(event):
        received.append(event)

    bus.subscribe(EventTopic.BACKUP_JOB_COMPLETED, handler)

    event = Event(
        topic=EventTopic.BACKUP_JOB_COMPLETED,
        event_type="full",
        payload={"job_id": "123"},
        producer="test",
    )
    await bus.publish(event)
    assert len(received) == 1
    assert received[0].payload["job_id"] == "123"


@pytest.mark.asyncio
async def test_unsubscribe():
    bus = PubSubBus()
    received = []

    def handler(event):
        received.append(event)

    bus.subscribe(EventTopic.SCHEDULER_BACKUP_DUE, handler)
    bus.unsubscribe(EventTopic.SCHEDULER_BACKUP_DUE, handler)

    await bus.publish(Event(
        topic=EventTopic.SCHEDULER_BACKUP_DUE,
        event_type="scheduled",
        payload={},
        producer="test",
    ))
    assert len(received) == 0


@pytest.mark.asyncio
async def test_event_history():
    bus = PubSubBus()

    await bus.publish(Event(
        topic=EventTopic.BACKUP_JOB_STARTED,
        event_type="full", payload={}, producer="t1",
    ))
    await bus.publish(Event(
        topic=EventTopic.BACKUP_JOB_COMPLETED,
        event_type="full", payload={}, producer="t1",
    ))

    assert len(bus.get_history()) == 2
    completed = bus.get_history(EventTopic.BACKUP_JOB_COMPLETED)
    assert len(completed) == 1
