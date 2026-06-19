"""Tests for storage node and cluster."""

import os
import pytest
from src.storage.node import StorageNode, NodeStatus
from src.storage.cluster import StorageCluster


class TestStorageNode:
    def setup_method(self):
        self.node = StorageNode(
            name="test-node", region="us-east-1", zone="us-east-1a",
            capacity_bytes=10_000,
        )

    def test_store_retrieve(self):
        h = "abc123"
        data = b"test data"
        assert self.node.store(h, data)
        assert self.node.retrieve(h) == data

    def test_store_duplicate(self):
        h = "abc123"
        assert self.node.store(h, b"data")
        assert not self.node.store(h, b"other")  # Already exists

    def test_retrieve_missing(self):
        assert self.node.retrieve("nonexistent") is None

    def test_delete(self):
        h = "abc123"
        self.node.store(h, b"data")
        assert self.node.delete(h)
        assert not self.node.delete(h)

    def test_capacity_limit(self):
        small_node = StorageNode(capacity_bytes=10)
        with pytest.raises(RuntimeError, match="capacity"):
            small_node.store("big", b"x" * 100)

    def test_offline_node(self):
        self.node.set_status(NodeStatus.OFFLINE)
        with pytest.raises(RuntimeError, match="offline"):
            self.node.store("h", b"data")
        with pytest.raises(RuntimeError, match="offline"):
            self.node.retrieve("h")


class TestStorageCluster:
    def setup_method(self):
        self.cluster = StorageCluster(replication_factor=3)
        for i in range(6):
            region = "us-east-1" if i < 4 else "eu-west-1"
            zone = f"{region}-{chr(97 + i)}"
            self.cluster.add_node(StorageNode(
                name=f"node-{i}", region=region, zone=zone,
                capacity_bytes=1_000_000,
            ))

    def test_quorum_write_read(self):
        h = "chunk_hash_1"
        data = os.urandom(1000)
        locations = self.cluster.quorum_write(h, data)
        assert len(locations) == 3

        result = self.cluster.quorum_read(h)
        assert result == data

    def test_quorum_read_from_any_node(self):
        h = "chunk_abc"
        data = b"test data"
        self.cluster.quorum_write(h, data)

        # Kill one node that has the data
        nodes = self.cluster.online_nodes()
        nodes[0].set_status(NodeStatus.OFFLINE)
        nodes[1].set_status(NodeStatus.OFFLINE)

        # Should still find data on remaining node
        result = self.cluster.quorum_read(h)
        assert result == data

    def test_node_count(self):
        assert self.cluster.node_count() == 6

    def test_online_nodes_filter(self):
        online = self.cluster.online_nodes()
        assert len(online) == 6
        us = self.cluster.online_nodes(region="us-east-1")
        assert len(us) == 4
