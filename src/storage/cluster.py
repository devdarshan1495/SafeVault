"""Storage cluster with replication-aware read/write."""

import random
from typing import Dict, List, Optional, Tuple
from src.storage.node import StorageNode, NodeStatus


class StorageCluster:
    """Manages a set of storage nodes with replication support."""

    def __init__(self, replication_factor: int = 3):
        self.replication_factor = replication_factor
        self._nodes: Dict[str, StorageNode] = {}

    def add_node(self, node: StorageNode) -> None:
        self._nodes[node.id] = node

    def remove_node(self, node_id: str) -> None:
        self._nodes.pop(node_id, None)

    def get_node(self, node_id: str) -> Optional[StorageNode]:
        return self._nodes.get(node_id)

    def online_nodes(self, region: Optional[str] = None) -> List[StorageNode]:
        nodes = [n for n in self._nodes.values()
                 if n.status == NodeStatus.ONLINE]
        if region:
            nodes = [n for n in nodes if n.region == region]
        return nodes

    def select_nodes_for_write(self, count: Optional[int] = None,
                               primary_region: str = "us-east-1") -> List[StorageNode]:
        """Select nodes: 2 from primary region, 1 from another."""
        if count is None:
            count = self.replication_factor
        candidates = self.online_nodes()
        if len(candidates) < count:
            raise RuntimeError(f"Not enough online nodes "
                               f"({len(candidates)} < {count})")

        primary = [n for n in candidates if n.region == primary_region]
        other = [n for n in candidates if n.region != primary_region]

        selected = random.sample(primary, min(2, len(primary)))
        if len(selected) < count and other:
            selected.append(random.choice(other))
        while len(selected) < count and candidates:
            n = random.choice(candidates)
            if n not in selected:
                selected.append(n)
        return selected[:count]

    def quorum_write(self, chunk_hash: str, data: bytes,
                     region: str = "us-east-1") -> List[Tuple[str, str]]:
        """Write to replication_factor nodes, wait for quorum (N/2+1)."""
        nodes = self.select_nodes_for_write(primary_region=region)
        success_count = 0
        locations: List[Tuple[str, str]] = []
        quorum = self.replication_factor // 2 + 1

        for node in nodes:
            try:
                node.store(chunk_hash, data)
                success_count += 1
                locations.append((node.id, f"/chunks/{chunk_hash[:4]}/{chunk_hash}"))
            except Exception as e:
                print(f"[WARN] Write to node {node.name} failed: {e}")

        if success_count < quorum:
            # Rollback successful writes
            for node_id, path in locations:
                n = self._nodes.get(node_id)
                if n:
                    try:
                        n.delete(chunk_hash)
                    except Exception:
                        pass
            raise RuntimeError(f"Quorum write failed: "
                               f"{success_count}/{self.replication_factor} acked")

        return locations

    def quorum_read(self, chunk_hash: str) -> Optional[bytes]:
        """Read from any online node that has the chunk."""
        for node in self.online_nodes():
            try:
                data = node.retrieve(chunk_hash)
                if data is not None:
                    return data
            except Exception:
                continue
        return None

    def node_count(self) -> int:
        return len(self._nodes)
