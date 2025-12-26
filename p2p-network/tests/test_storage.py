"""Unit tests for the distributed storage and search module.

These tests exercise the basic behaviours of the storage module
implemented in ``src/storage.py``.  They verify that keys are
assigned to the correct node based on a consistent hashing scheme,
that replication stores values on multiple successors, that values
can be retrieved via direct lookup on the ring and via a flooding
search, and that the heartbeat mechanism detects failed neighbours.

The tests are intentionally deterministic: rather than relying on the
system clock, they provide fixed timestamps when exercising the
heartbeat API.  This makes the assertions predictable and ensures
that test failures are meaningful.
"""

from __future__ import annotations

import time

# The tests rely solely on built‑in ``assert`` statements and do not
# require the ``pytest`` framework.  If ``pytest`` is installed it
# will discover and run these functions automatically.  Otherwise
# executing this file directly will still exercise the tests.

import os
import sys

# Ensure the parent directory of ``src`` is on the Python path so that
# ``import src.storage`` works whether the tests are run via pytest or
# directly via ``python tests/test_storage.py``.  Without this,
# Python may not locate the ``src`` package when executing the file
# directly.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.storage import (
    Node,
    calcular_hash,
    add_node,
    find_successor,
    put_value,
    get_value,
    flood_search,
)


def build_ring(addresses: list[tuple[str, int]]) -> list[Node]:
    """Create a ring from a list of (ip, port) tuples.

    The returned list is sorted by node ID and has the successor and
    predecessor pointers set appropriately for each node.
    """
    ring: list[Node] = []
    for ip, port in addresses:
        node = Node(ip, port)
        add_node(ring, node)
    return ring


def test_replication_and_lookup():
    """Values are stored on the correct nodes and replicated.

    This test constructs a ring of three nodes, stores a key and
    verifies that the value is present on the primary node and one
    successor (for a replication factor of 2).  It then checks that
    ``get_value`` retrieves the correct value, regardless of which
    node originally stored it.
    """
    ring = build_ring([
        ("127.0.0.1", 5000),
        ("127.0.0.1", 5001),
        ("127.0.0.1", 5002),
    ])
    # Put a key/value pair with replication factor 2.
    key = "alumno1"
    value = "nota=6.0"
    put_value(ring, key, value, replication=2)
    key_id = calcular_hash(key)
    primary = find_successor(ring, key_id)
    # Determine the indices of the primary and its successor.
    idx = ring.index(primary)
    replica_nodes = [ring[idx], ring[(idx + 1) % len(ring)]]
    # Ensure both replica nodes contain the key.
    for node in replica_nodes:
        assert key in node.data and node.data[key] == value
    # Nodes beyond the replication factor should not contain the key.
    non_replicas = [ring[(idx + 2) % len(ring)]]
    for node in non_replicas:
        assert key not in node.data
    # Retrieve the value via the ring's lookup helper.
    assert get_value(ring, key) == value


def test_flood_search_finds_value_within_ttl():
    """Flooding search returns the value when TTL is sufficient.

    The key is stored on one node and a flood search is initiated from
    another node.  With TTL >= distance, the search should find the
    value.  With TTL too low, it should return ``None``.
    """
    ring = build_ring([
        ("127.0.0.1", 5000),
        ("127.0.0.1", 5001),
        ("127.0.0.1", 5002),
    ])
    key = "important_key"
    value = "the_value"
    # Store on one node only (replication factor 1 for clarity).
    put_value(ring, key, value, replication=1)
    # Locate the node that holds the key.
    key_holder = find_successor(ring, calcular_hash(key))
    # Choose a different node to start the search.
    start_node = next(n for n in ring if n is not key_holder)
    # TTL of 2 is enough to reach the key in a three node ring.
    assert flood_search(start_node, key, ttl=2) == value
    # TTL of 0 means the search does not propagate beyond the start node.
    assert flood_search(start_node, key, ttl=0) is None


def test_heartbeat_detection():
    """Nodes detect failed neighbours when heartbeats lapse.

    A node receives heartbeats from two neighbours at controlled
    timestamps.  After advancing the clock beyond a timeout threshold
    for one neighbour, ``check_failed_neighbours`` should return that
    neighbour as failed while retaining the other.
    """
    a = Node("127.0.0.1", 5000)
    b = Node("127.0.0.1", 5001)
    c = Node("127.0.0.1", 5002)
    # Record heartbeats at specific times.
    base = 1000.0
    a.receive_heartbeat(b, timestamp=base)
    a.receive_heartbeat(c, timestamp=base)
    # After timeout/2 seconds, nothing should be considered failed.
    timeout = 10.0
    assert a.check_failed_neighbours(timeout, current_time=base + timeout / 2) == []
    # Advance time so that one neighbour's heartbeat is stale.
    # Provide a new heartbeat for c to keep it alive.
    a.receive_heartbeat(c, timestamp=base + timeout + 1)
    failed = a.check_failed_neighbours(timeout, current_time=base + timeout + 2)
    assert b in failed and c not in failed