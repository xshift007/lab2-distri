"""Distributed storage and search module for a simple P2P hash ring.

This module implements a very small subset of a distributed hash table
overlay suitable for educational purposes.  It does not communicate over
the network; instead it models a ring of nodes in memory.  Each node
stores key/value pairs assigned according to a consistent hash
function and replicates data onto its successors.  Keys may be
retrieved either by looking up the responsible node in the ring or by
flooding the ring with a search message constrained by a TTL.  A
simple heartbeat mechanism is also provided to monitor neighbour
liveness.

The design intentionally mirrors the high‑level requirements outlined
in the lab specification without implementing a full networked DHT.
This makes it possible to exercise and unit test the core storage
behaviours in isolation.  In a production system these functions
would be invoked in response to messages received over the network.

Functions and classes:

* ``calcular_hash(value: str, bits: int = 32) -> int``: compute a
  truncated SHA‑1 hash of the provided string.  A smaller bit width
  simplifies test scenarios while retaining deterministic ordering.

* ``Node``: represent a peer in the hash ring.  Each node has an ID
  derived from its IP and port, maintains pointers to its successor
  and predecessor and stores key/value data.  It also tracks
  heartbeat timestamps from critical neighbours.

* ``add_node(ring: list[Node], node: Node) -> None``: insert a new
  node into the ring, preserving the ring ordering and updating
  successor/predecessor pointers.

* ``find_successor(ring: list[Node], key_id: int) -> Node``: given a
  hash value, locate the node responsible for storing the key.

* ``put_value(ring: list[Node], key: str, value: str, replication: int = 2) -> None``:
  store a key/value pair on the responsible node and replicate it
  onto a fixed number of successors.

* ``get_value(ring: list[Node], key: str) -> str | None``: retrieve a
  stored value by its key, searching through replicas if necessary.

* ``flood_search(start: Node, key: str, ttl: int) -> str | None``:
  perform an unstructured search (flooding) starting from a node and
  constrained by a TTL.  Returns the first value found or ``None``
  if the TTL is exhausted.

* ``heartbeat`` methods on ``Node``: nodes can record receipt of
  heartbeats and detect failures when too much time has elapsed
  without a heartbeat.

This code is self‑contained and can be exercised directly from the
tests defined in ``tests/test_storage.py``.  It is deliberately
minimalistic but demonstrates how hashing, replication, search and
failure detection might be composed in a more complete system.
"""

from __future__ import annotations

import hashlib
import time
from typing import Dict, List, Optional, Set


def calcular_hash(value: str, bits: int = 32) -> int:
    """Return a truncated SHA‑1 hash of ``value`` as an integer.

    The full SHA‑1 digest yields a 160‑bit value.  For simplicity and
    to make tests easier to reason about, we truncate the hash to
    ``bits`` bits (default 32).  The truncation is done by taking the
    leading bits of the hexadecimal digest.  The result is
    deterministic for a given input and bit width.

    Args:
        value: The string to hash.
        bits: The number of bits of the resulting hash.  Must be
            between 1 and 160.

    Returns:
        An integer in the range [0, 2**bits).
    """
    if not (1 <= bits <= 160):
        raise ValueError("bits must be between 1 and 160")
    # Compute full SHA‑1 digest (40 hex characters == 160 bits).
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()
    # Convert hex digest to integer, then shift to truncate to desired bit length.
    full_int = int(digest, 16)
    # If bits == 160, no shift is needed.
    if bits == 160:
        return full_int
    # Shift right by (160 - bits) to keep the topmost bits.
    shift = 160 - bits
    return full_int >> shift


class Node:
    """A node in the simplified hash ring.

    Each node has an ``ip`` and ``port`` which together uniquely
    determine its ID via ``calcular_hash``.  Nodes maintain pointers to
    their successor and predecessor within the ring.  They also
    store key/value pairs in a local dictionary and track the last
    heartbeat times of critical neighbours (predecessor and successor).

    This class does not handle networking; in a full implementation the
    methods would send and receive messages using the networking and
    protocol modules.  For testing purposes, interactions are modelled
    as direct method calls.
    """

    def __init__(self, ip: str, port: int) -> None:
        self.ip: str = ip
        self.port: int = port
        # Compute a 32‑bit node ID from the IP and port.
        self.node_id: int = calcular_hash(f"{ip}:{port}")
        # Successor and predecessor pointers; set by ``add_node``.
        self.successor: Node = self
        self.predecessor: Node = self
        # Data store for keys and values.
        self.data: Dict[str, str] = {}
        # Last heartbeat time for critical neighbours.
        self._last_heartbeat: Dict[Node, float] = {}

    def __repr__(self) -> str:  # pragma: no cover - representation for debugging only
        return f"Node({self.ip}:{self.port}, id={self.node_id})"

    # ----------------------- Heartbeat API -----------------------

    def receive_heartbeat(self, sender: "Node", timestamp: Optional[float] = None) -> None:
        """Record a heartbeat from ``sender`` at the given time.

        In a networked system this method would be invoked whenever a
        heartbeat message is received.  Here, tests call it directly.

        Args:
            sender: The neighbouring node sending the heartbeat.
            timestamp: The time the heartbeat was received.  If
                omitted, the current time is used.
        """
        if timestamp is None:
            timestamp = time.time()
        self._last_heartbeat[sender] = timestamp

    def check_failed_neighbours(self, timeout: float, current_time: Optional[float] = None) -> List["Node"]:
        """Return a list of neighbours whose heartbeats have expired.

        A neighbour is considered failed if the time elapsed since its
        last heartbeat exceeds the specified ``timeout``.  In a
        realistic system this would trigger removal of the neighbour
        from the ring and redistribution of its keys.

        Args:
            timeout: The maximum allowed time (in seconds) since the
                last heartbeat.  If the difference exceeds this
                threshold, the neighbour is deemed dead.
            current_time: Optionally override the notion of 'now' for
                reproducible tests.

        Returns:
            A list of neighbours considered failed.  Ordering is
            unspecified.
        """
        if current_time is None:
            current_time = time.time()
        failed: List[Node] = []
        for neighbour, last in list(self._last_heartbeat.items()):
            if current_time - last > timeout:
                failed.append(neighbour)
        return failed


def add_node(ring: List[Node], node: Node) -> None:
    """Insert ``node`` into the hash ring, updating pointers.

    The ring is maintained as a list sorted by ``node_id``.  This
    function inserts the new node into the appropriate position so
    that the list remains ordered.  It also updates the successor and
    predecessor pointers of the new node and its neighbours.  If this
    is the first node, it becomes its own successor and predecessor.

    Args:
        ring: The current list of nodes in the ring, sorted by ID.
        node: The node to insert.
    """
    if not ring:
        # First node forms a trivial ring with itself.
        ring.append(node)
        node.successor = node
        node.predecessor = node
        return
    # Find insertion point by ID.
    index = 0
    while index < len(ring) and ring[index].node_id < node.node_id:
        index += 1
    ring.insert(index, node)
    # Determine successor and predecessor in the updated ring.
    succ = ring[(index + 1) % len(ring)]
    pred = ring[(index - 1) % len(ring)]
    # Update the new node's pointers.
    node.successor = succ
    node.predecessor = pred
    # Update neighbours' pointers to include the new node.
    pred.successor = node
    succ.predecessor = node


def find_successor(ring: List[Node], key_id: int) -> Node:
    """Locate the node responsible for ``key_id``.

    In a consistent hashing scheme, a key is stored on the first node
    whose ID is greater than or equal to the key's hash.  If no such
    node exists (i.e., the key hash is larger than all node IDs), the
    first node in the ring is responsible.

    Args:
        ring: A list of nodes sorted by ``node_id``.
        key_id: The hashed key to locate.

    Returns:
        The node responsible for the given key.
    """
    if not ring:
        raise ValueError("ring is empty")
    for node in ring:
        if key_id <= node.node_id:
            return node
    # Wrap around to the first node if the key's ID exceeds all node IDs.
    return ring[0]


def put_value(ring: List[Node], key: str, value: str, replication: int = 2) -> None:
    """Store ``value`` for ``key`` on the responsible node and its successors.

    The key is hashed to determine the responsible node in the ring.  The
    value is stored on that node and on ``replication - 1`` additional
    successors (for a total of ``replication`` replicas).  If the ring
    contains fewer nodes than the replication factor, the value is
    stored on every node.

    Args:
        ring: List of nodes in the ring (sorted by ID).
        key: The key to store.
        value: The value to associate with the key.
        replication: The number of replicas to store.  Must be at least 1.
    """
    if replication < 1:
        raise ValueError("replication factor must be >= 1")
    if not ring:
        raise ValueError("ring is empty")
    key_id = calcular_hash(key)
    primary = find_successor(ring, key_id)
    # Determine the index of the primary node.
    index = ring.index(primary)
    # Number of nodes to replicate on is min(replication, len(ring)).
    total = min(replication, len(ring))
    for i in range(total):
        target = ring[(index + i) % len(ring)]
        target.data[key] = value


def get_value(ring: List[Node], key: str) -> Optional[str]:
    """Retrieve the value for ``key`` from the ring.

    This function first identifies the primary node responsible for the
    key using the hashing scheme.  If the primary does not have the
    key (perhaps it failed after replication), the search continues
    along successors until the key is found or all nodes have been
    inspected.

    Args:
        ring: List of nodes in the ring (sorted by ID).
        key: The key to look up.

    Returns:
        The associated value if found, otherwise ``None``.
    """
    if not ring:
        return None
    key_id = calcular_hash(key)
    primary = find_successor(ring, key_id)
    index = ring.index(primary)
    # Walk the ring to find the key.
    for i in range(len(ring)):
        node = ring[(index + i) % len(ring)]
        if key in node.data:
            return node.data[key]
    return None


def flood_search(start: Node, key: str, ttl: int) -> Optional[str]:
    """Perform a flooding search for ``key`` starting from ``start``.

    The search forwards the query to the start node's successor and
    predecessor recursively until the TTL (time‑to‑live) reaches zero.
    To avoid revisiting the same nodes, a set of visited nodes is
    maintained.  When the key is found, its associated value is
    returned immediately.  If the TTL expires without locating the key
    the function returns ``None``.

    Args:
        start: The node initiating the search.
        key: The key to find.
        ttl: Maximum number of hops to follow.  Must be non‑negative.

    Returns:
        The associated value if found within the TTL, otherwise ``None``.
    """
    if ttl < 0:
        raise ValueError("ttl must be >= 0")

    visited: Set[Node] = set()

    def _search(node: Node, remaining: int) -> Optional[str]:
        # If key is stored here, return it.
        if key in node.data:
            return node.data[key]
        if remaining == 0:
            return None
        visited.add(node)
        # Explore successor and predecessor (flooding to known neighbours).
        for neighbour in (node.successor, node.predecessor):
            if neighbour not in visited:
                result = _search(neighbour, remaining - 1)
                if result is not None:
                    return result
        return None

    return _search(start, ttl)
