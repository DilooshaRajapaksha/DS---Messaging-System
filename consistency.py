"""
consistency.py — Quorum-Based Consistency & Deduplication
==========================================================
"""

import threading
from typing import List, Dict, Optional, Tuple
from storage import Message, MessageStore, ReplicationStatus
from replication import ReplicationConfig, MockReplicationManager


# ─────────────────────────────────────────────
# 1. QuorumConfig — defines the quorum rules
# ─────────────────────────────────────────────

class QuorumConfig:
    """
    Decides how many servers must agree for a read or write to be valid.

    The golden rule of quorum:
        quorum = (total_servers // 2) + 1

    So for 3 servers:
        quorum = (3 // 2) + 1 = 2

    Write Quorum (W) → how many servers must SAVE the message
    Read  Quorum (R) → how many servers must RESPOND to a read query
    
    Rule: W + R > N  (where N = total servers)
    This guarantees at least one server in every read has the latest write.
    
    For N=3: W=2, R=2 → 2+2=4 > 3 ✅
    """

    def __init__(self, total_servers: int):
        self.total_servers  = total_servers
        self.write_quorum   = (total_servers // 2) + 1   # minimum confirmations for a safe write
        self.read_quorum    = (total_servers // 2) + 1   # minimum responses for a safe read

        print(f"[Consistency] QuorumConfig: {total_servers} servers, "
              f"write_quorum={self.write_quorum}, read_quorum={self.read_quorum}")

    def is_write_quorum_met(self, confirmations: int) -> bool:
        """Did enough servers confirm the write?"""
        return confirmations >= self.write_quorum

    def is_read_quorum_met(self, responses: int) -> bool:
        """Did enough servers respond to the read?"""
        return responses >= self.read_quorum


# ─────────────────────────────────────────────
# 2. ConsistencyManager — the brain of Part 3
# ─────────────────────────────────────────────

class ConsistencyManager:

    def __init__(self, stores: Dict[str, MessageStore], quorum_config: QuorumConfig):
        """
        stores         → { server_id: MessageStore } — all server stores
        quorum_config  → the quorum rules
        """
        self.stores  = stores
        self.quorum  = quorum_config
        self._lock   = threading.Lock()

        # Deduplication registry: set of message_ids we've already processed
        # This lives in memory — in production this would be a distributed cache
        self._seen_ids: set = set()

        print(f"[Consistency] ConsistencyManager ready with {len(stores)} stores")

    # ── Deduplication ─────────────────────────

    def is_duplicate(self, message_id: str) -> bool:
        
        return message_id in self._seen_ids

    def register_message(self, message_id: str):
        """Mark a message_id as seen so future duplicates are caught."""
        with self._lock:
            self._seen_ids.add(message_id)

    # ── Quorum Write ──────────────────────────

    def quorum_write(self, message: Message, alive_server_ids: List[str]) -> Tuple[bool, int]:
        
        # Step 1: Duplicate check
        if self.is_duplicate(message.message_id):
            print(f"[Consistency] 🚫 DUPLICATE detected — rejecting {message.message_id[:8]}...")
            return False, 0

        # Step 2 & 3: Try writing to each alive server
        confirmations = 0
        for server_id in alive_server_ids:
            store = self.stores.get(server_id)
            if store and store.save(message):
                confirmations += 1
                print(f"[Consistency] ✅ Write confirmed by {server_id} "
                      f"({confirmations}/{self.quorum.write_quorum} needed)")

        # Step 4: Check if quorum is met
        success = self.quorum.is_write_quorum_met(confirmations)

        if success:
            # Step 5: Register to prevent future duplicates
            self.register_message(message.message_id)
            print(f"[Consistency] 🎉 Quorum WRITE SUCCESS — "
                  f"{confirmations}/{self.quorum.total_servers} servers confirmed")
        else:
            print(f"[Consistency] ❌ Quorum WRITE FAILED — "
                  f"only {confirmations}/{self.quorum.write_quorum} needed servers responded")

        return success, confirmations

    # ── Quorum Read ───────────────────────────

    def quorum_read(self, recipient: str, alive_server_ids: List[str]) -> Tuple[bool, List[Message]]:
        
        responses: Dict[str, List[Message]] = {}

        # Step 1 & 2: Collect responses from alive servers
        for server_id in alive_server_ids:
            store = self.stores.get(server_id)
            if store:
                msgs = store.get_by_recipient(recipient)
                responses[server_id] = msgs
                print(f"[Consistency] 📖 Read from {server_id}: {len(msgs)} messages for '{recipient}'")

        # Step 3: Check quorum
        if not self.quorum.is_read_quorum_met(len(responses)):
            print(f"[Consistency] ❌ Quorum READ FAILED — "
                  f"only {len(responses)}/{self.quorum.read_quorum} servers responded")
            return False, []

        # Step 4: Merge — for each message_id, keep the highest version
        merged: Dict[str, Message] = {}
        for server_id, messages in responses.items():
            for msg in messages:
                existing = merged.get(msg.message_id)
                if not existing or msg.version > existing.version:
                    # This server has a newer version → use it
                    if existing:
                        print(f"[Consistency] 🔄 Version conflict on {msg.message_id[:8]}... "
                              f"— keeping v{msg.version} over v{existing.version}")
                    merged[msg.message_id] = msg

        # Step 5: Sort by timestamp and return
        final = sorted(merged.values(), key=lambda m: m.timestamp)
        print(f"[Consistency] ✅ Quorum READ SUCCESS — returning {len(final)} messages for '{recipient}'")
        return True, final

    # ── Consistency Report ────────────────────

    def consistency_report(self, alive_server_ids: List[str]) -> dict:
        
        server_message_ids: Dict[str, set] = {}

        for server_id in alive_server_ids:
            store = self.stores.get(server_id)
            if store:
                ids = {m.message_id for m in store.get_all()}
                server_message_ids[server_id] = ids

        # Find messages not present on all servers
        all_ids = set().union(*server_message_ids.values()) if server_message_ids else set()
        inconsistencies = []

        for msg_id in all_ids:
            has_it = [sid for sid, ids in server_message_ids.items() if msg_id in ids]
            missing = [sid for sid, ids in server_message_ids.items() if msg_id not in ids]
            if missing:
                inconsistencies.append({
                    "message_id": msg_id[:8] + "...",
                    "present_on": has_it,
                    "missing_on": missing,
                })

        report = {
            "total_unique_messages": len(all_ids),
            "server_counts":         {sid: len(ids) for sid, ids in server_message_ids.items()},
            "inconsistencies_found": len(inconsistencies),
            "inconsistencies":       inconsistencies,
            "fully_consistent":      len(inconsistencies) == 0,
        }
        return report


# ─────────────────────────────────────────────
# 3. Demo / Simulation
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Consistency & Quorum Demo ===\n")

    # Setup: 3 server stores + quorum config
    config       = ReplicationConfig()
    quorum_cfg   = QuorumConfig(total_servers=3)

    store1 = MessageStore("server1")
    store2 = MessageStore("server2")
    store3 = MessageStore("server3")
    stores = {"server1": store1, "server2": store2, "server3": store3}

    manager = ConsistencyManager(stores, quorum_cfg)

    # ── Scenario A: Normal quorum write ───────
    print("─" * 52)
    print("Scenario A: Quorum write — all servers alive")
    print("─" * 52)

    msg_a = Message(sender="Alice", recipient="Bob", content="Hey Bob! Meeting at 3pm?")
    success, confirms = manager.quorum_write(msg_a, alive_server_ids=["server1", "server2", "server3"])
    print(f"→ Write success: {success}, confirmations: {confirms}\n")

    # ── Scenario B: Quorum write — one server down ─
    print("─" * 52)
    print("Scenario B: Quorum write — server3 is DOWN")
    print("─" * 52)

    msg_b = Message(sender="Charlie", recipient="Diana", content="Diana, are you coming?")
    success, confirms = manager.quorum_write(msg_b, alive_server_ids=["server1", "server2"])
    print(f"→ Write success: {success}, confirmations: {confirms}\n")

    # ── Scenario C: Quorum write FAILS (too many servers down) ─
    print("─" * 52)
    print("Scenario C: Quorum write FAILS — only 1 server alive")
    print("─" * 52)

    msg_c = Message(sender="Eve", recipient="Frank", content="This might not make it!")
    success, confirms = manager.quorum_write(msg_c, alive_server_ids=["server1"])
    print(f"→ Write success: {success}, confirmations: {confirms}\n")

    # ── Scenario D: Duplicate rejection ───────
    print("─" * 52)
    print("Scenario D: Sending the SAME message again (duplicate)")
    print("─" * 52)

    success, confirms = manager.quorum_write(msg_a, alive_server_ids=["server1", "server2", "server3"])
    print(f"→ Write success: {success}, confirmations: {confirms}\n")

    # ── Scenario E: Quorum read ───────────────
    print("─" * 52)
    print("Scenario E: Quorum read — Bob reads his messages")
    print("─" * 52)

    # Manually sync server3 (simulate it catching up after being offline in B)
    store3.save(msg_b)

    quorum_met, messages = manager.quorum_read("Bob", alive_server_ids=["server1", "server2", "server3"])
    print(f"→ Quorum met: {quorum_met}, messages found: {len(messages)}")
    for m in messages:
        print(f"   [{m.timestamp:.3f}] {m.sender} → {m.recipient}: {m.content}")

    # ── Scenario F: Consistency report ────────
    print("\n" + "─" * 52)
    print("Scenario F: Consistency report across all servers")
    print("─" * 52)

    report = manager.consistency_report(alive_server_ids=["server1", "server2", "server3"])
    print(f"  Total unique messages : {report['total_unique_messages']}")
    print(f"  Server message counts : {report['server_counts']}")
    print(f"  Inconsistencies found : {report['inconsistencies_found']}")
    print(f"  Fully consistent      : {report['fully_consistent']}")
    if report["inconsistencies"]:
        print("  Details:")
        for inc in report["inconsistencies"]:
            print(f"    {inc['message_id']} — present on {inc['present_on']}, missing on {inc['missing_on']}")