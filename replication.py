"""
replication.py — Primary-Backup Replication Engine
====================================================
Part 2 of Data Replication & Consistency module.
"""

import time
import json
import urllib.request
import urllib.error
import threading
from typing import List, Dict, Optional
from storage import Message, MessageStore, ReplicationStatus


# ─────────────────────────────────────────────
# 1. ReplicationConfig — knows all server addresses
# ─────────────────────────────────────────────

class ReplicationConfig:
    """
    In production these would be real IPs/ports.
    For local testing we use localhost with different ports:
        Server1 (primary) → http://localhost:8001
        Server2 (backup)  → http://localhost:8002
        Server3 (backup)  → http://localhost:8003
    """

    def __init__(self):
        # { server_id → base URL }
        self.servers: Dict[str, str] = {
            "server1": "http://localhost:8001",
            "server2": "http://localhost:8002",
            "server3": "http://localhost:8003",
        }
        self.primary_id = "server1"

    @property
    def backup_ids(self) -> List[str]:
        """Return all server IDs except the primary."""
        return [sid for sid in self.servers if sid != self.primary_id]

    def url_of(self, server_id: str, path: str) -> str:
        """Build a full URL for a server endpoint."""
        base = self.servers[server_id]
        return f"{base}{path}"


# ─────────────────────────────────────────────
# 2. ReplicationManager — does the actual copying
# ─────────────────────────────────────────────

class ReplicationManager:
    

    def __init__(self, local_store: MessageStore, config: ReplicationConfig, local_server_id: str):
        self.store           = local_store
        self.config          = config
        self.local_server_id = local_server_id

        # Retry queue: { message_id → [server_ids that still need this message] }
        self._retry_queue: Dict[str, List[str]] = {}
        self._lock = threading.Lock()

        # How many seconds to wait before retry
        self.retry_interval = 5

        print(f"[Replication] ReplicationManager started on {local_server_id}")

    # ── Public API ────────────────────────────

    def replicate(self, message: Message) -> Dict[str, bool]:
        """
        Push a message from primary to all backup servers.

        Steps:
        1. Save to local (primary) store first
        2. Try to send to each backup
        3. If a backup fails → add to retry queue
        4. Update replication status on the message

        Returns a dict of { server_id → success (True/False) }

        Example return:
            { "server2": True, "server3": False }
            → means server2 got it, server3 is down (will retry)
        """
        results = {}

        # Step 1: Save locally first (primary always has it)
        self.store.save(message)
        self.store.mark_replicated(message.message_id, self.local_server_id, len(self.config.servers))

        # Step 2: Send to each backup
        for backup_id in self.config.backup_ids:
            success = self._send_to_server(backup_id, message)
            results[backup_id] = success

            if success:
                # Backup confirmed → mark it
                self.store.mark_replicated(message.message_id, backup_id, len(self.config.servers))
                print(f"[Replication] ✅ Replicated to {backup_id}")
            else:
                # Backup failed → add to retry queue
                self._add_to_retry(message.message_id, backup_id)
                print(f"[Replication] ❌ Failed to replicate to {backup_id} — added to retry queue")

        return results

    def retry_pending(self):
        """
        Go through the retry queue and try to re-send messages
        to servers that were previously unreachable.

        Call this on a schedule (e.g., every 5 seconds) or when
        a server comes back online.

        Like a postman going back to deliver letters that couldn't
        be delivered earlier because the door was locked.
        """
        if not self._retry_queue:
            print("[Replication] Retry queue is empty — nothing to retry.")
            return

        print(f"[Replication] Retrying {len(self._retry_queue)} pending messages...")

        with self._lock:
            completed = []  # message_ids that are now fully replicated

            for message_id, pending_servers in self._retry_queue.items():
                message = self.store.get(message_id)
                if not message:
                    completed.append(message_id)  # message gone, remove from queue
                    continue

                still_pending = []
                for server_id in pending_servers:
                    if not self._is_server_alive(server_id):
                        still_pending.append(server_id)  # still down
                        print(f"[Replication] 🔄 {server_id} still down, keeping in queue")
                        continue

                    success = self._send_to_server(server_id, message)
                    if success:
                        self.store.mark_replicated(message_id, server_id, len(self.config.servers))
                        print(f"[Replication] ✅ Retry succeeded for {server_id} — message {message_id[:8]}...")
                    else:
                        still_pending.append(server_id)
                        print(f"[Replication] ❌ Retry failed again for {server_id}")

                if still_pending:
                    self._retry_queue[message_id] = still_pending
                else:
                    completed.append(message_id)

            # Remove fully replicated messages from retry queue
            for mid in completed:
                self._retry_queue.pop(mid, None)

    def start_background_retry(self):
        """
        Start a background thread that calls retry_pending()
        every `retry_interval` seconds automatically.

        This means even if you forget to call retry manually,
        the system will keep trying on its own.
        """
        def loop():
            while True:
                time.sleep(self.retry_interval)
                self.retry_pending()

        thread = threading.Thread(target=loop, daemon=True)
        thread.start()
        print(f"[Replication] Background retry started (every {self.retry_interval}s)")

    # ── Private Helpers ───────────────────────

    def _is_server_alive(self, server_id: str) -> bool:
        """
        Check if a backup server is reachable by calling its /heartbeat endpoint.
        This is your teammate's code from server1/2/3.py!

        Returns True if alive, False if down or unreachable.

        Like calling a branch on the phone to see if they're open.
        """
        url = self.config.url_of(server_id, "/heartbeat")
        try:
            req = urllib.request.urlopen(url, timeout=2)
            data = json.loads(req.read().decode())
            alive = data.get("status") == "alive"
            print(f"[Replication] 💓 Heartbeat {server_id}: {'alive' if alive else 'no response'}")
            return alive
        except Exception:
            print(f"[Replication] 💔 Heartbeat {server_id}: unreachable")
            return False

    def _send_to_server(self, server_id: str, message: Message) -> bool:
        """
        Send a message copy to a specific backup server via HTTP POST
        to its /replicate endpoint.

        Returns True if the backup accepted it, False otherwise.

        Like physically carrying a letter from Branch 1 to Branch 2.
        """
        # First check if it's alive (saves time vs waiting for timeout)
        if not self._is_server_alive(server_id):
            return False

        url = self.config.url_of(server_id, "/replicate")
        payload = json.dumps(message.to_dict()).encode("utf-8")

        try:
            req = urllib.request.Request(
                url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            response = urllib.request.urlopen(req, timeout=3)
            result = json.loads(response.read().decode())
            return result.get("success", False)

        except urllib.error.HTTPError as e:
            print(f"[Replication] HTTP error sending to {server_id}: {e.code}")
            return False
        except Exception as e:
            print(f"[Replication] Error sending to {server_id}: {e}")
            return False

    def _add_to_retry(self, message_id: str, server_id: str):
        """Add a server to the retry queue for a specific message."""
        with self._lock:
            if message_id not in self._retry_queue:
                self._retry_queue[message_id] = []
            if server_id not in self._retry_queue[message_id]:
                self._retry_queue[message_id].append(server_id)

    # ── Status / Debug ────────────────────────

    def status(self) -> dict:
        """Show current state of replication — useful for debugging and report."""
        return {
            "local_server":    self.local_server_id,
            "primary":         self.config.primary_id,
            "backups":         self.config.backup_ids,
            "retry_queue":     {mid[:8] + "...": servers
                                for mid, servers in self._retry_queue.items()},
            "store_summary":   self.store.summary(),
        }


# ─────────────────────────────────────────────
# 3. Demo / Simulation (no real servers needed)
# ─────────────────────────────────────────────

class MockReplicationManager(ReplicationManager):
    

    def __init__(self, local_store, config, local_server_id, server_states: Dict[str, bool]):
        super().__init__(local_store, config, local_server_id)
        self.server_states = server_states  # simulated alive/down states

    def _is_server_alive(self, server_id: str) -> bool:
        alive = self.server_states.get(server_id, False)
        print(f"[Replication] 💓 Heartbeat {server_id}: {'alive' if alive else 'DOWN (simulated)'}")
        return alive

    def _send_to_server(self, server_id: str, message: Message) -> bool:
        if not self._is_server_alive(server_id):
            return False
        # Simulate successful delivery
        print(f"[Replication] 📨 Mock-sent message to {server_id}")
        return True


# ─────────────────────────────────────────────
# 4. Run Demo
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Replication Engine Demo ===\n")

    config = ReplicationConfig()
    primary_store = MessageStore("server1")

    # ── Scenario A: All servers alive ─────────
    print("─" * 50)
    print("Scenario A: All servers alive")
    print("─" * 50)

    manager_a = MockReplicationManager(
        local_store=primary_store,
        config=config,
        local_server_id="server1",
        server_states={"server1": True, "server2": True, "server3": True}
    )

    msg_a = Message(sender="Alice", recipient="Bob", content="Hey Bob! Are you free tonight?")
    print(f"\nSending message: '{msg_a.content}'")
    results = manager_a.replicate(msg_a)
    print(f"Replication results: {results}")
    print(f"Status: {manager_a.status()}\n")

    # ── Scenario B: Server3 is DOWN ───────────
    print("─" * 50)
    print("Scenario B: Server3 is DOWN")
    print("─" * 50)

    primary_store_b = MessageStore("server1")
    manager_b = MockReplicationManager(
        local_store=primary_store_b,
        config=config,
        local_server_id="server1",
        server_states={"server1": True, "server2": True, "server3": False}
    )

    msg_b = Message(sender="Charlie", recipient="Diana", content="Server3 is down, will this reach Diana?")
    print(f"\nSending message: '{msg_b.content}'")
    results = manager_b.replicate(msg_b)
    print(f"Replication results: {results}")
    print(f"Status after failure: {manager_b.status()}")

    # ── Scenario C: Server3 comes back online → retry ─────
    print("\n─" * 50)
    print("Scenario C: Server3 comes back online → retrying...")
    print("─" * 50)

    manager_b.server_states["server3"] = True  # simulate server3 recovered
    manager_b.retry_pending()
    print(f"\nStatus after retry: {manager_b.status()}")