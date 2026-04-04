import json
import os

def load_messages(filename):
    if os.path.exists(filename):
        with open(filename, "r") as f:
            return json.load(f)
    return []

def save_messages(filename, messages):
    with open(filename, "w") as f:
        json.dump(messages, f, indent=2)
import uuid
import time
from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from enum import Enum


# ─────────────────────────────────────────────
# 1. Enums — fixed set of allowed values
# ─────────────────────────────────────────────

class ReplicationStatus(Enum):
    
    PENDING    = "pending"
    PARTIAL    = "partial"
    REPLICATED = "replicated"


class MessageStatus(Enum):
    
    STORED    = "stored"
    DELIVERED = "delivered"
    FAILED    = "failed"


# ─────────────────────────────────────────────
# 2. Message — the core data structure
# ─────────────────────────────────────────────

@dataclass
class Message:
    
    message_id:          str              = field(default_factory=lambda: str(uuid.uuid4()))
    sender:              str              = ""
    recipient:           str             = ""
    content:             str              = ""
    timestamp:           float            = field(default_factory=time.time)
    version:             int              = 1
    replication_status:  ReplicationStatus = ReplicationStatus.PENDING
    replicated_to:       List[str]        = field(default_factory=list)
    status:              MessageStatus    = MessageStatus.STORED

    def to_dict(self) -> dict:
        """Convert message to a plain dictionary (for sending over HTTP/JSON)."""
        return {
            "message_id":         self.message_id,
            "sender":             self.sender,
            "recipient":          self.recipient,
            "content":            self.content,
            "timestamp":          self.timestamp,
            "timestamp_readable": datetime.fromtimestamp(self.timestamp).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "version":            self.version,
            "replication_status": self.replication_status.value,
            "replicated_to":      self.replicated_to,
            "status":             self.status.value,
        }

    @staticmethod
    def from_dict(data: dict) -> "Message":
        """Rebuild a Message object from a dictionary (received over HTTP/JSON)."""
        return Message(
            message_id=         data["message_id"],
            sender=             data["sender"],
            recipient=          data["recipient"],
            content=            data["content"],
            timestamp=          data["timestamp"],
            version=            data.get("version", 1),
            replication_status= ReplicationStatus(data.get("replication_status", "pending")),
            replicated_to=      data.get("replicated_to", []),
            status=             MessageStatus(data.get("status", "stored")),
        )


# ─────────────────────────────────────────────
# 3. MessageStore — in-memory storage per server
# ─────────────────────────────────────────────

class MessageStore:
    

    def __init__(self, server_id: str):
        self.server_id = server_id
        # Main storage: { message_id → Message }
        self._store: Dict[str, Message] = {}
        print(f"[Storage] MessageStore initialized on {server_id}")

    # ── Write Operations ──────────────────────

    def save(self, message: Message) -> bool:
        
        existing = self._store.get(message.message_id)

        if existing:
            if message.version <= existing.version:
                # Duplicate or outdated — skip it
                print(f"[Storage] {self.server_id} SKIPPED duplicate message {message.message_id[:8]}...")
                return False
            else:
                # Newer version — update (e.g., edited message)
                print(f"[Storage] {self.server_id} UPDATED message {message.message_id[:8]}... (v{existing.version} → v{message.version})")
        else:
            print(f"[Storage] {self.server_id} SAVED new message {message.message_id[:8]}... from {message.sender}")

        self._store[message.message_id] = message
        return True

    def mark_replicated(self, message_id: str, server_id: str, total_servers: int):
        
        msg = self._store.get(message_id)
        if not msg:
            return

        if server_id not in msg.replicated_to:
            msg.replicated_to.append(server_id)

        # Update status: if all servers have it → REPLICATED, else PARTIAL
        if len(msg.replicated_to) >= total_servers:
            msg.replication_status = ReplicationStatus.REPLICATED
        else:
            msg.replication_status = ReplicationStatus.PARTIAL

        print(f"[Storage] {self.server_id} replication status for {message_id[:8]}...: "
              f"{msg.replication_status.value} ({len(msg.replicated_to)}/{total_servers} servers)")

    # ── Read Operations ───────────────────────

    def get(self, message_id: str) -> Optional[Message]:
        """Retrieve a single message by its ID."""
        return self._store.get(message_id)

    def get_all(self) -> List[Message]:
        """Return all messages, sorted by timestamp (oldest first)."""
        return sorted(self._store.values(), key=lambda m: m.timestamp)

    def get_by_recipient(self, recipient: str) -> List[Message]:
        """Return all messages for a specific recipient, sorted by timestamp."""
        return sorted(
            [m for m in self._store.values() if m.recipient == recipient],
            key=lambda m: m.timestamp
        )

    def get_pending_replication(self) -> List[Message]:
        """Return messages that haven't been fully replicated yet (for retry logic)."""
        return [m for m in self._store.values()
                if m.replication_status != ReplicationStatus.REPLICATED]

    # ── Utility ───────────────────────────────

    def has(self, message_id: str) -> bool:
        """Check if a message exists (used for deduplication checks)."""
        return message_id in self._store

    def count(self) -> int:
        """Total messages stored."""
        return len(self._store)

    def summary(self) -> dict:
        """Quick stats about this store — useful for debugging and the report."""
        statuses = [m.replication_status.value for m in self._store.values()]
        return {
            "server_id":  self.server_id,
            "total":      self.count(),
            "replicated": statuses.count("replicated"),
            "partial":    statuses.count("partial"),
            "pending":    statuses.count("pending"),
        }


# ─────────────────────────────────────────────
# 4. Demo / Quick Test
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Storage Layer Demo ===\n")

    # Each server gets its own store
    store1 = MessageStore("server1")
    store2 = MessageStore("server2")
    store3 = MessageStore("server3")

    # --- Create a message (as if a client sent it to server1) ---
    msg = Message(
        sender="Alice",
        recipient="Bob",
        content="Hello Bob! This is a distributed message.",
        timestamp=time.time()
    )
    print(f"\nCreated message ID: {msg.message_id}")
    print(f"Timestamp: {datetime.fromtimestamp(msg.timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")

    # --- Save to primary (server1) ---
    print("\n--- Saving to Server1 (primary) ---")
    store1.save(msg)
    store1.mark_replicated(msg.message_id, "server1", total_servers=3)

    # --- Replicate to server2 ---
    print("\n--- Replicating to Server2 ---")
    store2.save(msg)
    store1.mark_replicated(msg.message_id, "server2", total_servers=3)

    # --- Simulate duplicate arrival at server2 ---
    print("\n--- Simulating duplicate message arriving at Server2 ---")
    store2.save(msg)  # Should be skipped

    # --- Replicate to server3 ---
    print("\n--- Replicating to Server3 ---")
    store3.save(msg)
    store1.mark_replicated(msg.message_id, "server3", total_servers=3)

    # --- Final status ---
    print("\n--- Final Message Status ---")
    final = store1.get(msg.message_id)
    if final:
        for key, val in final.to_dict().items():
            print(f"  {key}: {val}")

    # --- Store summaries ---
    print("\n--- Store Summaries ---")
    for store in [store1, store2, store3]:
        print(f"  {store.summary()}")
