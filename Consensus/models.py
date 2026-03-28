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
        """Convert message to a plain dictionary."""
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
        """Rebuild a Message object from a dictionary."""
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

    def save(self, message: Message) -> bool:
        existing = self._store.get(message.message_id)
        if existing:
            if message.version <= existing.version:
                return False
        self._store[message.message_id] = message
        return True

    def get_all(self) -> List[Message]:
        return sorted(self._store.values(), key=lambda m: m.timestamp)
