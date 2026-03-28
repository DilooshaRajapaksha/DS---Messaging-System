import time
import threading
from datetime import datetime
from typing import Dict, List, Tuple
from collections import deque

class SynchronizedClock:
    def __init__(self, node_id: str, is_main_server: bool = False):
        self.node_id = node_id
        self.is_main_server = is_main_server
        self._offset = 0.0
        self._lock = threading.Lock()
        print(f"[TimeSync] {node_id} initialized (Main Server: {is_main_server})")

    def get_current_timestamp(self) -> float:
        with self._lock:
            return time.time() + self._offset

    def synchronize(self, reference_timestamp: float):
        with self._lock:
            local_now = time.time()
            self._offset = reference_timestamp - local_now
            print(f"[TimeSync] {self.node_id} synchronized. Offset: {self._offset:.6f}s")

    def correct_message_timestamp(self, original_timestamp: float) -> float:
        with self._lock:
            corrected = original_timestamp + self._offset
            print(f"[TimeSync] {self.node_id} corrected timestamp {original_timestamp:.3f} → {corrected:.3f}")
            return corrected

    def get_readable_time(self, timestamp: float = None) -> str:
        ts = timestamp if timestamp is not None else self.get_current_timestamp()
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

#Task 3: Message Reordering Buffer
class MessageReorderingBuffer:
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.buffer: deque[Tuple[float, Dict]] = deque()
        self.delivered: List[Dict] = []
        self._lock = threading.Lock()

    def add_message(self, msg: Dict):
        """Add message with its original timestamp and reorder buffer."""
        ts = msg.get('timestamp', time.time())
        with self._lock:
            self.buffer.append((ts, msg))
            # Sort by timestamp
            sorted_buffer = sorted(self.buffer, key=lambda x: x[0])
            self.buffer = deque(sorted_buffer)

            # Deliver all messages that are now in correct order
            while self.buffer:
                next_ts, next_msg = self.buffer.popleft()
                self.delivered.append(next_msg)
                print(f"[Reordering] {self.node_id} DELIVERED (in-order): "
                      f"'{next_msg.get('content', 'no-content')}' | ts={next_ts:.3f}")

    def get_delivered_messages(self) -> List[Dict]:
        with self._lock:
            return self.delivered.copy()


# Testing
if __name__ == "__main__":
    print("=== Time Synchronization v2 - With Message Reordering (Second Commit) ===\n")
    
    main_clock = SynchronizedClock("Server1", is_main_server=True)
    backup1_clock = SynchronizedClock("Server2")
    backup2_clock = SynchronizedClock("Server3")
    
    # One-time sync (from your previous version)
    reference_time = main_clock.get_current_timestamp()
    backup1_clock.synchronize(reference_time)
    backup2_clock.synchronize(reference_time)
    
    # New reordering buffer (Task 3)
    reorder_buffer = MessageReorderingBuffer("Server3")
    
    # Simulate out of order messages (network delay)
    msg1 = {'content': 'Hello from Server2', 'timestamp': time.time() - 5.2}
    msg2 = {'content': 'World from Server1', 'timestamp': time.time() - 1.8}
    msg3 = {'content': 'Late message from Server2', 'timestamp': time.time() - 8.7}
    
    print("Simulating out-of-order arrival...\n")
    reorder_buffer.add_message(msg3)   # arrives first (oldest)
    reorder_buffer.add_message(msg1)
    reorder_buffer.add_message(msg2)   # arrives last
    
    print("\n--- Final synchronized timestamps ---")
    print(f"Server1 : {main_clock.get_readable_time()}")
    print(f"Server2 : {backup1_clock.get_readable_time()}")
    print(f"Server3 : {backup2_clock.get_readable_time()}")
    print(f"\nDelivered messages in correct order: {len(reorder_buffer.get_delivered_messages())}")
    print("Time Synchronization Module - Ready for integration!")