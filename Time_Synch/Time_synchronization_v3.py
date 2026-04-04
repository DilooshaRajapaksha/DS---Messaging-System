import time
import threading
import random
from datetime import datetime
from typing import Dict, List, Tuple
from collections import deque

class SynchronizedClock:
    """
    Enhanced Synchronized Clock with simulated clock drift and NTP-style synchronization.
    This represents Task 3 - Time Synchronization.
    """

    def __init__(self, node_id: str, is_main_server: bool = False, max_drift: float = 0.001):
        self.node_id = node_id
        self.is_main_server = is_main_server
        self._offset = 0.0
        self._drift_rate = random.uniform(-max_drift, max_drift)  # Simulated hardware drift
        self._lock = threading.Lock()
        self.last_sync_time = time.time()
        
        print(f"[TimeSync] {node_id} initialized (Main: {is_main_server}, Drift rate: {self._drift_rate:.6f}s/s)")

    def get_current_timestamp(self) -> float:
        """Return current synchronized timestamp with simulated drift."""
        with self._lock:
            now = time.time()
            elapsed = now - self.last_sync_time
            drifted_offset = self._offset + (self._drift_rate * elapsed)
            return now + drifted_offset

    def synchronize(self, reference_timestamp: float):
        """Perform NTP-like synchronization with reference server."""
        with self._lock:
            local_now = time.time()
            self._offset = reference_timestamp - local_now
            self.last_sync_time = local_now
            print(f"[TimeSync] {self.node_id} synchronized with reference. New offset: {self._offset:.6f}s")

    def correct_message_timestamp(self, original_timestamp: float) -> float:
        """Correct incoming message timestamp using our synchronized clock."""
        with self._lock:
            corrected = original_timestamp + self._offset
            print(f"[TimeSync] {self.node_id} corrected timestamp {original_timestamp:.3f} → {corrected:.3f}")
            return corrected

    def get_readable_time(self, timestamp: float = None) -> str:
        ts = timestamp if timestamp is not None else self.get_current_timestamp()
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


class MessageReorderingBuffer:
    """
    Advanced message reordering buffer with timeout to handle late messages gracefully.
    Prevents indefinite buffering in case of very delayed messages.
    """

    def __init__(self, node_id: str, max_wait_time: float = 10.0):
        self.node_id = node_id
        self.max_wait_time = max_wait_time  # seconds to wait for missing messages
        self.buffer: List[Tuple[float, Dict]] = []  # (timestamp, message)
        self.delivered: List[Dict] = []
        self._lock = threading.Lock()
        print(f"[Reordering] {node_id} buffer initialized (max wait: {max_wait_time}s)")

    def add_message(self, msg: Dict):
        """Add message and deliver all in-order messages within the wait window."""
        ts = msg.get('timestamp', time.time())
        msg['corrected_timestamp'] = ts  # will be updated if needed

        with self._lock:
            self.buffer.append((ts, msg))
            # Sort buffer by original timestamp
            self.buffer.sort(key=lambda x: x[0])

            current_time = time.time()
            delivered_count = 0

            # Deliver messages that are in order and not too far in the future
            while self.buffer:
                next_ts, next_msg = self.buffer[0]
                
                # If message is too old (way past max_wait_time), deliver anyway
                if current_time - next_ts > self.max_wait_time:
                    self.buffer.pop(0)
                    self.delivered.append(next_msg)
                    delivered_count += 1
                    print(f"[Reordering] {self.node_id} DELIVERED (late/timeout): "
                          f"'{next_msg.get('content', 'no-content')}' | ts={next_ts:.3f}")
                    continue

                # Normal in-order delivery
                if not self.delivered or next_ts >= self.delivered[-1].get('corrected_timestamp', 0):
                    self.buffer.pop(0)
                    self.delivered.append(next_msg)
                    delivered_count += 1
                    print(f"[Reordering] {self.node_id} DELIVERED (in-order): "
                          f"'{next_msg.get('content', 'no-content')}' | ts={next_ts:.3f}")
                else:
                    break  # wait for earlier message

            if delivered_count > 0:
                print(f"[Reordering] {self.node_id} Delivered {delivered_count} messages this batch.")

    def get_delivered_messages(self) -> List[Dict]:
        with self._lock:
            return self.delivered.copy()

    def get_pending_count(self) -> int:
        with self._lock:
            return len(self.buffer)


# ==================== TESTING / DEMO ====================

if __name__ == "__main__":
    print("=== Time Synchronization Module v3 - Enhanced Version (Third Commit) ===\n")
    
    # Initialize clocks
    main_clock = SynchronizedClock("Server1-Main", is_main_server=True)
    backup1_clock = SynchronizedClock("Server2-Backup")
    backup2_clock = SynchronizedClock("Server3-Backup")
    
    # Simulate periodic synchronization (like real NTP)
    print("Performing initial synchronization...")
    reference_time = main_clock.get_current_timestamp()
    backup1_clock.synchronize(reference_time)
    backup2_clock.synchronize(reference_time)
    
    # Create reordering buffer for one of the servers
    reorder_buffer = MessageReorderingBuffer("Server3-Backup", max_wait_time=8.0)
    
    print("\n=== Simulating out-of-order message arrivals due to network delays ===\n")
    
    # Simulate messages with different delays
    messages = [
        {'content': 'Hello from Server1', 'timestamp': time.time() - 6.5},
        {'content': 'Important update from Server2', 'timestamp': time.time() - 2.1},
        {'content': 'Late message from Server1', 'timestamp': time.time() - 9.8},
        {'content': 'World from Server3', 'timestamp': time.time() - 0.5},
        {'content': 'Final message', 'timestamp': time.time() - 4.3}
    ]
    
    # Shuffle arrival order to simulate network disorder
    random.shuffle(messages)
    
    print("Arrival order (simulated network disorder):")
    for i, msg in enumerate(messages, 1):
        print(f"  {i}. {msg['content']} (original ts offset: {msg['timestamp'] - (time.time()-10):+.1f}s)")
    
    print("\nProcessing messages...\n")
    
    for msg in messages:
        time.sleep(0.3)  # simulate network arrival time
        reorder_buffer.add_message(msg)
    
    print("\n" + "="*60)
    print("FINAL RESULTS")
    print("="*60)
    print(f"Server1 (Main)  : {main_clock.get_readable_time()}")
    print(f"Server2 (Backup): {backup1_clock.get_readable_time()}")
    print(f"Server3 (Backup): {backup2_clock.get_readable_time()}")
    print(f"\nTotal delivered messages in correct order: {len(reorder_buffer.get_delivered_messages())}")
    print(f"Pending messages in buffer: {reorder_buffer.get_pending_count()}")
    
    print("\n=== Time Synchronization Module v3 - Ready for team integration! ===")
    print("Features implemented:")
    print("- NTP-style synchronization with offset")
    print("- Simulated clock drift")
    print("- Timestamp correction")
    print("- Smart message reordering with timeout")
    print("- Detailed logging for debugging")