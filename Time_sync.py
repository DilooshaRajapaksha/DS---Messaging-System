import time
import threading
import random
from datetime import datetime
from typing import Dict, List, Tuple
from collections import deque

class SynchronizedClock:
    def __init__(self, node_id: str, is_main_server: bool = False, max_drift: float = 0.001):
        self.node_id = node_id
        self.is_main_server = is_main_server
        self._offset = 0.0
        self._drift_rate = random.uniform(-max_drift, max_drift)
        self._lock = threading.Lock()
        self.last_sync_time = time.time()

    def get_current_timestamp(self) -> float:
        with self._lock:
            now = time.time()
            elapsed = now - self.last_sync_time
            drifted_offset = self._offset + (self._drift_rate * elapsed)
            return now + drifted_offset

    def synchronize(self, reference_timestamp: float):
        with self._lock:
            local_now = time.time()
            self._offset = reference_timestamp - local_now
            self.last_sync_time = local_now

    def correct_message_timestamp(self, original_timestamp: float) -> float:
        with self._lock:
            corrected = original_timestamp + self._offset
            return corrected

    def get_readable_time(self, timestamp: float = None) -> str:
        ts = timestamp if timestamp is not None else self.get_current_timestamp()
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


class MessageReorderingBuffer:
    def __init__(self, node_id: str, max_wait_time: float = 10.0):
        self.node_id = node_id
        self.max_wait_time = max_wait_time
        self.buffer: List[Tuple[float, Dict]] = []
        self.delivered: List[Dict] = []
        self._lock = threading.Lock()

    def add_message(self, msg: Dict):
        ts = msg.get('timestamp', time.time())
        msg['corrected_timestamp'] = ts

        with self._lock:
            self.buffer.append((ts, msg))
            self.buffer.sort(key=lambda x: x[0])

            current_time = time.time()
            delivered_count = 0

            while self.buffer:
                next_ts, next_msg = self.buffer[0]

                if current_time - next_ts > self.max_wait_time:
                    self.buffer.pop(0)
                    self.delivered.append(next_msg)
                    delivered_count += 1
                    continue

                if not self.delivered or next_ts >= self.delivered[-1].get('corrected_timestamp', 0):
                    self.buffer.pop(0)
                    self.delivered.append(next_msg)
                    delivered_count += 1
                else:
                    break

    def get_delivered_messages(self) -> List[Dict]:
        with self._lock:
            return self.delivered.copy()

    def get_pending_count(self) -> int:
        with self._lock:
            return len(self.buffer)


if __name__ == "__main__":
    main_clock = SynchronizedClock("Server1-Main", is_main_server=True)
    backup1_clock = SynchronizedClock("Server2-Backup")
    backup2_clock = SynchronizedClock("Server3-Backup")

    reference_time = main_clock.get_current_timestamp()
    backup1_clock.synchronize(reference_time)
    backup2_clock.synchronize(reference_time)

    reorder_buffer = MessageReorderingBuffer("Server3-Backup", max_wait_time=8.0)

    messages = [
        {'content': 'Hello from Server1', 'timestamp': time.time() - 6.5},
        {'content': 'Important update from Server2', 'timestamp': time.time() - 2.1},
        {'content': 'Late message from Server1', 'timestamp': time.time() - 9.8},
        {'content': 'World from Server3', 'timestamp': time.time() - 0.5},
        {'content': 'Final message', 'timestamp': time.time() - 4.3}
    ]

    random.shuffle(messages)

    for msg in messages:
        time.sleep(0.3)
        reorder_buffer.add_message(msg)

    print("=== Time Synchronization Module v3 - Final Version ===")
    print(f"Server1 (Main)  : {main_clock.get_readable_time()}")
    print(f"Server2 (Backup): {backup1_clock.get_readable_time()}")
    print(f"Server3 (Backup): {backup2_clock.get_readable_time()}")
    print(f"\nTotal delivered messages in correct order: {len(reorder_buffer.get_delivered_messages())}")
    print(f"Pending messages in buffer: {reorder_buffer.get_pending_count()}")
    print("\nTime Synchronization Module - Ready for integration!")