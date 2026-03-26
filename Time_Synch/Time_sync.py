import time
import threading
from datetime import datetime
from typing import Dict

class SynchronizedClock:
   
    def __init__(self, node_id: str, is_main_server: bool = False):
        self.node_id = node_id
        self.is_main_server = is_main_server
        self._offset = 0.0                    # seconds difference from true time
        self._lock = threading.Lock()         # thread-safe
        print(f"[TimeSync] {node_id} initialized (Server1: {is_main_server})")

    def get_current_timestamp(self) -> float:
        
        with self._lock:
            return time.time() + self._offset

    def synchronize(self, reference_timestamp: float):
    
        with self._lock:
            local_now = time.time()
            self._offset = reference_timestamp - local_now
            print(f"[TimeSync] {self.node_id} synchronized to reference. Offset: {self._offset:.6f}s")

    def correct_message_timestamp(self, original_timestamp: float) -> float:
        
        with self._lock:
            corrected = original_timestamp + self._offset
            print(f"[TimeSync] {self.node_id} corrected timestamp {original_timestamp:.3f} → {corrected:.3f}")
            return corrected

    def get_readable_time(self, timestamp: float = None) -> str:
        
        ts = timestamp if timestamp is not None else self.get_current_timestamp()
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

if __name__ == "__main__":
    print("=== Time Synchronization Demo - 3 Servers (Main + 2 Backups) ===\n")
    
   
    main_clock = SynchronizedClock("Server1", is_main_server=True)
    backup1_clock = SynchronizedClock("Server2")
    backup2_clock = SynchronizedClock("Server3")
    
    reference_time = main_clock.get_current_timestamp()
    print(f"Main Server reference time: {main_clock.get_readable_time(reference_time)}\n")
    
    backup1_clock.synchronize(reference_time)
    backup2_clock.synchronize(reference_time)
    
    msg_timestamp = backup1_clock.get_current_timestamp()
    print(f"\nMessage sent from Server2 at: {backup1_clock.get_readable_time(msg_timestamp)}")
    
    
    corrected_ts = backup2_clock.correct_message_timestamp(msg_timestamp)
    print(f"Message received & corrected on Server3 at: {backup2_clock.get_readable_time(corrected_ts)}")
    

    print("\n--- Final synchronized timestamps ---")
    print(f"Server1     : {main_clock.get_readable_time()}")
    print(f"Server2 : {backup1_clock.get_readable_time()}")
    print(f"Server3 : {backup2_clock.get_readable_time()}")
    print("\nTime Synchronization Module ready for integration!")