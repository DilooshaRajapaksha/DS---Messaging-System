import random
import time
import asyncio
import requests
import threading
from enum import Enum
from typing import List, Dict, Optional
from models import Message

class RaftState(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

class RaftNode:
    def __init__(self, node_id: str, peers: List[str]):
        self.node_id = node_id
        self.peers = peers  # List of peer URLs (e.g., http://localhost:8001)
        self.state = RaftState.FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.log = []  # Log entries: [{"term": t, "message": msg}]
        self.commit_index = -1
        self.last_applied = -1
        
        # Leader-specific state
        self.next_index = {}
        self.match_index = {}
        
        self.election_timeout = random.uniform(2.0, 4.0)
        self.last_heartbeat = time.time()
        self.heartbeat_interval = 0.5
        
        print(f"[Raft-{self.node_id}] Initialized as FOLLOWER")

    async def run(self):
        """Main loop for the Raft node."""
        while True:
            if self.state == RaftState.FOLLOWER:
                if time.time() - self.last_heartbeat > self.election_timeout:
                    print(f"[Raft-{self.node_id}] Election timeout! Starting election...")
                    await self.start_election()
            elif self.state == RaftState.LEADER:
                await self.send_heartbeats()
                await asyncio.sleep(self.heartbeat_interval)
            
            await asyncio.sleep(0.1)

    async def start_election(self):
        self.state = RaftState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        votes = 1
        
        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index]["term"] if last_log_index >= 0 else 0
        
        print(f"[Raft-{self.node_id}] Candidate for Term {self.current_term}")
        
        for peer in self.peers:
            try:
                # Use a separate thread or non-blocking request for voting
                resp = requests.post(f"{peer}/request_vote", json={
                    "term": self.current_term,
                    "candidate_id": self.node_id,
                    "last_log_index": last_log_index,
                    "last_log_term": last_log_term
                }, timeout=1)
                
                if resp.status_code == 200:
                    data = resp.json()
                    if data["vote_granted"]:
                        votes += 1
                    elif data["term"] > self.current_term:
                        self.current_term = data["term"]
                        self.state = RaftState.FOLLOWER
                        self.voted_for = None
            except:
                pass
        
        if votes > (len(self.peers) + 1) / 2:
            print(f"[Raft-{self.node_id}] Won election! Becoming LEADER for Term {self.current_term}")
            self.state = RaftState.LEADER
            self.next_index = {peer: len(self.log) for peer in self.peers}
            self.match_index = {peer: -1 for peer in self.peers}
        else:
            self.state = RaftState.FOLLOWER
            self.last_heartbeat = time.time()

    async def send_heartbeats(self):
        for peer in self.peers:
            try:
                prev_log_index = self.next_index.get(peer, 0) - 1
                prev_log_term = self.log[prev_log_index]["term"] if prev_log_index >= 0 else 0
                
                entries = self.log[self.next_index[peer]:]
                
                resp = requests.post(f"{peer}/append_entries", json={
                    "term": self.current_term,
                    "leader_id": self.node_id,
                    "prev_log_index": prev_log_index,
                    "prev_log_term": prev_log_term,
                    "entries": entries,
                    "leader_commit": self.commit_index
                }, timeout=1)
                
                if resp.status_code == 200:
                    data = resp.json()
                    if data["success"]:
                        self.next_index[peer] = len(self.log)
                        self.match_index[peer] = len(self.log) - 1
                    elif data["term"] > self.current_term:
                        self.current_term = data["term"]
                        self.state = RaftState.FOLLOWER
                        self.voted_for = None
                    else:
                        # Consistency check failed, decrement nextIndex
                        self.next_index[peer] = max(0, self.next_index[peer] - 1)
            except:
                pass
        
        # Update commit index if majority reached
        for i in range(len(self.log) - 1, self.commit_index, -1):
            if self.log[i]["term"] == self.current_term:
                count = 1
                for peer in self.peers:
                    if self.match_index.get(peer, -1) >= i:
                        count += 1
                if count > (len(self.peers) + 1) / 2:
                    self.commit_index = i
                    print(f"[Raft-{self.node_id}] Committed entry at index {i}")
                    break

    def handle_request_vote(self, term, candidate_id, last_log_index, last_log_term):
        if term > self.current_term:
            self.current_term = term
            self.state = RaftState.FOLLOWER
            self.voted_for = None
            
        vote_granted = False
        if term == self.current_term and (self.voted_for is None or self.voted_for == candidate_id):
            my_last_log_index = len(self.log) - 1
            my_last_log_term = self.log[my_last_log_index]["term"] if my_last_log_index >= 0 else 0
            
            # Check if candidate's log is at least as up-to-date as ours
            if last_log_term > my_last_log_term or (last_log_term == my_last_log_term and last_log_index >= my_last_log_index):
                vote_granted = True
                self.voted_for = candidate_id
                self.last_heartbeat = time.time()
                
        return {"term": self.current_term, "vote_granted": vote_granted}

    def handle_append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        if term > self.current_term:
            self.current_term = term
            self.state = RaftState.FOLLOWER
            self.voted_for = None
            
        if term < self.current_term:
            return {"term": self.current_term, "success": False}
        
        self.state = RaftState.FOLLOWER
        self.last_heartbeat = time.time()
        
        # Log consistency check
        if prev_log_index >= 0:
            if prev_log_index >= len(self.log) or self.log[prev_log_index]["term"] != prev_log_term:
                return {"term": self.current_term, "success": False}
        
        # Append new entries
        for i, entry in enumerate(entries):
            idx = prev_log_index + 1 + i
            if idx < len(self.log):
                if self.log[idx]["term"] != entry["term"]:
                    self.log = self.log[:idx]
                    self.log.append(entry)
            else:
                self.log.append(entry)
                
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            
        return {"term": self.current_term, "success": True}
