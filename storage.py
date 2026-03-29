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