#!/usr/bin/env python3
import subprocess
import sys
import os

def test_basic():
    # Use absolute or relative path to the executable
    exe_path = os.path.join(os.path.dirname(__file__), "..", "build", "freq")
    if not os.path.exists(exe_path):
        print(f"Executable not found at {exe_path}. Please build the project first.")
        sys.exit(1)

    with open("in.txt", "w") as f:
        f.write("The time has come, the Walrus said, to talk of many things...\n")
    
    subprocess.run([exe_path, "in.txt", "out.txt"], check=True)
    
    with open("out.txt", "r") as f:
        lines = f.read().strip().split('\n')
        
    expected = [
        "2 the",
        "1 come",
        "1 has",
        "1 many",
        "1 of",
        "1 said",
        "1 talk",
        "1 things",
        "1 time",
        "1 to",
        "1 walrus"
    ]
    
    if lines != expected:
        print("Test failed!")
        print("Expected:")
        print(expected)
        print("Got:")
        print(lines)
        sys.exit(1)
    
    print("Basic test passed!")

if __name__ == "__main__":
    test_basic()
