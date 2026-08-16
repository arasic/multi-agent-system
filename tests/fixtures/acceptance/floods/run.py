"""Hostile suite: floods stdout (400 MB) as fast as possible. Must be cut off at the cap, never reach the host disk."""

import sys

chunk = b"A" * (1024 * 1024)
for _ in range(400):
    sys.stdout.buffer.write(chunk)
sys.stdout.flush()
