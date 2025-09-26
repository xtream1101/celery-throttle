#!/usr/bin/env python3
"""
Task dispatcher script - runs the dispatcher to process queued tasks.
This should be run in a separate terminal.
"""

import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.tasks import RateLimitedTaskDispatcher


def main():
    dispatcher = RateLimitedTaskDispatcher()

    interval = 0.1  # Default 100ms
    if len(sys.argv) > 1:
        try:
            interval = float(sys.argv[1])
        except ValueError:
            print("Invalid interval, using default 0.1 seconds")

    print(f"Starting task dispatcher with {interval}s interval...")
    print("Press Ctrl+C to stop")

    dispatcher.run_dispatcher(interval)


if __name__ == "__main__":
    main()