#!/usr/bin/env python3
"""
Celery worker startup script.
"""

import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.tasks import app


def main():
    # Start Celery worker with specific configuration
    app.worker_main([
        'worker',
        '--loglevel=info',
        '--concurrency=1',
        '--prefetch-multiplier=1',
        '--without-mingle',
        '--without-gossip'
    ])


if __name__ == "__main__":
    main()