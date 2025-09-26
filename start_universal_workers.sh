#!/bin/bash

# Simple script to start workers for the universal queue system
# Workers only need to listen to the 'universal_queue' - no dynamic queue management needed!

echo "🚀 Starting Celery workers for Universal Queue System"
echo "Workers will process ALL dynamic queues through 'universal_queue'"
echo ""

# Start workers listening only to the universal_queue
celery -A main worker \
  --queues=universal_queue \
  --loglevel=info \
  --prefetch-multiplier=1 \
  --without-gossip \
  --without-mingle \
  --concurrency=8

echo "✅ Workers started for universal queue system"
