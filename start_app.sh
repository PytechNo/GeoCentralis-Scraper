#!/bin/bash
cd "$(dirname "$0")"
source venv/bin/activate

# Execute the main application
# Using --host 0.0.0.0 to make it accessible outside the container
exec python3 main.py --host 0.0.0.0 "$@"
