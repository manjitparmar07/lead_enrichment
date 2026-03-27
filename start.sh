#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Create venv if not present
if [ ! -d "venv" ]; then
  echo "Creating virtual environment..."
  python3 -m venv venv
fi

source venv/bin/activate

echo "Installing dependencies..."
pip install -q -r requirements.txt

# Copy .env.example to .env if no .env exists yet
if [ ! -f ".env" ]; then
  cp .env.example .env
  echo "Created .env from .env.example — fill in your API keys"
fi

echo "Starting Lead Enrichment API on port ${PORT:-8020}..."
uvicorn main:app --host 0.0.0.0 --port "${PORT:-8020}" --reload
