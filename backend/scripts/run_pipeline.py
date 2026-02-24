#!/usr/bin/env python3
"""Script to manually run the data pipeline."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.pipeline.orchestrator import run_pipeline

if __name__ == "__main__":
    run_pipeline()
