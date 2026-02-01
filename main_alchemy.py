#!/usr/bin/env python3
"""
Main Entry Point - SQLAlchemy Version
"""
from dotenv import load_dotenv
from src.runner_alchemy import RunnerAlchemy
import sys

def main():
    load_dotenv()
    runner = RunnerAlchemy()
    runner.run()
    
if __name__ == "__main__":
    main()
