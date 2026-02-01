#!/usr/bin/env python3
"""
Pokemon ETL Main Entry Point
"""

import sys
from dotenv import load_dotenv
from src.runner import Runner

def main():
    """Main function to run the Pokemon ETL process"""
    print("🚀 Starting Pokemon ETL", flush=True)
    sys.stdout.flush()
    
    # Load environment variables
    load_dotenv()
    print("📄 Environment variables loaded", flush=True)
    
    # Create and run the ETL
    runner = Runner()
    print("🏃 Running ETL process...", flush=True)
    result = runner.run()
    
    print(f"✅ ETL completed successfully! Processed {len(result) if result else 0} records.", flush=True)

if __name__ == "__main__":
    main()