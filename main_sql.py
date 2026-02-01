#!/usr/bin/env python3
"""
Pokemon ETL Main Entry Point - SQL Connector Version
Compatible with Databricks Community Edition and SQL Warehouses
"""

import sys
from dotenv import load_dotenv
from src.runner_sql import RunnerSQL

def main():
    """Main function to run the Pokemon ETL process using SQL Connector"""
    print("🚀 Starting Pokemon ETL (SQL Connector Version)", flush=True)
    print("=" * 60)
    sys.stdout.flush()
    
    # Load environment variables
    load_dotenv()
    print("📄 Environment variables loaded", flush=True)
    
    # Create and run the ETL
    runner = RunnerSQL()
    print("🏃 Running ETL process...\n", flush=True)
    result = runner.run()
    
    if result:
        print(f"\n✅ ETL completed successfully! Processed {len(result)} records.", flush=True)
    else:
        print(f"\n❌ ETL failed or returned no results.", flush=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
