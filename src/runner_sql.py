"""
ETL Runner using Databricks SQL Connector
Compatible with Databricks Community Edition and SQL Warehouses
"""
import os
from src.config import Config
from src.services.pokeapi import PokeAPIService
from src.loaders.databricks_sql import DatabricksSQLLoader
from src.parsers.parser import PokemonParser


class RunnerSQL:
    """ETL Runner using SQL Connector instead of Databricks Connect"""
    
    def __init__(self):
        self.config = Config()
        self.service = PokeAPIService()
        self.parser = PokemonParser()
        self.destination = DatabricksSQLLoader()
        self.endpoint = "pokemon"
        self.offset = 0

    def run(self):
        print("🚀 Starting PokeAPI Ingestion (SQL Connector)")
        print(f"⚙️  Configuration: Batch size={self.config.ETL_BATCH_SIZE}, Max records={self.config.ETL_MAX_RECORDS}")

        # Open Databricks SQL connection
        try:
            self.destination.open_connection()
        except Exception as e:
            print(f"❌ Failed to connect to Databricks: {e}")
            print("\n💡 Troubleshooting tips:")
            print("   1. Verify your .env file has correct credentials")
            print("   2. Check if SQL Warehouse is running in Databricks")
            print("   3. Verify token has not expired")
            return None

        records = []
        
        print(f"\n📡 Fetching data from PokeAPI...")
        
        while True:
            try:
                response = self.service._request(
                    method="GET",
                    endpoint=self.endpoint,
                    params={"offset": self.offset, "limit": self.config.ETL_BATCH_SIZE},
                )

                if response and response.status_code == 200:
                    batch_records = response.json().get("results", [])
                    records += batch_records
                    next_page = response.json().get("next", None)
                    
                    print(f"  ✅ Fetched {len(batch_records)} records (total: {len(records)})")
                else:
                    print(f"  ❌ Failed to fetch data from PokeAPI (status: {response.status_code if response else 'None'})")
                    break

                self.offset += self.config.ETL_BATCH_SIZE
                
                if next_page is None or self.offset >= self.config.ETL_MAX_RECORDS:
                    break
                    
            except Exception as e:
                print(f"  ❌ Error fetching data: {e}")
                break

        if not records:
            print("⚠️  No records fetched from API")
            self.destination.close_connection()
            return None

        print(f"\n🔄 Parsing {len(records)} records...")
        parsed_records = self.parser.parse(records)
        print(f"  ✅ Parsed into DataFrame with shape: {parsed_records.shape}")

        print(f"\n💾 Loading data to Databricks...")
        try:
            self.destination.load_records(
                pandas_df=parsed_records, 
                table="pokemon",
                mode="overwrite"
            )
        except Exception as e:
            print(f"❌ Failed to load data: {e}")
            self.destination.close_connection()
            return None
        
        self.destination.close_connection()
        
        print(f"\n🎉 ETL completed successfully!")
        print(f"   Total records processed: {len(records)}")
        print(f"   Sample record: {records[0] if records else 'No records'}")

        return records
