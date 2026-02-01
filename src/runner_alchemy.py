"""
ETL Runner using Databricks SQLAlchemy
Clean, Modern, Simple
"""
from src.config import Config
from src.services.pokeapi import PokeAPIService
from src.loaders.databricks_alchemy import DatabricksAlchemyLoader
from src.parsers.parser import PokemonParser

class RunnerAlchemy:
    def __init__(self):
        self.config = Config()
        self.service = PokeAPIService()
        self.parser = PokemonParser()
        self.destination = DatabricksAlchemyLoader()
        self.endpoint = "pokemon"
        self.offset = 0

    def run(self):
        print("🚀 Starting PokeAPI Ingestion (SQLAlchemy Version)")
        
        # 1. Extraction (same as before)
        records = []
        while True:
            response = self.service._request(
                method="GET", 
                endpoint=self.endpoint,
                params={"offset": self.offset, "limit": self.config.ETL_BATCH_SIZE}
            )
            if not response or response.status_code != 200: break
            
            batch = response.json().get("results", [])
            records += batch
            print(f"  ✅ Fetched {len(batch)} records")
            
            if not response.json().get("next") or len(records) >= self.config.ETL_MAX_RECORDS:
                break
            self.offset += self.config.ETL_BATCH_SIZE

        # 2. Transformation
        df = self.parser.parse(records)
        print(f"  ✅ Parsed {len(df)} records")

        # 3. Loading (The clean part!)
        try:
            self.destination.load_records(
                pandas_df=df,
                table="pokemon",
                mode="replace" # Replace table content
            )
        finally:
            self.destination.close_connection()
            
        return records
