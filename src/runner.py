import os
from src.config import Config
from src.services.pokeapi import PokeAPIService
from src.loaders.databricks import DatabricksLoader
from src.parsers.parser import PokemonParser


class Runner:
    def __init__(self):
        self.config = Config()
        self.service = PokeAPIService()
        self.parser = PokemonParser()
        self.destination = DatabricksLoader()
        self.endpoint = "pokemon"
        self.offset = 0

    def run(self):
        print("Starting PokeAPI Ingestion")
        print(f"Configuration: Batch size={self.config.ETL_BATCH_SIZE}, Max records={self.config.ETL_MAX_RECORDS}")

        # Databricks connection temporarily disabled for testing
        self.destination.open_connection()

        records = []
        while True:
            response = self.service._request(
                method="GET",
                endpoint=self.endpoint,
                params={"offset": self.offset, "limit": self.config.ETL_BATCH_SIZE},
            )

            if response and response.status_code == 200:
                records += response.json().get("results", [])
                next_page = response.json().get("next", None)

            else:
                print("Failed to fetch data from PokeAPI")
                break

            self.offset += self.config.ETL_BATCH_SIZE
            if next_page is None or self.offset >= self.config.ETL_MAX_RECORDS:
                break

        parsed_records = self.parser.parse(records)

        # Databricks loading temporarily disabled for testing
        self.destination.load_records(pandas_df=parsed_records, table="pokemon")
        self.destination.close_connection()
        
        print("📊 Data successfully extracted and parsed!")
        print(f"Sample record: {records[0] if records else 'No records'}")

        print(f"Finished PokeAPI Ingestion - Total records: {len(records)}")
        return records