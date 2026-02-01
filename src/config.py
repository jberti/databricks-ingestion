"""
Configuration module for environment variables and settings
"""
import os
from typing import Optional


class Config:
    """Centralized configuration class"""
    
    def __init__(self):
        """Initialize config - variables are read dynamically"""
        pass
    
    @property
    def DATABRICKS_SERVER_HOSTNAME(self) -> Optional[str]:
        return os.getenv("DATABRICKS_SERVER_HOSTNAME")
    
    @property
    def DATABRICKS_HTTP_PATH(self) -> Optional[str]:
        return os.getenv("DATABRICKS_HTTP_PATH")
    
    @property
    def DATABRICKS_TOKEN(self) -> Optional[str]:
        return os.getenv("DATABRICKS_TOKEN")
    
    @property
    def DATABRICKS_CATALOG(self) -> str:
        return os.getenv("DATABRICKS_CATALOG", "jornada")
    
    @property
    def DATABRICKS_SCHEMA(self) -> str:
        return os.getenv("DATABRICKS_SCHEMA", "databricks")
    
    @property
    def POKEAPI_BASE_URL(self) -> str:
        return os.getenv("POKEAPI_BASE_URL", "https://pokeapi.co/api/v2/")
    
    @property
    def POKEAPI_RATE_LIMIT(self) -> int:
        return int(os.getenv("POKEAPI_RATE_LIMIT", "1"))
    
    @property
    def POKEAPI_TIMEOUT(self) -> int:
        return int(os.getenv("POKEAPI_TIMEOUT", "60"))
    
    @property
    def POKEAPI_MAX_RETRIES(self) -> int:
        return int(os.getenv("POKEAPI_MAX_RETRIES", "3"))
    
    @property
    def ETL_BATCH_SIZE(self) -> int:
        return int(os.getenv("ETL_BATCH_SIZE", "15"))
    
    @property
    def ETL_MAX_RECORDS(self) -> int:
        return int(os.getenv("ETL_MAX_RECORDS", "300"))
    
    @property
    def LOG_LEVEL(self) -> str:
        return os.getenv("LOG_LEVEL", "INFO")
    
    @property
    def LOG_FORMAT(self) -> str:
        return os.getenv("LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    def validate_databricks_config(self) -> bool:
        """Validate if Databricks configuration is complete"""
        has_auth = all([self.DATABRICKS_SERVER_HOSTNAME, self.DATABRICKS_TOKEN])
        has_compute = os.getenv("DATABRICKS_CLUSTER_ID") or os.getenv("DATABRICKS_SERVERLESS_COMPUTE_ID") == "auto"
        return has_auth and has_compute
    
    def get_databricks_config(self) -> dict:
        """Get Databricks configuration as dictionary"""
        return {
            "hostname": self.DATABRICKS_SERVER_HOSTNAME,
            "http_path": self.DATABRICKS_HTTP_PATH,
            "token": self.DATABRICKS_TOKEN,
            "catalog": self.DATABRICKS_CATALOG,
            "schema": self.DATABRICKS_SCHEMA
        }
    
    def get_api_config(self) -> dict:
        """Get API configuration as dictionary"""
        return {
            "base_url": self.POKEAPI_BASE_URL,
            "rate_limit": self.POKEAPI_RATE_LIMIT,
            "timeout": self.POKEAPI_TIMEOUT,
            "max_retries": self.POKEAPI_MAX_RETRIES
        }
    
    def get_etl_config(self) -> dict:
        """Get ETL configuration as dictionary"""
        return {
            "batch_size": self.ETL_BATCH_SIZE,
            "max_records": self.ETL_MAX_RECORDS
        }