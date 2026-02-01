import os
import pandas as pd
from databricks.connect import DatabricksSession
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp
from src.config import Config

# from databricks.connect.cache import clear_cache


class DatabricksLoader:
    def __init__(self) -> None:
        self.config = Config()
        self.session = None

    def _get_databricks_host(self) -> str:
        """Build Databricks host URL (SDK expects https://host format)"""
        hostname = self.config.DATABRICKS_SERVER_HOSTNAME or ""
        if not hostname.startswith("http"):
            return f"https://{hostname}" if hostname else ""
        return hostname

    def open_connection(self) -> None:
        """Open Databricks connection using SQL Warehouse.
        
        For SQL Warehouse, Databricks Connect uses environment variables:
        - DATABRICKS_HOST (or DATABRICKS_SERVER_HOSTNAME)
        - DATABRICKS_TOKEN
        - DATABRICKS_HTTP_PATH (for SQL Warehouse)
        
        Note: SQL Warehouse does NOT use DATABRICKS_CLUSTER_ID
        """
        host = self._get_databricks_host()
        token = self.config.DATABRICKS_TOKEN
        http_path = self.config.DATABRICKS_HTTP_PATH
        
        if not host or not token:
            raise ValueError(
                "Databricks auth: configure DATABRICKS_SERVER_HOSTNAME and DATABRICKS_TOKEN in .env. "
                "See: https://docs.databricks.com/en/dev-tools/auth.html"
            )
        
        if not http_path:
            raise ValueError(
                "SQL Warehouse connection requires DATABRICKS_HTTP_PATH in .env.\n"
                "Find it in Databricks: SQL Warehouses > Your Warehouse > Connection Details > HTTP Path"
            )

        print(f"🔌 Connecting to Databricks SQL Warehouse...")
        print(f"   Host: {self.config.DATABRICKS_SERVER_HOSTNAME}")
        print(f"   HTTP Path: {http_path}")

        # Set environment variables - Databricks Connect reads these automatically
        os.environ["DATABRICKS_HOST"] = host
        os.environ["DATABRICKS_TOKEN"] = token
        os.environ["DATABRICKS_HTTP_PATH"] = http_path
        
        try:
            # Create Databricks session - it will read env vars automatically
            self.session = DatabricksSession.builder.getOrCreate()
            
            # Test connection with simple query
            test_result = self.session.sql("SELECT 1 as test").collect()
            
            print("✅ Connected to Databricks SQL Warehouse successfully!")
            print(f"   Test query result: {test_result}")
            
        except Exception as e:
            print(f"❌ Failed to connect to Databricks SQL Warehouse")
            print(f"   Error: {e}")
            print("\n💡 Troubleshooting:")
            print("   1. Verify SQL Warehouse is running in Databricks UI")
            print("   2. Check token has not expired")
            print("   3. Confirm HTTP_PATH is correct (format: /sql/1.0/warehouses/...)")
            print("   4. Ensure you have permissions to access the warehouse")
            raise

    def close_connection(self) -> None:
        """Close Databricks connection"""
        if self.session is not None:
            try:
                self.session.stop()
            except:
                pass
            self.session = None

    def load_records(
        self, pandas_df: pd.DataFrame, table: str, pk_columns: list = None
    ) -> None:
        """Load records into Databricks Delta table"""
        assert self.session is not None, "Session not initialized. Call open_connection() first."

        print(f"Loading records into Databricks table: {table}")

        table_path = f"{self.config.DATABRICKS_CATALOG}.{self.config.DATABRICKS_SCHEMA}.{table}"

        # Create schema if it doesn't exist (skip catalog creation for Community Edition)
        try:
            self.session.sql(f"CREATE SCHEMA IF NOT EXISTS {self.config.DATABRICKS_SCHEMA}")
        except Exception as e:
            print(f"Warning: Could not create schema: {e}")
            print("Using default schema")

        # Convert pandas DataFrame to Spark DataFrame
        df = self.session.createDataFrame(pandas_df)
        df = df.withColumn("ingestion_date", current_timestamp())

        # Write to Delta table
        df.write.format("delta").mode("overwrite").saveAsTable(table_path)
        
        print(f"Successfully loaded {pandas_df.shape[0]} records to {table_path}")