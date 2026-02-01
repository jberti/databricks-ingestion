"""
Databricks SQLAlchemy Loader
Clean and modern approach using df.to_sql() with Staging Pattern
"""
import pandas as pd
from sqlalchemy import create_engine, text
from src.config import Config
import os
import uuid

class DatabricksAlchemyLoader:
    """Loader using SQLAlchemy - clean and pythonic"""
    
    def __init__(self) -> None:
        self.config = Config()
        self.engine = None

    def open_connection(self) -> None:
        """Create SQLAlchemy Engine"""
        hostname = self.config.DATABRICKS_SERVER_HOSTNAME
        http_path = self.config.DATABRICKS_HTTP_PATH
        token = self.config.DATABRICKS_TOKEN
        catalog = self.config.DATABRICKS_CATALOG
        schema = self.config.DATABRICKS_SCHEMA
        
        if not all([hostname, http_path, token]):
            raise ValueError("Missing Databricks configuration.")
            
        print(f"🔌 Connecting to Databricks (SQLAlchemy)...")
        
        # Build connection string
        connection_string = (
            f"databricks://token:{token}@{hostname}:443/{catalog}/{schema}"
            f"?http_path={http_path}"
        )
        
        try:
            self.engine = create_engine(connection_string)
            
            # Test connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).scalar()
                print(f"✅ Connected! Test result: {result}")
                
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            raise

    def close_connection(self) -> None:
        """Dispose engine"""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            print("🔌 Databricks connection closed")

    def load_records(
        self, 
        pandas_df: pd.DataFrame, 
        table: str, 
        mode: str = "replace"
    ) -> None:
        """Load records using Staging Table pattern (Robust for Unity Catalog)"""
        if self.engine is None:
            self.open_connection()
            
        if pandas_df.empty:
            print("⚠️  No records to load")
            return

        catalog = self.config.DATABRICKS_CATALOG
        schema = self.config.DATABRICKS_SCHEMA

        # Add timestamp if missing
        if 'ingestion_date' not in pandas_df.columns:
            pandas_df = pandas_df.copy()
            pandas_df['ingestion_date'] = pd.Timestamp.now()
            
        print(f"📊 Loading {len(pandas_df)} records...")
        
        try:
            with self.engine.connect() as conn:
                # 1. Setup Context
                conn.execute(text(f"CREATE CATALOG IF NOT EXISTS `{catalog}`"))
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`"))
                conn.execute(text(f"USE CATALOG `{catalog}`"))
                conn.execute(text(f"USE SCHEMA `{schema}`"))
                print(f"✅ Context set to: {catalog}.{schema}")
                
                # 2. Upload to Staging Table
                # We use a unique name to allow parallel runs and avoid conflicts
                stage_table = f"{table}_stg_{uuid.uuid4().hex[:8]}"
                print(f"📦 Creating staging table `{stage_table}`...")
                
                pandas_df.to_sql(
                    name=stage_table,
                    con=conn,
                    if_exists='replace',
                    index=False,
                    method='multi',
                    chunksize=50 # Safer limit for Databricks (max 256 params per query)
                )
                
                # 3. Create Target Table if not exists
                print(f"🏗️  Ensuring target table `{table}` exists...")
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS `{table}` 
                USING DELTA 
                AS SELECT * FROM `{stage_table}` WHERE 1=0
                """
                conn.execute(text(create_sql))
                
                # 4. Insert data from Staging
                print(f"🚚 Moving data from staging to `{table}`...")
                insert_sql = f"INSERT INTO `{table}` SELECT * FROM `{stage_table}`"
                conn.execute(text(insert_sql))
                
                # 5. Cleanup
                print(f"🧹 Dropping staging table...")
                conn.execute(text(f"DROP TABLE `{stage_table}`"))
                
                conn.commit()
            
            print(f"🎉 Successfully loaded data to `{table}`")
            
        except Exception as e:
            print(f"❌ Load failed: {e}")
            raise
