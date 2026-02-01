"""
Databricks SQL Connector Loader
Compatible with Databricks Community Edition and SQL Warehouses
"""
import pandas as pd
from databricks import sql
from src.config import Config
from typing import Optional


class DatabricksSQLLoader:
    """Loader using Databricks SQL Connector (works with Community Edition)"""
    
    def __init__(self) -> None:
        self.config = Config()
        self.connection = None
        self.cursor = None

    def open_connection(self) -> None:
        """Open Databricks SQL connection using SQL Warehouse"""
        hostname = self.config.DATABRICKS_SERVER_HOSTNAME
        http_path = self.config.DATABRICKS_HTTP_PATH
        token = self.config.DATABRICKS_TOKEN
        
        if not all([hostname, http_path, token]):
            raise ValueError(
                "Missing Databricks SQL configuration. Required in .env:\n"
                "  - DATABRICKS_SERVER_HOSTNAME\n"
                "  - DATABRICKS_HTTP_PATH\n"
                "  - DATABRICKS_TOKEN"
            )
        
        try:
            print(f"🔌 Connecting to Databricks SQL Warehouse...")
            print(f"   Host: {hostname}")
            print(f"   HTTP Path: {http_path}")
            
            self.connection = sql.connect(
                server_hostname=hostname,
                http_path=http_path,
                access_token=token
            )
            
            self.cursor = self.connection.cursor()
            
            # Test connection
            self.cursor.execute("SELECT 1 as test")
            result = self.cursor.fetchone()
            
            print("✅ Connected to Databricks SQL Warehouse successfully!")
            
        except Exception as e:
            print(f"❌ Failed to connect to Databricks: {e}")
            raise

    def close_connection(self) -> None:
        """Close Databricks SQL connection"""
        if self.cursor:
            try:
                self.cursor.close()
            except:
                pass
            self.cursor = None
            
        if self.connection:
            try:
                self.connection.close()
            except:
                pass
            self.connection = None
        
        print("🔌 Databricks connection closed")

    def _create_table_if_not_exists(self, table: str, df: pd.DataFrame) -> None:
        """Create table schema based on DataFrame structure"""
        catalog = self.config.DATABRICKS_CATALOG
        schema = self.config.DATABRICKS_SCHEMA
        table_path = f"`{catalog}`.`{schema}`.`{table}`"
        
        # Map pandas dtypes to SQL types
        type_mapping = {
            'int64': 'BIGINT',
            'int32': 'INT',
            'float64': 'DOUBLE',
            'float32': 'FLOAT',
            'bool': 'BOOLEAN',
            'object': 'STRING',
            'datetime64[ns]': 'TIMESTAMP'
        }
        
        # Build column definitions from DataFrame (including ingestion_date)
        columns = []
        for col_name, dtype in df.dtypes.items():
            sql_type = type_mapping.get(str(dtype), 'STRING')
            columns.append(f"`{col_name}` {sql_type}")
        
        columns_sql = ",\n  ".join(columns)
        
        # Create catalog if not exists
        try:
            self.cursor.execute(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
            print(f"✅ Catalog `{catalog}` ready")
        except Exception as e:
            print(f"⚠️  Catalog creation warning: {e}")
        
        # Create schema if not exists
        try:
            self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
            print(f"✅ Schema `{catalog}`.`{schema}` ready")
        except Exception as e:
            print(f"⚠️  Schema creation warning: {e}")
        
        # Create table if not exists
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_path} (
          {columns_sql}
        )
        USING DELTA
        """
        
        try:
            self.cursor.execute(create_table_sql)
            print(f"✅ Table {table_path} ready")
        except Exception as e:
            print(f"⚠️  Table creation warning: {e}")

    def load_records(
        self, 
        pandas_df: pd.DataFrame, 
        table: str, 
        mode: str = "overwrite"
    ) -> None:
        """
        Load records into Databricks table - simplified approach
        
        Args:
            pandas_df: DataFrame with data to load
            table: Table name (without catalog/schema)
            mode: 'overwrite' or 'append'
        """
        if self.connection is None or self.cursor is None:
            raise RuntimeError("Connection not initialized. Call open_connection() first.")
        
        if pandas_df.empty:
            print("⚠️  No records to load (empty DataFrame)")
            return
        
        catalog = self.config.DATABRICKS_CATALOG
        schema = self.config.DATABRICKS_SCHEMA
        table_path = f"`{catalog}`.`{schema}`.`{table}`"
        
        print(f"📊 Loading {len(pandas_df)} records into {table_path}...")
        
        # Add ingestion timestamp
        pandas_df = pandas_df.copy()
        pandas_df['ingestion_date'] = pd.Timestamp.now()
        
        # Create table if needed
        self._create_table_if_not_exists(table, pandas_df)
        
        # Truncate if overwrite mode
        if mode == "overwrite":
            try:
                self.cursor.execute(f"TRUNCATE TABLE {table_path}")
                print(f"🗑️  Table truncated (overwrite mode)")
            except Exception as e:
                print(f"⚠️  Could not truncate: {e}")
        
        # Prepare data for bulk insert
        columns = list(pandas_df.columns)
        columns_str = ", ".join([f"`{col}`" for col in columns])
        placeholders = ", ".join(["?" for _ in columns])
        
        insert_sql = f"INSERT INTO {table_path} ({columns_str}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples
        records = [tuple(row) for row in pandas_df.values]
        
        # Insert in batches using executemany
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            try:
                self.cursor.executemany(insert_sql, batch)
                total_inserted += len(batch)
                print(f"  ✅ Inserted batch {i//batch_size + 1}: {len(batch)} records (total: {total_inserted})")
            except Exception as e:
                print(f"  ❌ Error inserting batch {i//batch_size + 1}: {e}")
                raise
        
        print(f"🎉 Successfully loaded {total_inserted} records to {table_path}")