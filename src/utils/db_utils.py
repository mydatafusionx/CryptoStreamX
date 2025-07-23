"""Utility functions for database operations in CryptoStreamX."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType, IntegerType
from typing import Dict, Any, List, Optional, Union
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DeltaTableManager:
    """Helper class for managing Delta tables."""
    
    def __init__(self, spark: SparkSession, catalog_name: str, schema_name: str):
        """Initialize the DeltaTableManager.
        
        Args:
            spark: Active SparkSession
            catalog_name: Name of the catalog (e.g., 'datafusionx_catalog')
            schema_name: Name of the schema (e.g., 'bronze', 'silver', 'gold')
        """
        self.spark = spark
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.full_schema_path = f"{catalog_name}.{schema_name}"
        
        # Ensure catalog and schema exist
        self._ensure_catalog_and_schema()
    
    def _ensure_catalog_and_schema(self):
        """Ensure that the catalog and schema exist."""
        # Create catalog if it doesn't exist
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog_name}")
        
        # Create schema if it doesn't exist
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.full_schema_path}")
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the current schema.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            bool: True if the table exists, False otherwise
        """
        try:
            self.spark.sql(f"DESCRIBE TABLE {self.full_schema_path}.{table_name}")
            return True
        except Exception as e:
            if "Table or view not found" in str(e):
                return False
            raise
    
    def create_table(
        self, 
        table_name: str, 
        schema: StructType,
        partition_columns: Optional[List[str]] = None,
        comment: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None
    ) -> None:
        """Create a new Delta table.
        
        Args:
            table_name: Name of the table to create
            schema: PySpark StructType defining the table schema
            partition_columns: List of column names to partition by
            comment: Table comment/description
            properties: Additional table properties
        """
        if self.table_exists(table_name):
            logger.info(f"Table {self.full_schema_path}.{table_name} already exists")
            return
            
        # Build the CREATE TABLE statement
        create_stmt = f"CREATE TABLE {self.full_schema_path}.{table_name} ("
        
        # Add columns
        for field in schema.fields:
            create_stmt += f"\n  {field.name} {field.dataType.simpleString()},"
        
        # Remove trailing comma and close columns
        create_stmt = create_stmt.rstrip(',') + "\n)"
        
        # Add partitioning if specified
        if partition_columns:
            create_stmt += f"\nPARTITIONED BY ({', '.join(partition_columns)})"
        
        # Add comment if specified
        if comment:
            create_stmt += f"\nCOMMENT '{comment}'"
        
        # Add table properties if specified
        if properties:
            props_str = ', '.join([f"'{k}' = '{v}'" for k, v in properties.items()])
            create_stmt += f"\nTBLPROPERTIES ({props_str})"
        
        # Execute the CREATE TABLE statement
        logger.info(f"Creating table {self.full_schema_path}.{table_name}")
        self.spark.sql(create_stmt)
    
    def write_dataframe(
        self,
        df,
        table_name: str,
        mode: str = "append",
        merge_schema: bool = False,
        overwrite_schema: bool = False,
        partition_by: Optional[List[str]] = None
    ) -> None:
        """Write a DataFrame to a Delta table.
        
        Args:
            df: Input DataFrame
            table_name: Target table name
            mode: Write mode ('append', 'overwrite', 'ignore', 'error', 'errorifexists')
            merge_schema: If True, merges the input schema with existing table schema
            overwrite_schema: If True, allows overwriting the table schema
            partition_by: List of column names to partition the table by
        """
        full_table_name = f"{self.full_schema_path}.{table_name}"
        
        # Add metadata columns if they don't exist
        if "ingestion_timestamp" not in df.columns:
            df = df.withColumn("ingestion_timestamp", current_timestamp())
        
        if "pipeline_run_id" not in df.columns and "pipeline_run_id" in df.columns:
            # You can set a proper run ID here, e.g., from environment variable
            df = df.withColumn("pipeline_run_id", lit("manual_run"))
        
        # Write the DataFrame
        writer = df.write.format("delta")
        
        # Apply partitioning if specified
        if partition_by:
            writer = writer.partitionBy(partition_by)
        
        if merge_schema:
            writer = writer.option("mergeSchema", "true")
        if overwrite_schema:
            writer = writer.option("overwriteSchema", "true")
        
        writer.mode(mode).saveAsTable(full_table_name)
        
        logger.info(f"Successfully wrote {df.count()} rows to {full_table_name} in {mode} mode")
        
        # Optimize the table after write if it's an overwrite operation
        if mode.lower() == "overwrite" and partition_by:
            logger.info(f"Optimizing table {full_table_name} after overwrite")
            self.optimize_table(table_name, zorder_columns=partition_by)
    
    def optimize_table(self, table_name: str, zorder_columns: Optional[List[str]] = None) -> None:
        """Optimize a Delta table.
        
        Args:
            table_name: Name of the table to optimize
            zorder_columns: List of columns to Z-ORDER BY
        """
        full_table_name = f"{self.full_schema_path}.{table_name}"
        
        # Run OPTIMIZE
        optimize_sql = f"OPTIMIZE {full_table_name}"
        
        # Add Z-ORDER BY if specified
        if zorder_columns:
            optimize_sql += f" ZORDER BY ({', '.join(zorder_columns)})"
        
        logger.info(f"Optimizing table {full_table_name}")
        self.spark.sql(optimize_sql)
        
        # Run VACUUM to clean up old files
        self.spark.sql(f"VACUUM {full_table_name} RETAIN 24 HOURS")
        
        # Collect table statistics
        self.spark.sql(f"ANALYZE TABLE {full_table_name} COMPUTE STATISTICS")
        
        logger.info(f"Table {full_table_name} optimization complete")
    
    def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """Get statistics for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary containing table statistics
        """
        full_table_name = f"{self.full_schema_path}.{table_name}"
        
        # Get table details
        table_info = self.spark.sql(f"DESCRIBE EXTENDED {full_table_name}")
        
        # Get row count
        row_count = self.spark.sql(f"SELECT COUNT(*) as count FROM {full_table_name}").collect()[0]["count"]
        
        # Get column stats
        column_stats = {}
        for col_info in self.spark.sql(f"DESCRIBE TABLE EXTENDED {full_table_name}").collect():
            if "col_name" in col_info and "data_type" in col_info:
                column_stats[col_info["col_name"]] = col_info["data_type"]
        
        # Get partition information
        try:
            partitions = self.spark.sql(f"SHOW PARTITIONS {full_table_name}")
            partition_count = partitions.count()
        except:
            partition_count = 0
        
        return {
            "table_name": full_table_name,
            "row_count": row_count,
            "column_count": len(column_stats),
            "partition_count": partition_count,
            "last_updated": datetime.now().isoformat(),
            "columns": column_stats
        }
