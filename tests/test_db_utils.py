"""Tests for the database utilities module."""
import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from src.utils.db_utils import DeltaTableManager

class TestDeltaTableManager:
    """Test cases for the DeltaTableManager class."""
    
    @pytest.fixture
    def test_schema(self):
        """Define a test schema for table creation."""
        return StructType([
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("price", DoubleType(), nullable=True),
            StructField("quantity", IntegerType(), nullable=True),
            StructField("timestamp", TimestampType(), nullable=True)
        ])
    
    def test_initialization(self, spark):
        """Test that the DeltaTableManager initializes correctly."""
        # Test with default catalog and schema
        manager = DeltaTableManager(spark, "test_catalog", "test_schema")
        assert manager.catalog_name == "test_catalog"
        assert manager.schema_name == "test_schema"
        assert manager.full_schema_path == "test_catalog.test_schema"
        
        # Verify catalog and schema were created
        catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
        assert "test_catalog" in catalogs
        
        schemas = [row.databaseName for row in spark.sql("SHOW DATABASES IN test_catalog").collect()]
        assert "test_schema" in schemas
    
    def test_table_operations(self, spark, test_schema):
        """Test table creation and existence checks."""
        manager = DeltaTableManager(spark, "test_catalog", "test_schema")
        table_name = "test_table"
        full_table_name = f"test_catalog.test_schema.{table_name}"
        
        # Table shouldn't exist yet
        assert not manager.table_exists(table_name)
        
        # Create the table
        manager.create_table(
            table_name=table_name,
            schema=test_schema,
            partition_columns=["timestamp"],
            comment="Test table for unit testing",
            properties={"delta.autoOptimize.optimizeWrite": "true"}
        )
        
        # Table should exist now
        assert manager.table_exists(table_name)
        
        # Verify table properties
        table_info = spark.sql(f"DESCRIBE TABLE EXTENDED {full_table_name}").collect()
        table_props = {row.col_name: row.data_type for row in table_info if row.col_name == "Type"}
        assert "delta" in table_props.get("Type", "").lower()
        
        # Verify schema
        table_schema = spark.table(full_table_name).schema
        assert len(table_schema.fields) == len(test_schema.fields) + 2  # +2 for ingestion_timestamp and pipeline_run_id
        
        # Clean up
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
    
    def test_write_dataframe(self, spark, test_schema):
        """Test writing a DataFrame to a Delta table."""
        from pyspark.sql import Row
        
        manager = DeltaTableManager(spark, "test_catalog", "test_schema")
        table_name = "test_write"
        
        # Create test data
        test_data = [
            Row(id="1", name="Item 1", price=10.5, quantity=5, timestamp=None),
            Row(id="2", name="Item 2", price=20.0, quantity=3, timestamp=None),
            Row(id="3", name="Item 3", price=15.75, quantity=10, timestamp=None)
        ]
        
        df = spark.createDataFrame(test_data, schema=test_schema)
        
        # Write the DataFrame
        manager.write_dataframe(
            df=df,
            table_name=table_name,
            mode="overwrite",
            merge_schema=True
        )
        
        # Verify data was written correctly
        result = spark.table(f"test_catalog.test_schema.{table_name}")
        assert result.count() == 3
        
        # Check that metadata columns were added
        assert "ingestion_timestamp" in result.columns
        assert "pipeline_run_id" in result.columns
        
        # Clean up
        spark.sql(f"DROP TABLE IF EXISTS test_catalog.test_schema.{table_name}")
    
    def test_optimize_table(self, spark, test_schema):
        """Test table optimization."""
        manager = DeltaTableManager(spark, "test_catalog", "test_schema")
        table_name = "test_optimize"
        
        # Create and populate a test table
        manager.create_table(table_name, test_schema)
        
        # Write some test data
        from pyspark.sql import Row
        test_data = [Row(id=str(i), name=f"Item {i}", price=i*1.1, quantity=i, timestamp=None) for i in range(100)]
        df = spark.createDataFrame(test_data, schema=test_schema)
        manager.write_dataframe(df, table_name, "append")
        
        # Optimize the table
        manager.optimize_table(table_name, zorder_columns=["id", "timestamp"])
        
        # Verify optimization by checking table history
        history = spark.sql(f"DESCRIBE HISTORY test_catalog.test_schema.{table_name}")
        assert history.filter("operation = 'OPTIMIZE'").count() >= 1
        
        # Clean up
        spark.sql(f"DROP TABLE IF EXISTS test_catalog.test_schema.{table_name}")
    
    def test_get_table_stats(self, spark, test_schema):
        """Test getting table statistics."""
        manager = DeltaTableManager(spark, "test_catalog", "test_schema")
        table_name = "test_stats"
        
        # Create and populate a test table
        manager.create_table(table_name, test_schema)
        
        # Write some test data
        from pyspark.sql import Row
        test_data = [Row(id=str(i), name=f"Item {i}", price=i*1.1, quantity=i, timestamp=None) for i in range(10)]
        df = spark.createDataFrame(test_data, schema=test_schema)
        manager.write_dataframe(df, table_name, "append")
        
        # Get table stats
        stats = manager.get_table_stats(table_name)
        
        # Verify stats
        assert stats["table_name"] == f"test_catalog.test_schema.{table_name}"
        assert stats["row_count"] == 10
        assert stats["column_count"] == len(test_schema.fields) + 2  # +2 for metadata columns
        assert "timestamp" in stats["columns"]
        
        # Clean up
        spark.sql(f"DROP TABLE IF EXISTS test_catalog.test_schema.{table_name}")
    
    def test_write_with_different_modes(self, spark, test_schema):
        """Test writing with different save modes."""
        manager = DeltaTableManager(spark, "test_catalog", "test_schema")
        table_name = "test_modes"
        
        # Create initial data
        data1 = [
            ("1", "Item 1", 10.5, 5, None),
            ("2", "Item 2", 20.0, 3, None)
        ]
        df1 = spark.createDataFrame(data1, schema=test_schema)
        
        # Write with overwrite mode
        manager.write_dataframe(df1, table_name, "overwrite")
        assert spark.table(f"test_catalog.test_schema.{table_name}").count() == 2
        
        # Write with append mode
        data2 = [("3", "Item 3", 15.0, 7, None)]
        df2 = spark.createDataFrame(data2, schema=test_schema)
        manager.write_dataframe(df2, table_name, "append")
        assert spark.table(f"test_catalog.test_schema.{table_name}").count() == 3
        
        # Write with errorIfExists mode (should fail)
        df3 = spark.createDataFrame([("4", "Item 4", 12.0, 2, None)], schema=test_schema)
        with pytest.raises(Exception):
            manager.write_dataframe(df3, table_name, "error")
        
        # Write with ignore mode (should not add new data)
        manager.write_dataframe(df3, table_name, "ignore")
        assert spark.table(f"test_catalog.test_schema.{table_name}").count() == 3
        
        # Clean up
        spark.sql(f"DROP TABLE IF EXISTS test_catalog.test_schema.{table_name}")
