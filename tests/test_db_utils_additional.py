"""Additional tests for the database utilities module."""
import pytest
from pyspark.sql.types import StringType
from src.utils.db_utils import DeltaTableManager

class TestDeltaTableManagerAdditional:
    """Additional test cases for the DeltaTableManager class."""
    
    def test_create_table_without_partition_or_properties(self, spark, test_schema):
        """Test table creation without partition columns or properties."""
        manager = DeltaTableManager(spark, "test_catalog", "test_schema")
        table_name = "test_minimal_table"
        
        # Create table with minimal parameters
        manager.create_table(
            table_name=table_name,
            schema=test_schema,
            comment="Minimal test table"
        )
        
        # Verify table exists and has the correct schema
        assert manager.table_exists(table_name)
        table_schema = spark.table(f"test_catalog.test_schema.{table_name}").schema
        assert len(table_schema.fields) == len(test_schema.fields) + 2  # +2 for metadata columns
        
        # Clean up
        spark.sql(f"DROP TABLE IF EXISTS test_catalog.test_schema.{table_name}")

    def test_write_dataframe_with_partitioning(self, spark, test_schema):
        """Test writing a DataFrame with partitioning."""
        from pyspark.sql import Row
        
        manager = DeltaTableManager(spark, "test_catalog", "test_schema")
        table_name = "test_partitioned_write"
        
        # Create test data with a partition column
        test_data = [
            Row(id="1", name="Item 1", price=10.5, quantity=5, timestamp="2023-01-01"),
            Row(id="2", name="Item 2", price=20.0, quantity=3, timestamp="2023-01-01"),
            Row(id="3", name="Item 3", price=15.75, quantity=10, timestamp="2023-01-02")
        ]
        
        # Add timestamp field to schema
        schema_with_ts = test_schema.add("timestamp", StringType())
        df = spark.createDataFrame(test_data, schema=schema_with_ts)
        
        # Write with partitioning
        manager.write_dataframe(
            df=df,
            table_name=table_name,
            mode="overwrite",
            partition_by=["timestamp"]
        )
        
        # Verify data was written with partitioning
        result = spark.table(f"test_catalog.test_schema.{table_name}")
        assert result.count() == 3
        
        # Check partition pruning works
        partitioned_count = spark.sql(
            f"SELECT COUNT(*) as cnt FROM test_catalog.test_schema.{table_name} WHERE timestamp = '2023-01-01'"
        ).collect()[0]["cnt"]
        assert partitioned_count == 2
        
        # Clean up
        spark.sql(f"DROP TABLE IF EXISTS test_catalog.test_schema.{table_name}")

    def test_write_dataframe_error_handling(self, spark, test_schema):
        """Test error handling in write_dataframe."""
        manager = DeltaTableManager(spark, "test_catalog", "test_schema")
        
        # Test with invalid mode
        with pytest.raises(ValueError):
            manager.write_dataframe(
                df=spark.createDataFrame([], test_schema),
                table_name="invalid_mode_test",
                mode="invalid_mode"
            )
        
        # Test with non-existent partition column
        with pytest.raises(Exception):
            manager.write_dataframe(
                df=spark.createDataFrame([], test_schema),
                table_name="invalid_partition_test",
                partition_by=["non_existent_column"]
            )

    def test_optimize_table_empty(self, spark, test_schema):
        """Test optimizing an empty table."""
        manager = DeltaTableManager(spark, "test_catalog", "test_schema")
        table_name = "test_optimize_empty"
        
        # Create an empty table
        manager.create_table(table_name, test_schema)
        
        # Should not raise an exception
        manager.optimize_table(table_name)
        
        # Clean up
        spark.sql(f"DROP TABLE IF EXISTS test_catalog.test_schema.{table_name}")

    def test_get_table_stats_empty_table(self, spark, test_schema):
        """Test getting stats for an empty table."""
        manager = DeltaTableManager(spark, "test_catalog", "test_schema")
        table_name = "test_empty_stats"
        
        # Create an empty table
        manager.create_table(table_name, test_schema)
        
        # Get stats
        stats = manager.get_table_stats(table_name)
        
        # Verify stats for empty table
        assert stats["table_name"] == f"test_catalog.test_schema.{table_name}"
        assert stats["row_count"] == 0
        
        # Clean up
        spark.sql(f"DROP TABLE IF EXISTS test_catalog.test_schema.{table_name}")

    def test_table_exists_nonexistent_table(self, spark):
        """Test table_exists with a non-existent table."""
        manager = DeltaTableManager(spark, "test_catalog", "test_schema")
        assert not manager.table_exists("non_existent_table_12345")
