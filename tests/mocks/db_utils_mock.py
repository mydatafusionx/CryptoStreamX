"""Mocks for db_utils module."""
from unittest.mock import MagicMock

class MockDeltaTableManager:
    """Mock for DeltaTableManager class."""
    
    def __init__(self, spark, catalog_name, schema_name):
        """Initialize the mock DeltaTableManager."""
        self.spark = spark
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.tables = {}
        
        # Mock methods
        self.table_exists = MagicMock(side_effect=self._table_exists)
        self.create_table = MagicMock(side_effect=self._create_table)
        self.write_data = MagicMock(side_effect=self._write_data)
        self.read_table = MagicMock(side_effect=self._read_table)
        self.optimize_table = MagicMock(return_value={"status": "success"})
        self.get_table_stats = MagicMock(return_value={
            "num_files": 1,
            "size_in_bytes": 1024,
            "records_count": 10
        })
    
    def _table_exists(self, table_name):
        """Check if a table exists in the mock database."""
        full_name = f"{self.catalog_name}.{self.schema_name}.{table_name}"
        return full_name in self.tables
    
    def _create_table(self, table_name, schema, partition_columns=None):
        """Create a table in the mock database."""
        full_name = f"{self.catalog_name}.{self.schema_name}.{table_name}"
        self.tables[full_name] = {
            "schema": schema,
            "data": [],
            "partition_columns": partition_columns or []
        }
        return {"status": "success"}
    
    def _write_data(self, df, table_name, mode="overwrite"):
        """Write data to a mock table."""
        full_name = f"{self.catalog_name}.{self.schema_name}.{table_name}"
        
        # If table doesn't exist, create it
        if full_name not in self.tables:
            self._create_table(table_name, df.schema)
        
        # Convert DataFrame to list of rows
        data = [row.asDict() for row in df.collect()]
        
        # Handle write mode
        if mode.lower() == "overwrite":
            self.tables[full_name]["data"] = data
        elif mode.lower() == "append":
            self.tables[full_name]["data"].extend(data)
        else:
            raise ValueError(f"Unsupported write mode: {mode}")
        
        return {"status": "success", "records_processed": len(data)}
    
    def _read_table(self, table_name):
        """Read data from a mock table."""
        full_name = f"{self.catalog_name}.{self.schema_name}.{table_name}"
        if full_name not in self.tables:
            raise ValueError(f"Table {full_name} does not exist")
        
        # Return a mock DataFrame with the stored data
        mock_df = MagicMock()
        mock_df.collect.return_value = [MagicMock(**row) for row in self.tables[full_name]["data"]]
        mock_df.count.return_value = len(self.tables[full_name]["data"])
        return mock_df
