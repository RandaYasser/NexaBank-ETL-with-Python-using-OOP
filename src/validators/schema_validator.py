import pandas as pd
import json
from typing import Dict, Any
from src.utils.logger import Logger

class SchemaValidator:
    """Validates data schemas across the pipeline."""
    
    def __init__(self, schema_file_path: str):
        """
        Initialize SchemaValidator with schema file.
        """
        self.logger = Logger(__name__)
        try:
            with open(schema_file_path, 'r') as f:
                self.schemas = json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load schema file: {str(e)}")
            raise
            
    def validate_schema(self, df: pd.DataFrame, table_name: str) -> bool:
        """ Validate if DataFrame matches the schema for the specified table."""
        try:
            if table_name not in self.schemas:
                self.logger.error(f"Table {table_name} not found in schema")
                return False
                
            schema = self.schemas[table_name]
            properties = schema['properties']
            required = schema['required']
            
            # Check if all required columns are present
            missing_cols = set(required) - set(df.columns)
            if missing_cols:
                self.logger.error(f"Missing required columns: {missing_cols}")
                return False
                
            # Check column types
            for col, col_schema in properties.items():
                if col not in df.columns:
                    self.logger.error(f"Column {col} not found in DataFrame")
                    return False
                    
                expected_type = col_schema['type']
                actual_type = str(df[col].dtype)
                
                if actual_type != expected_type:
                    self.logger.error(
                        f"Type mismatch for column {col}: "
                        f"expected {expected_type}, got {actual_type}"
                    )
                    return False
                    
            return True
            
        except Exception as e:
            self.logger.error(f"Schema validation failed: {str(e)}")
            return False
            
