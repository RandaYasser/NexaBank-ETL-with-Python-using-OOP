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
        self.logger.info(f"Initializing SchemaValidator with schema file: {schema_file_path}")
        try:
            with open(schema_file_path, 'r') as f:
                self.schemas = json.load(f)
            self.logger.info(f"Successfully loaded schema file with {len(self.schemas)} table definitions")
        except Exception as e:
            self.logger.error(f"Failed to load schema file: {str(e)}")
            raise
            
    def validate_schema(self, df: pd.DataFrame, table_name: str) -> bool:
        """ Validate if DataFrame matches the schema for the specified table."""
        self.logger.info(f"Starting schema validation for table: {table_name}")
        self.logger.info(f"DataFrame shape: {df.shape}")
        
        try:
            if table_name not in self.schemas:
                self.logger.error(f"Table {table_name} not found in schema definitions")
                return False
                
            schema = self.schemas[table_name]
            properties = schema['properties']
            required = schema['required']
            
            self.logger.info(f"Schema validation details:")
            self.logger.info(f"- Required columns: {required}")
            self.logger.info(f"- Total columns in schema: {len(properties)}")
            self.logger.info(f"- Total columns in DataFrame: {len(df.columns)}")
            
            # Check if all required columns are present
            missing_cols = set(required) - set(df.columns)
            if missing_cols:
                self.logger.error(f"Missing required columns: {missing_cols}")
                return False
                
            # Check column types
            self.logger.info("Starting column type validation")
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
                else:
                    self.logger.info(f"Column {col} type validation passed: {actual_type}")
            
            self.logger.info(f"Schema validation completed successfully for table: {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Schema validation failed: {str(e)}")
            return False
            
