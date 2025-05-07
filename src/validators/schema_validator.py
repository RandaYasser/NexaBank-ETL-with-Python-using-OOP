import pandas as pd
from typing import Dict, Any
from .logger import Logger

class SchemaValidator:
    """Validates data schemas across the pipeline."""
    
    def __init__(self):
        self.logger = Logger(__name__)
        
    def validate_schema(self, df: pd.DataFrame, expected_schema: Dict[str, Any]) -> bool:
        """
        Validate if DataFrame matches expected schema.
        
        Args:
            df: DataFrame to validate
            expected_schema: Dictionary mapping column names to their expected types
            
        Returns:
            bool: True if schema is valid, False otherwise
        """
        try:
            # Check if all expected columns are present
            missing_cols = set(expected_schema.keys()) - set(df.columns)
            if missing_cols:
                self.logger.error(f"Missing columns: {missing_cols}")
                return False
                
            # Check column types
            for col, expected_type in expected_schema.items():
                actual_type = str(df[col].dtype)
                if not self._types_match(actual_type, expected_type):
                    self.logger.error(f"Type mismatch for column {col}: expected {expected_type}, got {actual_type}")
                    return False
                    
            return True
        except Exception as e:
            self.logger.error(f"Schema validation failed: {str(e)}")
            return False
            
    def _types_match(self, actual_type: str, expected_type: str) -> bool:
        """
        Check if actual type matches expected type.
        Handles common type aliases and conversions.
        """
        # Map of common type aliases
        type_aliases = {
            'str': ['object', 'string'],
            'int': ['int64', 'int32', 'int16', 'int8'],
            'float': ['float64', 'float32'],
            'datetime64[ns]': ['datetime64[ns]', 'datetime64'],
            'bool': ['bool', 'boolean']
        }
        
        # Check if types match directly or through aliases
        for base_type, aliases in type_aliases.items():
            if expected_type == base_type and actual_type in aliases:
                return True
                
        return False 