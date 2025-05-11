from abc import ABC, abstractmethod
import pandas as pd
from datetime import datetime
from typing import Dict, Any

class BaseTransformer(ABC):
    """Base class for all data transformers."""
    
    def __init__(self, partition_date: str, partition_hour: int):
        self.partition_date = partition_date
        self.partition_hour = partition_hour
        
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform the input DataFrame according to business rules."""
        pass
        
    def add_metadata_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add common metadata columns to all transformed DataFrames."""
        df['processing_time'] = datetime.now()
        df['partition_date'] = self.partition_date
        df['partition_hour'] = self.partition_hour
        return df
        
