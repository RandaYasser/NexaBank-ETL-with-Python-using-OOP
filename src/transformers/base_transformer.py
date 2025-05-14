from abc import ABC, abstractmethod
import pandas as pd
from datetime import datetime
from typing import Dict, Any
import logging

class BaseTransformer(ABC):
    """Base class for all data transformers."""
    
    def __init__(self, partition_date: str, partition_hour: int):
        self.partition_date = partition_date
        self.partition_hour = partition_hour
        self.logger = logging.getLogger(__name__)
        
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform the input DataFrame according to business rules."""
        pass
        
    def add_metadata_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add common metadata columns to all transformed DataFrames."""
        self.logger.info(f"Started adding metadata columns to the DataFrame")
        df['processing_time'] = datetime.now()
        df['partition_date'] = self.partition_date
        df['partition_hour'] = self.partition_hour
        self.logger.info(f"Completed adding metadata columns to the DataFrame ,partition_date = {self.partition_date} ,partition_hour = {self.partition_hour} ,processing_time = {datetime.now()}")
        return df
        
