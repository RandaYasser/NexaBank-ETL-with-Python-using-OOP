from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict,Tuple,Optional
import pandas as pd
import os

class BaseExtractor(ABC):
    """Base class for all data extractors."""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        
    @abstractmethod
    def extract(self) -> Tuple[Optional[pd.DataFrame], Dict[str, str]]:
        """Extract data from the source file and return DataFrame with partition info"""
        pass
        
    def get_metadata(self) -> dict:
        """Extract partition date and hour from file path"""
        path_parts = self.file_path.split(os.sep)
        partition_date = path_parts[-3]  # 2025-04-18
        partition_hour = path_parts[-2]  # 14
        return {'partition_date': partition_date, 'partition_hour': partition_hour}
    
    
