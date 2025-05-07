from abc import ABC, abstractmethod
from typing import Dict, Any
import pandas as pd

class BaseExtractor(ABC):
    """Base class for all data extractors."""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        
    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Extract data from source file and return as DataFrame."""
        pass
    
