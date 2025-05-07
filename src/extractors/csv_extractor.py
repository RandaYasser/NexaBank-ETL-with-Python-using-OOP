import pandas as pd
from .base_extractor import BaseExtractor
from ..utils.logger import Logger

class CSVExtractor(BaseExtractor):
    """Extractor for CSV files."""
    
    def __init__(self, file_path: str):
        super().__init__(file_path)
        self.logger = Logger(__name__)
        
    def extract(self) -> pd.DataFrame:
        """Extract data from CSV file."""
        pass
    
    def get_metadata(self) -> dict[str:str]:
        """Return partition date, partition hour to be used in transformation"""
        pass