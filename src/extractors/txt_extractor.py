import pandas as pd
from .base_extractor import BaseExtractor
from ..utils.logger import Logger

class TXTExtractor(BaseExtractor):
    """Extractor for TXT files."""
    
    def __init__(self, file_path: str):
        super().__init__(file_path)
        self.logger = Logger(__name__)
        
    def extract(self) -> pd.DataFrame:
        """Extract data from TXT file."""
        pass
