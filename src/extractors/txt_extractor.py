import pandas as pd
from typing import Dict, Optional, Tuple
from .base_extractor import BaseExtractor
from ..utils.logger import Logger

class TXTExtractor(BaseExtractor):
    """Extractor for TXT files."""
    
    def __init__(self, file_path: str):
        super().__init__(file_path)
        self.logger = Logger(__name__)
        
    def extract(self) -> Tuple[Optional[pd.DataFrame], Dict[str, str]]:
        """Extract data from TXT file."""
        try:
            df = pd.read_csv(self.file_path, sep='|')
            return df, self.get_metadata()
        except Exception as e:
            self.logger.error(f"Error extracting data from {self.file_path}: {e}")
            return None, None
        