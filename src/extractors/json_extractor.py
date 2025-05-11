import pandas as pd
import json
from typing import Dict, Optional, Tuple
from .base_extractor import BaseExtractor
from ..utils.logger import Logger

class JSONExtractor(BaseExtractor):
    """Extractor for JSON files."""
    
    def __init__(self, file_path: str):
        super().__init__(file_path)
        self.logger = Logger(__name__)
        
    def extract(self) -> Tuple[Optional[pd.DataFrame], Dict[str, str]]:
        """Extract data from JSON file and return DataFrame with partition info"""
        try: 
            # Read JSON file
            with open(self.file_path, 'r') as f:
                data = json.load(f)
            
            # Convert to DataFrame
            df = pd.DataFrame(data)

            return df, self.get_metadata()
            
        except Exception as e:
            self.logger.error(f"Error extracting data from {self.file_path}: {e}")
            return None, self.get_metadata()
