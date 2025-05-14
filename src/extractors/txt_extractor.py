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
            dict_metadata = self.get_metadata()
            self.logger.info(f"Started extracting {dict_metadata['table_name']} from TXT file at {self.file_path} ")
            df = pd.read_csv(self.file_path, sep='|')
            self.logger.info(f"Completed extracting {dict_metadata['table_name']} Number of rows extracted = {df.shape[0]} , Extracted schema = {df.columns.tolist()} , Partition date = {dict_metadata['partition_date']} , Partition hour = {dict_metadata['partition_hour']} ")
            return df, dict_metadata
        except Exception as e:
            self.logger.error(f"Error extracting data from {self.file_path}: {e}")
            return None, None
        