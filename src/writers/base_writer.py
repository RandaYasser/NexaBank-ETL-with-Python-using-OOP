from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional
import subprocess
from ..utils.logger import Logger

class BaseWriter:
    """Base class for all data writers."""
    
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.logger = Logger(__name__)
        
    @abstractmethod
    def write(self, df: pd.DataFrame, file_name: str) -> str:
        """Write DataFrame to storage in specified format."""
        pass
        
    def upload_to_hdfs(self, local_path: str, hdfs_path: str):
        """Upload file to HDFS using subprocess."""
        try:
            command = f"hadoop fs -put -f {local_path} {hdfs_path}"
            subprocess.run(command, shell=True, check=True)
            self.logger.info(f"Successfully uploaded {local_path} to HDFS at {hdfs_path}")
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to upload to HDFS: {str(e)}")
            raise 