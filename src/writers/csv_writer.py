import pandas as pd
import os
from src.writers.base_writer import BaseWriter

class CsvWriter(BaseWriter):
    """Writer for CSV files."""
    def __init__(self, output_dir: str):
        super().__init__(output_dir)

    def write(self, df: pd.DataFrame, file_name: str) -> str:
        """
        Write DataFrame to CSV format.
        
        Args:
            df: DataFrame to write
            file_name: Name of the output file (without extension)
            
        Returns:
            str: Path to the written file
        """
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Construct output path
        output_path = os.path.join(self.output_dir, f"{file_name}.csv")
        
        try:
            # Write to parquet
            df.to_csv(output_path, index=False)
            self.logger.info(f"Successfully wrote {len(df)} rows to {output_path}")
            return output_path
        except Exception as e:
            self.logger.error(f"Failed to write csv file: {str(e)}")
            raise 