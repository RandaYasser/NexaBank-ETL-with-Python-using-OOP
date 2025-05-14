import pandas as pd
from datetime import datetime
from src.transformers.base_transformer import BaseTransformer

class SupportTicketsTransformer(BaseTransformer):
    """Transformer for support tickets data. Base columns: ticket_id, customer_id, complaint_category, complaint_date, severity. """
    
    def __init__(self, partition_date: str, partition_hour: int):
        super().__init__(partition_date, partition_hour)
        
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform support tickets data by adding:
        - age: number of days since the ticket was issued
        """
        self.logger.info(f"Starting support tickets transformation for {len(df)} records")
        partition_date = pd.to_datetime(self.partition_date)
        df['age'] = (partition_date - pd.to_datetime(df['complaint_date'])).dt.days
        self.logger.info("Added age")
        self.logger.info(f"Completed support tickets transformation for {len(df)} records at {datetime.now()}")
        return df
