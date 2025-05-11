import pandas as pd
from datetime import datetime
from .base_transformer import BaseTransformer

class SupportTicketsTransformer(BaseTransformer):
    """Transformer for support tickets data. Base columns: ticket_id, customer_id, complaint_category, complaint_date, severity. """
    
    def __init__(self, partition_date: str, partition_hour: int):
        super().__init__(partition_date, partition_hour)
        
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform support tickets data by adding:
        - age: number of days since the ticket was issued
        """
        df['age'] = (self.partition_date - df['issue_date']).dt.days
        return df
