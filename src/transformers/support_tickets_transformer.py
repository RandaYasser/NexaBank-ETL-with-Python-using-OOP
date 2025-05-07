import pandas as pd
from datetime import datetime
from .base_transformer import BaseTransformer

class SupportTicketsTransformer(BaseTransformer):
    """Transformer for support tickets data."""
    
    def __init__(self, partition_date: str, partition_hour: int):
        pass
        
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform support tickets data by adding:
        - age: number of days since the ticket was issued
        """
        pass 