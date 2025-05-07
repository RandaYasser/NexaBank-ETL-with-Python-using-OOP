import pandas as pd
from datetime import datetime
from .base_transformer import BaseTransformer

class TransactionsTransformer(BaseTransformer):
    """Transformer for money transfers and purchases data."""
    
    def __init__(self, partition_date: str, partition_hour: int):
        pass
        
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform transactions data by adding:
        - cost: transaction fee (50 cents + 0.1% of transaction amount)
        - total_amount: transaction_amount + cost
        """
        pass 