import pandas as pd
from datetime import datetime
from .base_transformer import BaseTransformer

class TransactionsTransformer(BaseTransformer):
    """Transformer for money transfers and purchases data. Base columns: sender, receiver, transaction_amount, transaction_date. """
    
    def __init__(self, partition_date: str, partition_hour: int):
        super().__init__(partition_date, partition_hour)
        
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform transactions data by adding:
        - cost: transaction fee (50 cents + 0.1% of transaction amount)
        - total_amount: transaction_amount + cost
        """
        df['cost'] = df['transaction_amount'] * 0.001 + 0.5
        self.logger.info("Added cost")
        df['total_amount'] = df['transaction_amount'] + df['cost']
        self.logger.info("Added total amount")
        return df
    