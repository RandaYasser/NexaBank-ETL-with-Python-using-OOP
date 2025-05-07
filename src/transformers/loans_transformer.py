import pandas as pd
from datetime import datetime
from .base_transformer import BaseTransformer

class LoansTransformer(BaseTransformer):
    """Transformer for loans data."""
    
    def __init__(self, partition_date: str, partition_hour: int):
        pass
        
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform loans data by adding:
        - age: number of days since the transaction was completed
        - total_cost: annual cost of the loan (20% of value + $1000)
        """
        pass 