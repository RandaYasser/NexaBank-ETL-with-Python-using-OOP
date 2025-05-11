import pandas as pd
from datetime import datetime
from .base_transformer import BaseTransformer
from ..utils.encryptor import Encryptor
import math

class LoansTransformer(BaseTransformer):
    """Transformer for loans data. Base columns: customer_id, loan_type, amount_utilized, utilization_date, loan_reason. """
    
    def __init__(self, partition_date: str, partition_hour: int):
        super().__init__(partition_date, partition_hour)
        
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform loans data by adding:
        - age: number of days since the transaction was completed
        - total_cost: annual cost of the loan (20% of value + $1000)
        """
        df['age'] = (self.partition_date - df['utilization_date']).dt.days
        df['total_cost'] = self._calculate_total_cost(df['amount_utilized'], df['utilization_date'])
        df['loan_reason'] = Encryptor.encrypt(df['loan_reason'])
        return df
    
    def _calculate_total_cost(self, amount_utilized: float, utilization_date: datetime) -> float:
        """Calculate total cost of the loan across the years"""
        years = math.ceil((self.partition_date - utilization_date).days / 365)
        return amount_utilized * (1.2 ** years) + 1000 * ((1.2 ** years - 1) / 0.2)

    
