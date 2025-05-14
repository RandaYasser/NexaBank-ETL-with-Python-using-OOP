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
        self.logger.info(f"Starting loans transformation for {len(df)} records")
        partition_date = pd.to_datetime(self.partition_date)
        df['age'] = (partition_date - pd.to_datetime(df['utilization_date'])).dt.days
        self.logger.info("Added age")
        df['total_cost'] = self._calculate_total_cost(df['amount_utilized'], pd.to_datetime(df['utilization_date']))
        self.logger.info("Added total cost")
        self.logger.info("Starting encryption")
        df['loan_reason'] = df['loan_reason'].apply(Encryptor.encrypt)
        self.logger.info("Encrypted loan reason")
        self.logger.info(f"Completed loans transformation for {len(df)} records at {datetime.now()}")
        return df
    
    def _calculate_total_cost(self, amounts: pd.Series, utilization_dates: pd.Series) -> pd.Series:
        partition_date = pd.to_datetime(self.partition_date)
        years = ((partition_date - utilization_dates).dt.days / 365).apply(math.ceil)
        return amounts * (1.2 ** years) + 1000 * ((1.2 ** years - 1) / 0.2)

