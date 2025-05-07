import pandas as pd
from datetime import datetime
from .base_transformer import BaseTransformer

class CreditCardsBillingTransformer(BaseTransformer):
    """Transformer for credit cards billing data."""
    
    def __init__(self, partition_date: str, partition_hour: int):
        pass
        
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform credit cards billing data by adding:
        - fully_paid: boolean indicating if bill was fully paid
        - debt: remaining amount after payment
        - late_days: days between due date (1st of month) and payment date
        - fine: late_days * 5.15
        - total_amount: amount_due + fine
        """
        pass 