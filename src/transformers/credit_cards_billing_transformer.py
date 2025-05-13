import pandas as pd
from datetime import datetime
from .base_transformer import BaseTransformer

class CreditCardsBillingTransformer(BaseTransformer):
    """Transformer for credit cards billing data. Base columns: bill_id, customer_id, month, amount_due, amount_paid, payment_date. """
    
    def __init__(self, partition_date: str, partition_hour: int):
        super().__init__(partition_date, partition_hour)
        
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform credit cards billing data by adding:
        - fully_paid: boolean indicating if bill was fully paid
        - debt: remaining amount after payment
        - late_days: days between due date (1st of month) and payment date
        - fine: late_days * 5.15
        - total_amount: amount_due + fine
        """
        # Set due date as first day of each month
        df["due_date"] = pd.to_datetime(df["month"] + "-01") 
        df['payment_date'] = pd.to_datetime(df['payment_date'])
        df['late_days'] = (df['payment_date'] - df['due_date']).dt.days
        df['fully_paid'] = df['amount_due'] >= df['amount_paid']
        df['debt'] = df['amount_due'] - df['amount_paid']
        df['fine'] = df['late_days'] * 5.15
        df['total_amount'] = df['amount_due'] + df['fine']
        return df
