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
        self.logger.info(f"Starting credit cards billing transformation for {len(df)} records")
        
        # Set due date as first day of each month
        df["due_date"] = pd.to_datetime(df["month"] + "-01")
        self.logger.info("Added due date")
        
        df['payment_date'] = pd.to_datetime(df['payment_date'])
        self.logger.info("Converted payment dates")
        
        df['late_days'] = (df['payment_date'] - df['due_date']).dt.days
        self.logger.info("Added late days")
        
        df['fully_paid'] = df['amount_due'] >= df['amount_paid']
        df['debt'] = df['amount_due'] - df['amount_paid']
        self.logger.info("Added payment status and debt")
        
        df['fine'] = df['late_days'] * 5.15
        df['total_amount'] = df['amount_due'] + df['fine']
        self.logger.info("Added fines and total amounts")

        self.logger.info(f"Completed credit cards billing transformation for {len(df)} records at {datetime.now()}")
        
        return df
