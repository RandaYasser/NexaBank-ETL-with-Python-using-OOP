import pandas as pd
from src.transformers.base_transformer import BaseTransformer
from datetime import datetime
class CustomerProfileTransformer(BaseTransformer):
    """Transformer for customer profile data. Base columns: customer_id, name, gender, age, city, account_open_date, product_type, 
customer_tier. """
    
    def __init__(self, partition_date: str, partition_hour: int):
        super().__init__(partition_date, partition_hour)
        
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform customer profile data by adding:
        - tenure: number of years since customer joined
        - customer_segment: categorization based on tenure
        """
        self.logger.info(f"Starting customer profile transformation for {len(df)} records")
        partition_date = pd.to_datetime(self.partition_date)
        df['tenure'] = (partition_date -  pd.to_datetime(df['account_open_date'])).dt.days // 365
        self.logger.info("Added tenure")
        df['customer_segment'] = df['tenure'].apply(self._determine_segment)
        self.logger.info("Added customer segment")
        self.logger.info(f"Completed customer profile transformation for {len(df)} records at {datetime.now()}")
        return df
            
    def _determine_segment(self, tenure: int) -> str:
        """Determine customer segment based on tenure."""
        if tenure <= 1:
            return 'New Customer'
        elif tenure > 5:
            return 'Loyal'
        else:
            return 'Normal'
