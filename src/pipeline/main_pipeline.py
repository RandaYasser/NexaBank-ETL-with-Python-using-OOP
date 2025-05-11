import os
import time
from datetime import datetime
import pandas as pd
from typing import Dict, Optional, Tuple
from ..extractors.csv_extractor import CSVExtractor
from ..extractors.json_extractor import JSONExtractor
from ..extractors.txt_extractor import TXTExtractor
from ..transformers.customer_profile_transformer import CustomerProfileTransformer
from ..transformers.credit_cards_billing_transformer import CreditCardsBillingTransformer
from ..transformers.support_tickets_transformer import SupportTicketsTransformer
from ..transformers.loans_transformer import LoansTransformer
from ..transformers.transactions_transformer import TransactionsTransformer
from ..writers.parquet_writer import ParquetWriter
from ..utils.logger import Logger
from ..utils.error_handler import ErrorHandler

class MainPipeline:
    """Main pipeline class that orchestrates the ETL process."""
    
    def __init__(self, incoming_data_dir: str = "incoming_data"):
        self.incoming_data_dir = incoming_data_dir
        self.logger = Logger(__name__)
        self.error_handler = ErrorHandler()
        
        # Map file patterns to their respective extractors and transformers
        self.file_handlers = {
            "customer_profiles.csv": (CSVExtractor, CustomerProfileTransformer, "customer_profiles"),
            "credit_cards_billing.csv": (CSVExtractor, CreditCardsBillingTransformer, "credit_cards_billing"),
            "support_tickets.csv": (CSVExtractor, SupportTicketsTransformer, "support_tickets"),
            "loans.txt": (TXTExtractor, LoansTransformer, "loans"),
            "transactions.json": (JSONExtractor, TransactionsTransformer, "transactions")
        }
        
        #track processed files
        self.processed_files = set()
        
    def run(self):
        """Main pipeline execution loop."""
        self.logger.info("Starting pipeline execution")
        
        while True:
            try:
                pass
            except Exception as e:
                self.error_handler.handle_error(e, "Pipeline execution", self.run)
 