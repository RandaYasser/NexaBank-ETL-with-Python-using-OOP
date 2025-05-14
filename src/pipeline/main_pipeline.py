import os
import time
from datetime import datetime
import pandas as pd
from typing import Dict, Optional, Tuple
from itertools import count
from src.extractors.csv_extractor import CSVExtractor
from src.extractors.json_extractor import JSONExtractor
from src.extractors.txt_extractor import TXTExtractor
from src.transformers.customer_profile_transformer import CustomerProfileTransformer
from src.transformers.credit_cards_billing_transformer import CreditCardsBillingTransformer
from src.transformers.support_tickets_transformer import SupportTicketsTransformer
from src.transformers.loans_transformer import LoansTransformer
from src.transformers.transactions_transformer import TransactionsTransformer
from src.writers.parquet_writer import ParquetWriter
from src.writers.csv_writer import CsvWriter
from src.utils.logger import Logger
from src.utils.error_handler import ErrorHandler
from src.validators.schema_validator import SchemaValidator

class MainPipeline:
    """Main pipeline class that orchestrates the ETL process."""
    
    def __init__(self, incoming_data_dir: str = "incoming_data"):
        self.incoming_data_dir = incoming_data_dir
        self.logger = Logger(__name__)
        self.error_handler = ErrorHandler()
        
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        schema_path = os.path.join(BASE_DIR, "..", "schema", "tables_schema.json")
        self.schema_validator = SchemaValidator(schema_path)
        
        self.email_config_path = os.path.join(BASE_DIR, "..", "..", "config", "email_config.txt")
        self.sequence_generator = count(start=1, step=1)
        
        # Setup checkpoint directories
        self.checkpoint_dirs = {
            'extracted': 'checkpoint/extracted',
            'transformed': 'checkpoint/transformed',
            'loaded': 'checkpoint/loaded'
        }
        self._setup_checkpoint_dirs()
        
        # Map file patterns to their respective extractors and transformers
        self.file_handlers = {
            "customer_profiles.csv": (CSVExtractor, CustomerProfileTransformer, "customer_profiles"),
            "credit_cards_billing.csv": (CSVExtractor, CreditCardsBillingTransformer, "credit_cards_billing"),
            "support_tickets.csv": (CSVExtractor, SupportTicketsTransformer, "support_tickets"),
            "loans.txt": (TXTExtractor, LoansTransformer, "loans"),
            "transactions.json": (JSONExtractor, TransactionsTransformer, "transactions")
        }
        
    def _setup_checkpoint_dirs(self):
        """Create checkpoint directory structure."""
        for dir_path in self.checkpoint_dirs.values():
            os.makedirs(dir_path, exist_ok=True)
            
    def _get_current_hour_dir(self) -> str:
        """Get the current hour directory path."""
        current_date = datetime.now().strftime("%Y-%m-%d")
        current_hour = datetime.now().strftime("%H")
        return os.path.join(self.incoming_data_dir, current_date, current_hour)
        
    def _get_checkpoint_path(self, stage: str, date: str, hour: str) -> str:
        """Get checkpoint directory path for a specific stage, date, and hour.
        Args:
            stage (str): The stage of the pipeline (e.g., 'extracted', 'transformed', 'loaded')
            date (str): The date in YYYY-MM-DD format
            hour (str): The hour in HH format
        Returns:
            str: The full path to the checkpoint directory
        """
        return os.path.join(self.checkpoint_dirs[stage], date, hour)
        
    def _mark_file_processing(self, file_path: str, seq_num: int) -> str:
        """Rename file to mark it as processing."""
        path_without_ext, ext = file_path.split(".")
        new_path = f"{path_without_ext}_processing__{seq_num}.{ext}"
        os.rename(file_path, new_path)
        return new_path
        
    def _mark_file_processed(self, file_path: str):
        """Rename file to mark it as processed."""
        new_path = file_path.replace('_processing__', '_processed__')
        os.rename(file_path, new_path)
        
    def run(self):
        """Main pipeline execution loop."""
        self.logger.info("Starting pipeline execution")
        while True:
            try:
                current_hour_dir = self._get_current_hour_dir()
                # sleep if directory does not exist yet
                if not os.path.exists(current_hour_dir):
                    time.sleep(10)
                    continue
                    
                # Get files that need processing
                files_to_process = [
                    f for f in os.listdir(current_hour_dir)
                    if not ('_processing__' in f or '_processed__' in f)
                ]
                
                if not files_to_process:
                    time.sleep(10)
                    continue
                
                # set batch number
                seq_num = next(self.sequence_generator)
                # Process each file
                for filename in files_to_process:
                    file_path = os.path.join(current_hour_dir, filename)
                    self._process_file(file_path, seq_num)
                    
            except Exception as e:
                self.error_handler.handle_error(e, "Pipeline execution")
                
                
    def _process_file(self, file_path: str, seq_num: int):
        """Process a single file through the pipeline stages."""
        try:
            file_parts = file_path.split(os.sep)
            filename = file_parts[-1]
            current_date = file_parts[-3]
            current_hour = file_parts[-2]
            
            # Mark file as processing
            processing_path = self._mark_file_processing(file_path, seq_num)
            self.logger.info(f"Started processing {filename}")
            
            # Get filename handler
            handler = self._get_file_handler(filename)
            if not handler:
                self.logger.warning(f"No handler found for file: {filename}")
                return
                
            extractor_class, transformer_class, table_name = handler
            
            # Extract and validate
            extractor = extractor_class(processing_path)
            df, metadata = extractor.extract()
            
            # Validate schema
            if not self.schema_validator.validate_schema(df, metadata['table_name']):
                error_msg = f"Schema validation failed for {filename}"
                self.logger.error(error_msg)
                self.error_handler.handle_error(Exception(error_msg), f"Schema validation for {filename}")
                return
                
            # Save to extracted checkpoint
            extracted_dir = self._get_checkpoint_path('extracted', current_date, current_hour)
            os.makedirs(extracted_dir, exist_ok=True)
            extracted_path = os.path.join(extracted_dir, table_name)
            extracted_writer = CsvWriter(extracted_dir)
            extracted_writer.write(df, table_name)
            
            # Transform
            transformer = transformer_class(current_date, current_hour)
            df = transformer.transform(df)
            
            # Save to transformed checkpoint
            transformed_dir = self._get_checkpoint_path('transformed', current_date, current_hour)
            os.makedirs(transformed_dir, exist_ok=True)
            transformed_writer = CsvWriter(transformed_dir)
            transformed_writer.write(df, table_name)
            
            # Load to HDFS
            loaded_dir = self._get_checkpoint_path('loaded', current_date, current_hour)
            os.makedirs(loaded_dir, exist_ok=True)
            loaded_path = os.path.join(loaded_dir, table_name)
            loaded_writer = ParquetWriter(loaded_dir)
            loaded_writer.write(df, f'{table_name}_{seq_num}')
            hdfs_path = f"/user/hive/warehouse/nexabank.db/{table_name}/"
            try:
                loaded_writer.upload_to_hdfs(f"{loaded_path}_{seq_num}.parquet", hdfs_path)
            except Exception as e:
                self.error_handler.handle_error(e, f"Uploading to HDFS for {filename}")
           
            
            # Mark as processed
            self._mark_file_processed(processing_path)
            self.logger.info(f"Successfully processed {filename}")
            
        except Exception as e:
            self.error_handler.handle_error(e, f"Processing file {filename}")
            
    def _get_file_handler(self, filename: str) -> Optional[Tuple]:
        """Get the appropriate extractor, transformer, and table name for a file."""
        for pattern, handler in self.file_handlers.items():
            if filename == pattern:
                return handler
        return None
 