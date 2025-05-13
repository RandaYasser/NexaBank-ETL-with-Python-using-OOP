from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from typing import Optional
from .logger import Logger
import traceback
from dotenv import load_dotenv
from smtplib import SMTP_SSL


class ErrorHandler:
    """Handles errors and exceptions across the application."""
    
    def __init__(self):
        self.logger = Logger(__name__)
        load_dotenv()
        self.email_config = self._load_email_config()
        

    def _load_email_config(self) -> dict:
        """Load email configuration from environment variables."""
        try:
            return {
                'smtp_server': os.getenv('SMTP_SERVER'),
                'smtp_port': os.getenv('SMTP_PORT'),
                'sender_email': os.getenv('SENDER_EMAIL'),
                'sender_password': os.getenv('SENDER_PASSWORD'),
                'recipient_email': os.getenv('RECIPIENT_EMAIL'),
        }
        except Exception as e:
            self.logger.error(f"Failed to load email config: {str(e)}")
            return {}
            
    def handle_error(self, error: Exception, context: str, retry_function: Optional[callable] = None):
        """
        Handle an error by logging it, sending notification, and optionally retrying.
        
        Args:
            error: The exception that occurred
            context: Description of where/when the error occurred
            retry_function: Optional function to retry after error
        """
        error_message = f"Error in {context}: {str(error)}\n{traceback.format_exc()}"
        self.logger.error(error_message)
        
        # Send email notification
        self._send_error_notification(error_message)
        
        # Retry if function provided
        if retry_function:
            try:
                self.logger.info(f"Retrying {context}...")
                retry_function()
            except Exception as retry_error:
                self.logger.error(f"Retry failed: {str(retry_error)}")
                
    def _send_error_notification(self, error_message: str):
        """Send email notification about the error."""
    
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_config.get('sender_email')
            msg['To'] = self.email_config.get('recipient_email')
            msg['Subject'] = "Pipeline Error Notification"
            
            body = f"The following error occurred in the data pipeline:\n\n{error_message}"
            msg.attach(MIMEText(body, 'plain'))
            
            server = SMTP_SSL(self.email_config.get('smtp_server'), int(self.email_config.get('smtp_port')))
            server.ehlo()
            
            server.login(
                self.email_config.get('sender_email'),
                self.email_config.get('sender_password')
            )
            self.logger.info("Connected to email server")
            server.send_message(msg)
            self.logger.info(f"Mail sent successfully to {self.email_config.get('recipient_email')}")
            server.quit()
            self.logger.info("Disconnected from email server")
        except Exception as e:
            self.logger.error(f"Failed to send error notification email: {str(e)}") 