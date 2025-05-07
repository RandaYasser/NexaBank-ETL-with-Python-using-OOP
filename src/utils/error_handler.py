import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from typing import Optional
from .logger import Logger
import traceback

class ErrorHandler:
    """Handles errors and exceptions across the application."""
    
    def __init__(self, email_config_path: str = 'config/email_config.txt'):
        self.logger = Logger(__name__)
        self.email_config = self._load_email_config(email_config_path)
        
    def _load_email_config(self, config_path: str) -> dict:
        """Load email configuration from file."""
        config = {}
        try:
            with open(config_path, 'r') as f:
                for line in f:
                    key, value = line.strip().split('=')
                    config[key.strip()] = value.strip()
            return config
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
        if not self.email_config:
            self.logger.error("Cannot send error notification: email config not loaded")
            return
            
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_config.get('sender_email')
            msg['To'] = self.email_config.get('recipient_email')
            msg['Subject'] = "Pipeline Error Notification"
            
            body = f"The following error occurred in the data pipeline:\n\n{error_message}"
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(
                self.email_config.get('sender_email'),
                self.email_config.get('sender_password')
            )
            server.send_message(msg)
            server.quit()
            
            self.logger.info("Error notification email sent successfully")
        except Exception as e:
            self.logger.error(f"Failed to send error notification email: {str(e)}") 