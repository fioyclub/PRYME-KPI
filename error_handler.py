"""
Error Handling and Logging Module

This module provides centralized error handling and logging functionality for the Telegram KPI Bot.
It includes:
- Centralized error handling for Google API operations
- Telegram-specific error handling
- Comprehensive logging configuration
- User-friendly error messages with emojis
- Retry mechanisms for transient failures
"""

import logging
import time
import functools
from typing import Optional, Callable, Any, Dict
from datetime import datetime
from googleapiclient.errors import HttpError
from telegram.error import TelegramError, NetworkError, RetryAfter, TimedOut
import google_sheets
import google_drive

# Configure logging
def setup_logging(log_level: str = "INFO", log_file: str = "kpi_bot.log") -> None:
    """
    Set up comprehensive logging configuration
    
    Args:
        log_level (str): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file (str): Log file path
    """
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    
    # Set up file handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(getattr(logging, log_level.upper()))
    
    # Set up console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Configure specific loggers
    logging.getLogger('telegram').setLevel(logging.WARNING)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('googleapiclient').setLevel(logging.WARNING)
    
    logging.info("Logging system initialized")

# Error message templates with emojis
ERROR_MESSAGES = {
    # Google API errors
    'google_auth_failed': "🔐 Authentication failed. Please check API credentials.",
    'google_permission_denied': "❌ Permission denied. Please check service account permissions.",
    'google_rate_limit': "⏳ Rate limit exceeded. Please try again in a few minutes.",
    'google_not_found': "🔍 Resource not found. Please check the configuration.",
    'google_quota_exceeded': "📊 API quota exceeded. Please try again later.",
    'google_network_error': "🌐 Network error. Please check your internet connection.",
    'google_server_error': "🔧 Google server error. Please try again later.",
    
    # Telegram errors
    'telegram_network_error': "📡 Network connection issue. Please try again.",
    'telegram_timeout': "⏰ Request timed out. Please try again.",
    'telegram_rate_limit': "⏳ Too many requests. Please wait before trying again.",
    'telegram_bot_blocked': "🚫 Bot was blocked by user.",
    'telegram_chat_not_found': "💬 Chat not found or bot was removed from chat.",
    'telegram_message_too_long': "📝 Message is too long. Please shorten your input.",
    'telegram_file_too_large': "📁 File is too large. Please use a smaller image.",
    'telegram_invalid_file': "📷 Invalid file format. Please use JPG or PNG images.",
    
    # Application errors
    'user_not_registered': "👤 You need to register first. Use /register to get started.",
    'user_not_authorized': "🔒 You don't have permission to perform this action.",
    'invalid_input': "⚠️ Invalid input. Please check your entry and try again.",
    'database_error': "💾 Database error. Please try again later.",
    'system_error': "⚙️ System error occurred. Please contact support if this persists.",
    'validation_error': "✏️ Input validation failed. Please check your data.",
    'memory_error': "🧠 Memory error. The system is handling this automatically.",
    'file_processing_error': "📁 File processing failed. Please try uploading again.",
    
    # Generic fallback
    'unknown_error': "❓ An unexpected error occurred. Please try again or contact support."
}

class ErrorHandler:
    """Centralized error handler for the KPI Bot"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.error_counts = {}  # Track error frequencies
    
    def handle_google_api_error(self, error: Exception, operation: str, user_id: Optional[int] = None) -> str:
        """
        Handle Google API errors with appropriate user messages and logging
        
        Args:
            error (Exception): The exception that occurred
            operation (str): Description of the operation that failed
            user_id (int, optional): User ID for context
            
        Returns:
            str: User-friendly error message
        """
        error_key = f"google_api_{operation}"
        self._increment_error_count(error_key)
        
        if isinstance(error, HttpError):
            status_code = error.resp.status
            
            if status_code == 401:
                message = ERROR_MESSAGES['google_auth_failed']
                self.logger.error(f"Google API authentication failed in {operation}: {error}")
            elif status_code == 403:
                message = ERROR_MESSAGES['google_permission_denied']
                self.logger.error(f"Google API permission denied in {operation}: {error}")
            elif status_code == 404:
                message = ERROR_MESSAGES['google_not_found']
                self.logger.warning(f"Google API resource not found in {operation}: {error}")
            elif status_code == 429:
                message = ERROR_MESSAGES['google_rate_limit']
                self.logger.warning(f"Google API rate limit exceeded in {operation}: {error}")
            elif status_code == 413:
                message = ERROR_MESSAGES['telegram_file_too_large']
                self.logger.warning(f"File too large in {operation}: {error}")
            elif 500 <= status_code < 600:
                message = ERROR_MESSAGES['google_server_error']
                self.logger.error(f"Google API server error in {operation}: {error}")
            else:
                message = f"❌ {operation} failed with error {status_code}. Please try again."
                self.logger.error(f"Google API error {status_code} in {operation}: {error}")
        else:
            message = ERROR_MESSAGES['google_network_error']
            self.logger.error(f"Google API network error in {operation}: {error}")
        
        # Log user context if provided
        if user_id:
            self.logger.info(f"Error context - User ID: {user_id}, Operation: {operation}")
        
        return message
    
    def handle_telegram_error(self, error: Exception, operation: str, user_id: Optional[int] = None) -> str:
        """
        Handle Telegram API errors with appropriate user messages and logging
        
        Args:
            error (Exception): The exception that occurred
            operation (str): Description of the operation that failed
            user_id (int, optional): User ID for context
            
        Returns:
            str: User-friendly error message
        """
        error_key = f"telegram_{operation}"
        self._increment_error_count(error_key)
        
        if isinstance(error, TimedOut):
            message = ERROR_MESSAGES['telegram_timeout']
            self.logger.warning(f"Telegram timeout in {operation}: {error}")
        elif isinstance(error, NetworkError):
            message = ERROR_MESSAGES['telegram_network_error']
            self.logger.warning(f"Telegram network error in {operation}: {error}")
        elif isinstance(error, RetryAfter):
            retry_after = error.retry_after
            message = f"⏳ Rate limited. Please wait {retry_after} seconds before trying again."
            self.logger.warning(f"Telegram rate limit in {operation}: retry after {retry_after}s")
        elif isinstance(error, TelegramError):
            error_message = str(error).lower()
            
            if 'blocked' in error_message:
                message = ERROR_MESSAGES['telegram_bot_blocked']
                self.logger.info(f"Bot blocked by user in {operation}: {error}")
            elif 'chat not found' in error_message:
                message = ERROR_MESSAGES['telegram_chat_not_found']
                self.logger.info(f"Chat not found in {operation}: {error}")
            elif 'message is too long' in error_message:
                message = ERROR_MESSAGES['telegram_message_too_long']
                self.logger.warning(f"Message too long in {operation}: {error}")
            elif 'file too large' in error_message:
                message = ERROR_MESSAGES['telegram_file_too_large']
                self.logger.warning(f"File too large in {operation}: {error}")
            else:
                message = f"📡 Telegram error: {error}"
                self.logger.error(f"Telegram error in {operation}: {error}")
        else:
            message = ERROR_MESSAGES['system_error']
            self.logger.error(f"Unknown error in {operation}: {error}")
        
        # Log user context if provided
        if user_id:
            self.logger.info(f"Error context - User ID: {user_id}, Operation: {operation}")
        
        return message
    
    def handle_application_error(self, error: Exception, operation: str, error_type: str = 'system_error', user_id: Optional[int] = None) -> str:
        """
        Handle application-specific errors
        
        Args:
            error (Exception): The exception that occurred
            operation (str): Description of the operation that failed
            error_type (str): Type of error for message selection
            user_id (int, optional): User ID for context
            
        Returns:
            str: User-friendly error message
        """
        error_key = f"app_{error_type}_{operation}"
        self._increment_error_count(error_key)
        
        message = ERROR_MESSAGES.get(error_type, ERROR_MESSAGES['unknown_error'])
        
        # Log based on error severity
        if error_type in ['validation_error', 'invalid_input']:
            self.logger.warning(f"Application validation error in {operation}: {error}")
        elif error_type in ['user_not_registered', 'user_not_authorized']:
            self.logger.info(f"Application authorization error in {operation}: {error}")
        else:
            self.logger.error(f"Application error ({error_type}) in {operation}: {error}")
        
        # Log user context if provided
        if user_id:
            self.logger.info(f"Error context - User ID: {user_id}, Operation: {operation}")
        
        return message
    
    def _increment_error_count(self, error_key: str) -> None:
        """Track error frequency for monitoring"""
        self.error_counts[error_key] = self.error_counts.get(error_key, 0) + 1
        
        # Log high-frequency errors
        if self.error_counts[error_key] % 10 == 0:
            self.logger.warning(f"High frequency error: {error_key} occurred {self.error_counts[error_key]} times")
    
    def get_error_statistics(self) -> Dict[str, int]:
        """Get error frequency statistics"""
        return self.error_counts.copy()
    
    def reset_error_statistics(self) -> None:
        """Reset error frequency counters"""
        self.error_counts.clear()
        self.logger.info("Error statistics reset")

# Global error handler instance
error_handler = ErrorHandler()

# Retry decorator for transient failures
def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0, exceptions: tuple = (Exception,)):
    """
    Decorator to retry function calls on transient failures
    
    Args:
        max_retries (int): Maximum number of retry attempts
        delay (float): Initial delay between retries in seconds
        backoff (float): Backoff multiplier for delay
        exceptions (tuple): Tuple of exceptions to catch and retry on
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    # Don't retry on certain errors
                    if isinstance(e, HttpError):
                        # Don't retry on client errors (4xx) except rate limiting
                        if 400 <= e.resp.status < 500 and e.resp.status != 429:
                            break
                    
                    if isinstance(e, TelegramError):
                        # Don't retry on user-related errors
                        error_msg = str(e).lower()
                        if any(keyword in error_msg for keyword in ['blocked', 'chat not found', 'unauthorized']):
                            break
                    
                    if attempt < max_retries:
                        logging.getLogger(__name__).warning(
                            f"Attempt {attempt + 1} failed for {func.__name__}: {e}. "
                            f"Retrying in {current_delay:.1f}s..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logging.getLogger(__name__).error(
                            f"All {max_retries + 1} attempts failed for {func.__name__}: {e}"
                        )
            
            # Re-raise the last exception if all retries failed
            raise last_exception
        
        return wrapper
    return decorator

# Specific retry decorators for different operations
def retry_google_api(max_retries: int = 3):
    """Retry decorator specifically for Google API operations"""
    return retry_on_failure(
        max_retries=max_retries,
        delay=1.0,
        backoff=2.0,
        exceptions=(HttpError, ConnectionError, TimeoutError)
    )

def retry_telegram_api(max_retries: int = 2):
    """Retry decorator specifically for Telegram API operations"""
    return retry_on_failure(
        max_retries=max_retries,
        delay=0.5,
        backoff=1.5,
        exceptions=(NetworkError, TimedOut)
    )

# Context managers for error handling
class ErrorContext:
    """Context manager for handling errors in specific operations"""
    
    def __init__(self, operation: str, user_id: Optional[int] = None, error_type: str = 'system'):
        self.operation = operation
        self.user_id = user_id
        self.error_type = error_type
        self.logger = logging.getLogger(__name__)
    
    def __enter__(self):
        self.logger.debug(f"Starting operation: {self.operation}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            if issubclass(exc_type, HttpError):
                error_msg = error_handler.handle_google_api_error(exc_val, self.operation, self.user_id)
            elif issubclass(exc_type, TelegramError):
                error_msg = error_handler.handle_telegram_error(exc_val, self.operation, self.user_id)
            else:
                error_msg = error_handler.handle_application_error(exc_val, self.operation, self.error_type, self.user_id)
            
            self.logger.error(f"Operation {self.operation} failed: {error_msg}")
            # Store error message for retrieval
            self.error_message = error_msg
            return False  # Don't suppress the exception
        else:
            self.logger.debug(f"Operation {self.operation} completed successfully")
            return True

# Utility functions for common error scenarios
def format_error_message(error_type: str, custom_message: Optional[str] = None) -> str:
    """
    Format user-friendly error message
    
    Args:
        error_type (str): Type of error
        custom_message (str, optional): Custom error message
        
    Returns:
        str: Formatted error message with emoji
    """
    if custom_message:
        return f"❌ {custom_message}"
    
    return ERROR_MESSAGES.get(error_type, ERROR_MESSAGES['unknown_error'])

def log_user_action(user_id: int, action: str, details: Optional[str] = None) -> None:
    """
    Log user actions for audit trail
    
    Args:
        user_id (int): User ID
        action (str): Action performed
        details (str, optional): Additional details
    """
    logger = logging.getLogger('user_actions')
    log_message = f"User {user_id} performed action: {action}"
    if details:
        log_message += f" - {details}"
    logger.info(log_message)

def log_system_event(event: str, details: Optional[str] = None, level: str = 'INFO') -> None:
    """
    Log system events
    
    Args:
        event (str): Event description
        details (str, optional): Additional details
        level (str): Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    logger = logging.getLogger('system_events')
    log_message = f"System event: {event}"
    if details:
        log_message += f" - {details}"
    
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger.log(log_level, log_message)

# Health check functions
def check_system_health() -> Dict[str, Any]:
    """
    Perform system health check
    
    Returns:
        dict: Health check results
    """
    health_status = {
        'timestamp': datetime.now().isoformat(),
        'google_sheets': False,
        'google_drive': False,
        'logging': True,
        'errors': error_handler.get_error_statistics()
    }
    
    try:
        # Import here to avoid circular imports
        import google_sheets
        # Test Google Sheets connection
        if google_sheets.sheets_service.service:
            health_status['google_sheets'] = google_sheets.test_sheets_connection()
    except Exception as e:
        logging.getLogger(__name__).error(f"Google Sheets health check failed: {e}")
    
    try:
        # Import here to avoid circular imports
        import google_drive
        # Test Google Drive connection
        if google_drive.drive_service.service:
            health_status['google_drive'] = google_drive.test_drive_connection()
    except Exception as e:
        logging.getLogger(__name__).error(f"Google Drive health check failed: {e}")
    
    return health_status

# Initialize logging on module import
setup_logging()