"""
Utility Functions Module

This module provides shared functionality including:
- InlineKeyboard generation functions
- Progress bar formatting with emojis
- Date/time utility functions for folder naming
- Success/error message formatting functions
- Data validation helpers
"""

import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

# Try to import telegram components, gracefully handle if not available
try:
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False
    # Create dummy classes for type hints when telegram is not available
    class InlineKeyboardButton:
        def __init__(self, text, callback_data):
            self.text = text
            self.callback_data = callback_data
    
    class InlineKeyboardMarkup:
        def __init__(self, keyboard):
            self.inline_keyboard = keyboard

# Import error handling utilities
try:
    from error_handler import format_error_message as error_format_message, log_user_action
    ERROR_HANDLER_AVAILABLE = True
except ImportError:
    ERROR_HANDLER_AVAILABLE = False
    # Fallback function if error_handler is not available
    def error_format_message(error_type: str, custom_message: Optional[str] = None) -> str:
        return f"❌ {custom_message or 'An error occurred'}"
    
    def log_user_action(user_id: int, action: str, details: Optional[str] = None) -> None:
        pass

logger = logging.getLogger(__name__)


def create_sales_rep_keyboard(sales_reps: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    """
    Generate InlineKeyboard for sales representative selection
    
    Args:
        sales_reps (List[Dict]): List of sales rep dictionaries with 'user_id' and 'name' keys
        
    Returns:
        InlineKeyboardMarkup: Telegram inline keyboard for selection
    """
    if not sales_reps:
        # Return empty keyboard if no sales reps
        return InlineKeyboardMarkup([])
    
    keyboard = []
    for rep in sales_reps:
        user_id = rep.get('user_id')
        name = rep.get('name', f'User {user_id}')
        
        # Create button with callback data containing user_id
        button = InlineKeyboardButton(
            text=f"👤 {name}",
            callback_data=f"select_user_{user_id}"
        )
        keyboard.append([button])
    
    # Add cancel button
    cancel_button = InlineKeyboardButton(
        text="❌ Cancel",
        callback_data="cancel_selection"
    )
    keyboard.append([cancel_button])
    
    return InlineKeyboardMarkup(keyboard)


def create_confirmation_keyboard(action: str, data: str = "") -> InlineKeyboardMarkup:
    """
    Generate confirmation keyboard for yes/no actions
    
    Args:
        action (str): Action identifier for callback data
        data (str): Additional data to include in callback
        
    Returns:
        InlineKeyboardMarkup: Confirmation keyboard
    """
    keyboard = [
        [
            InlineKeyboardButton("✅ Yes", callback_data=f"confirm_{action}_{data}"),
            InlineKeyboardButton("❌ No", callback_data=f"cancel_{action}_{data}")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


def create_menu_keyboard(menu_items: List[Dict[str, str]]) -> InlineKeyboardMarkup:
    """
    Generate menu keyboard from list of menu items
    
    Args:
        menu_items (List[Dict]): List of menu items with 'text' and 'callback_data' keys
        
    Returns:
        InlineKeyboardMarkup: Menu keyboard
    """
    keyboard = []
    for item in menu_items:
        button = InlineKeyboardButton(
            text=item['text'],
            callback_data=item['callback_data']
        )
        keyboard.append([button])
    
    return InlineKeyboardMarkup(keyboard)


def format_progress_bar(current: int, target: int, bar_length: int = 10) -> str:
    """
    Create visual progress bar with percentage and emojis
    
    Args:
        current (int): Current progress value
        target (int): Target value
        bar_length (int): Length of the progress bar in characters
        
    Returns:
        str: Formatted progress bar string
    """
    if target <= 0:
        return "🚫 No target set"
    
    # Calculate percentage
    percentage = min((current / target) * 100, 100.0)
    
    # Calculate filled bars
    filled_bars = int((current / target) * bar_length)
    filled_bars = min(filled_bars, bar_length)
    
    # Create progress bar
    filled = "🟩" * filled_bars
    empty = "⬜" * (bar_length - filled_bars)
    progress_bar = filled + empty
    
    # Add status emoji
    if percentage >= 100:
        status_emoji = "🎉"
    elif percentage >= 75:
        status_emoji = "🔥"
    elif percentage >= 50:
        status_emoji = "💪"
    elif percentage >= 25:
        status_emoji = "📈"
    else:
        status_emoji = "🚀"
    
    return f"{status_emoji} {progress_bar} {percentage:.1f}% ({current}/{target})"


def format_progress_summary(meetup_current: int, meetup_target: int, 
                          sales_current: float, sales_target: float) -> str:
    """
    Format complete progress summary for both meetups and sales
    
    Args:
        meetup_current (int): Current meetup count
        meetup_target (int): Target meetup count
        sales_current (float): Current sales amount
        sales_target (float): Target sales amount
        
    Returns:
        str: Formatted progress summary
    """
    meetup_bar = format_progress_bar(meetup_current, meetup_target)
    sales_bar = format_progress_bar(int(sales_current), int(sales_target))
    
    # Overall completion
    meetup_pct = (meetup_current / meetup_target * 100) if meetup_target > 0 else 0
    sales_pct = (sales_current / sales_target * 100) if sales_target > 0 else 0
    overall_pct = (meetup_pct + sales_pct) / 2
    
    if overall_pct >= 100:
        overall_emoji = "🏆"
        overall_status = "All targets achieved!"
    elif overall_pct >= 75:
        overall_emoji = "⭐"
        overall_status = "Excellent progress!"
    elif overall_pct >= 50:
        overall_emoji = "👍"
        overall_status = "Good progress!"
    elif overall_pct >= 25:
        overall_emoji = "📊"
        overall_status = "Making progress!"
    else:
        overall_emoji = "💼"
        overall_status = "Just getting started!"
    
    return f"""📊 **KPI Progress Summary**

🤝 **Meetups:**
{meetup_bar}

💰 **Sales:**
{sales_bar}

{overall_emoji} **Overall:** {overall_status} ({overall_pct:.1f}%)"""


def get_current_month_folder() -> str:
    """
    Generate current month folder name for Google Drive organization
    Format: MM_MonthName (e.g., "03_March")
    
    Returns:
        str: Formatted month folder name
    """
    now = datetime.now()
    month_names = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    
    month_num = now.month
    month_name = month_names[month_num - 1]
    
    return f"{month_num:02d}_{month_name}"


def get_month_folder(year: int, month: int) -> str:
    """
    Generate month folder name for specific year and month
    Format: MM_MonthName (e.g., "03_March")
    
    Args:
        year (int): Year
        month (int): Month (1-12)
        
    Returns:
        str: Formatted month folder name
        
    Raises:
        ValueError: If month is not between 1-12
    """
    if not (1 <= month <= 12):
        raise ValueError("Month must be between 1 and 12")
    
    month_names = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    
    month_name = month_names[month - 1]
    return f"{month:02d}_{month_name}"


def get_year_month_path(year: int, month: int) -> str:
    """
    Generate full year/month path for Google Drive folder structure
    Format: YYYY/MM_MonthName (e.g., "2024/03_March")
    
    Args:
        year (int): Year
        month (int): Month (1-12)
        
    Returns:
        str: Full year/month path
    """
    month_folder = get_month_folder(year, month)
    return f"{year}/{month_folder}"


def format_success_message(action: str, next_step: Optional[str] = None) -> str:
    """
    Generate consistent success messages with emojis
    
    Args:
        action (str): The action that was completed
        next_step (str, optional): Suggestion for next step
        
    Returns:
        str: Formatted success message
    """
    success_emojis = {
        'registration': '🎉',
        'kpi_submission': '✅',
        'target_setting': '🎯',
        'photo_upload': '📸',
        'data_save': '💾',
        'update': '🔄',
        'delete': '🗑️',
        'default': '✅'
    }
    
    emoji = success_emojis.get(action.lower(), success_emojis['default'])
    
    message = f"{emoji} **Success!** {action.replace('_', ' ').title()} completed successfully."
    
    if next_step:
        message += f"\n\n💡 **Next:** {next_step}"
    
    return message


def format_error_message(error_type: str, details: Optional[str] = None) -> str:
    """
    Generate consistent error messages with emojis
    Uses centralized error handler if available, otherwise falls back to local implementation
    
    Args:
        error_type (str): Type of error
        details (str, optional): Additional error details
        
    Returns:
        str: Formatted error message
    """
    if ERROR_HANDLER_AVAILABLE:
        return error_format_message(error_type, details)
    
    # Fallback implementation
    error_emojis = {
        'validation': '⚠️',
        'permission': '🚫',
        'network': '🌐',
        'upload': '📤',
        'not_found': '🔍',
        'timeout': '⏰',
        'rate_limit': '⏳',
        'default': '❌'
    }
    
    emoji = error_emojis.get(error_type.lower(), error_emojis['default'])
    
    message = f"{emoji} **Error:** {error_type.replace('_', ' ').title()}"
    
    if details:
        message += f"\n{details}"
    
    message += "\n\n🔄 Please try again or contact support if the problem persists."
    
    return message


def format_info_message(title: str, content: str, emoji: str = "ℹ️") -> str:
    """
    Generate consistent info messages with emojis
    
    Args:
        title (str): Message title
        content (str): Message content
        emoji (str): Emoji to use (default: info emoji)
        
    Returns:
        str: Formatted info message
    """
    return f"{emoji} **{title}**\n\n{content}"


def format_currency(amount: float, currency: str = "$") -> str:
    """
    Format currency amount with proper formatting
    
    Args:
        amount (float): Amount to format
        currency (str): Currency symbol
        
    Returns:
        str: Formatted currency string
    """
    return f"{currency}{amount:,.2f}"


def truncate_text(text: str, max_length: int = 50, suffix: str = "...") -> str:
    """
    Truncate text to specified length with suffix
    
    Args:
        text (str): Text to truncate
        max_length (int): Maximum length
        suffix (str): Suffix to add if truncated
        
    Returns:
        str: Truncated text
    """
    if len(text) <= max_length:
        return text
    
    return text[:max_length - len(suffix)] + suffix


def validate_callback_data(callback_data: str, expected_prefix: str) -> bool:
    """
    Validate callback data format
    
    Args:
        callback_data (str): Callback data to validate
        expected_prefix (str): Expected prefix
        
    Returns:
        bool: True if valid format
    """
    if not callback_data or not isinstance(callback_data, str):
        return False
    
    return callback_data.startswith(expected_prefix)


def extract_user_id_from_callback(callback_data: str) -> Optional[int]:
    """
    Extract user ID from callback data
    Expected format: "action_user_123456789"
    
    Args:
        callback_data (str): Callback data string
        
    Returns:
        Optional[int]: User ID if found, None otherwise
    """
    try:
        parts = callback_data.split('_')
        if len(parts) >= 3 and parts[-2] == 'user':
            return int(parts[-1])
    except (ValueError, IndexError):
        logger.warning(f"Failed to extract user ID from callback data: {callback_data}")
    
    return None


def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename for safe storage
    
    Args:
        filename (str): Original filename
        
    Returns:
        str: Sanitized filename
    """
    # Remove or replace invalid characters
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    
    # Limit length
    if len(filename) > 100:
        name, ext = filename.rsplit('.', 1) if '.' in filename else (filename, '')
        filename = name[:95] + ('.' + ext if ext else '')
    
    return filename


def generate_photo_filename(user_id: int, record_type: str, timestamp: Optional[datetime] = None) -> str:
    """
    Generate standardized photo filename
    
    Args:
        user_id (int): User ID
        record_type (str): Type of record ('meetup' or 'sale')
        timestamp (datetime, optional): Timestamp for filename
        
    Returns:
        str: Generated filename
    """
    if timestamp is None:
        timestamp = datetime.now()
    
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
    return f"{record_type}_{user_id}_{timestamp_str}.jpg"


def format_datetime_display(dt: datetime, include_time: bool = True) -> str:
    """
    Format datetime for display in messages
    
    Args:
        dt (datetime): Datetime to format
        include_time (bool): Whether to include time
        
    Returns:
        str: Formatted datetime string
    """
    if include_time:
        return dt.strftime("%B %d, %Y at %I:%M %p")
    else:
        return dt.strftime("%B %d, %Y")


def get_greeting_emoji() -> str:
    """
    Get appropriate greeting emoji based on time of day
    
    Returns:
        str: Greeting emoji
    """
    hour = datetime.now().hour
    
    if 5 <= hour < 12:
        return "🌅"  # Morning
    elif 12 <= hour < 17:
        return "☀️"  # Afternoon
    elif 17 <= hour < 21:
        return "🌆"  # Evening
    else:
        return "🌙"  # Night