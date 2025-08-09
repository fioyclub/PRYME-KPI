"""
Authentication and Authorization Module

This module handles role-based access control for the Telegram KPI Bot:
- User role identification (admin vs sales)
- Role-based command access decorators
- Admin user configuration management
- Access denial handling for unauthorized users
"""

import logging
import os
from functools import wraps
from typing import Optional, List, Callable, Any
from telegram import Update
from telegram.ext import ContextTypes
import google_sheets

logger = logging.getLogger(__name__)

# Admin configuration
ADMIN_SHEET = 'Admin_Config'
DEFAULT_ADMIN_IDS = []  # Can be set via environment variable

class RoleManager:
    """Manages user roles and access control"""
    
    def __init__(self):
        self._admin_cache = set()
        self._cache_initialized = False
    
    def _initialize_admin_cache(self) -> bool:
        """
        Initialize admin cache from Google Sheets and environment variables
        
        Returns:
            bool: True if initialization successful, False otherwise
        """
        try:
            # Load admin IDs from environment variable
            env_admin_ids = os.getenv('ADMIN_USER_IDS', '')
            if env_admin_ids:
                for admin_id in env_admin_ids.split(','):
                    try:
                        self._admin_cache.add(int(admin_id.strip()))
                    except ValueError:
                        logger.warning(f"Invalid admin ID in environment: {admin_id}")
            
            # Load admin IDs from Google Sheets
            admin_ids = self._get_admin_ids_from_sheets()
            self._admin_cache.update(admin_ids)
            
            self._cache_initialized = True
            logger.info(f"Admin cache initialized with {len(self._admin_cache)} admin users")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize admin cache: {e}")
            return False
    
    def _get_admin_ids_from_sheets(self) -> List[int]:
        """
        Retrieve admin user IDs from Google Sheets
        
        Returns:
            List[int]: List of admin user IDs
        """
        try:
            if not google_sheets.sheets_service.service:
                logger.warning("Google Sheets service not initialized")
                return []
            
            # Ensure Admin_Config sheet exists
            google_sheets._ensure_sheet_exists(ADMIN_SHEET, ['User ID', 'Name', 'Added Date'])
            
            # Read admin data
            range_name = f'{ADMIN_SHEET}!A:C'
            result = google_sheets.sheets_service.service.spreadsheets().values().get(
                spreadsheetId=google_sheets.SPREADSHEET_ID,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            if not values or len(values) <= 1:  # No data or only header
                return []
            
            admin_ids = []
            # Skip header row
            for row in values[1:]:
                if len(row) >= 1:
                    try:
                        admin_ids.append(int(row[0]))
                    except ValueError:
                        logger.warning(f"Invalid admin ID in sheets: {row[0]}")
            
            return admin_ids
            
        except Exception as e:
            logger.error(f"Error retrieving admin IDs from sheets: {e}")
            return []
    
    def get_user_role(self, user_id: int) -> str:
        """
        Determine user role (admin or sales)
        
        Args:
            user_id (int): Telegram user ID
            
        Returns:
            str: User role ('admin' or 'sales')
        """
        if self.is_admin(user_id):
            return 'admin'
        return 'sales'
    
    def is_admin(self, user_id: int) -> bool:
        """
        Check if user has admin privileges
        
        Args:
            user_id (int): Telegram user ID
            
        Returns:
            bool: True if user is admin, False otherwise
        """
        # Initialize cache if not done yet
        if not self._cache_initialized:
            self._initialize_admin_cache()
        
        return user_id in self._admin_cache
    
    def add_admin(self, user_id: int, name: str = "") -> bool:
        """
        Add a new admin user
        
        Args:
            user_id (int): Telegram user ID
            name (str): Admin user name (optional)
            
        Returns:
            bool: True if admin added successfully, False otherwise
        """
        try:
            if not google_sheets.sheets_service.service:
                logger.error("Google Sheets service not initialized")
                return False
            
            # Check if already admin
            if self.is_admin(user_id):
                logger.info(f"User {user_id} is already an admin")
                return True
            
            # Ensure Admin_Config sheet exists
            google_sheets._ensure_sheet_exists(ADMIN_SHEET, ['User ID', 'Name', 'Added Date'])
            
            # Add to Google Sheets
            from datetime import datetime
            values = [[user_id, name, datetime.now().isoformat()]]
            
            range_name = f'{ADMIN_SHEET}!A:C'
            body = {'values': values}
            
            google_sheets.sheets_service.service.spreadsheets().values().append(
                spreadsheetId=google_sheets.SPREADSHEET_ID,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
            
            # Update cache
            self._admin_cache.add(user_id)
            
            logger.info(f"Added admin user: {user_id} ({name})")
            return True
            
        except Exception as e:
            logger.error(f"Error adding admin user: {e}")
            return False
    
    def remove_admin(self, user_id: int) -> bool:
        """
        Remove an admin user
        
        Args:
            user_id (int): Telegram user ID
            
        Returns:
            bool: True if admin removed successfully, False otherwise
        """
        try:
            if not google_sheets.sheets_service.service:
                logger.error("Google Sheets service not initialized")
                return False
            
            # Check if user is admin
            if not self.is_admin(user_id):
                logger.info(f"User {user_id} is not an admin")
                return True
            
            # Find and remove from Google Sheets
            range_name = f'{ADMIN_SHEET}!A:C'
            result = google_sheets.sheets_service.service.spreadsheets().values().get(
                spreadsheetId=google_sheets.SPREADSHEET_ID,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            if not values:
                return False
            
            # Find the row to delete
            row_index = None
            for i, row in enumerate(values[1:], start=2):  # Start from row 2 (skip header)
                if len(row) >= 1 and str(row[0]) == str(user_id):
                    row_index = i
                    break
            
            if row_index:
                # Delete the row (Google Sheets API doesn't have direct row deletion,
                # so we'll clear the row and let it be handled manually or implement
                # a more complex solution)
                clear_range = f'{ADMIN_SHEET}!A{row_index}:C{row_index}'
                google_sheets.sheets_service.service.spreadsheets().values().clear(
                    spreadsheetId=google_sheets.SPREADSHEET_ID,
                    range=clear_range
                ).execute()
                
                logger.info(f"Cleared admin user row: {user_id}")
            
            # Update cache
            self._admin_cache.discard(user_id)
            
            logger.info(f"Removed admin user: {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error removing admin user: {e}")
            return False
    
    def refresh_admin_cache(self) -> bool:
        """
        Refresh admin cache from Google Sheets
        
        Returns:
            bool: True if refresh successful, False otherwise
        """
        try:
            self._admin_cache.clear()
            self._cache_initialized = False
            return self._initialize_admin_cache()
        except Exception as e:
            logger.error(f"Error refreshing admin cache: {e}")
            return False
    
    def get_all_admins(self) -> List[dict]:
        """
        Get all admin users with their information
        
        Returns:
            List[dict]: List of admin user dictionaries
        """
        try:
            if not google_sheets.sheets_service.service:
                logger.error("Google Sheets service not initialized")
                return []
            
            # Read admin data
            range_name = f'{ADMIN_SHEET}!A:C'
            result = google_sheets.sheets_service.service.spreadsheets().values().get(
                spreadsheetId=google_sheets.SPREADSHEET_ID,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            if not values or len(values) <= 1:  # No data or only header
                return []
            
            admins = []
            # Skip header row
            for row in values[1:]:
                if len(row) >= 3:
                    try:
                        admins.append({
                            'user_id': int(row[0]),
                            'name': row[1],
                            'added_date': row[2]
                        })
                    except ValueError:
                        logger.warning(f"Invalid admin data in sheets: {row}")
            
            return admins
            
        except Exception as e:
            logger.error(f"Error retrieving all admins: {e}")
            return []

# Global role manager instance
role_manager = RoleManager()

def get_user_role(user_id: int) -> str:
    """
    Determine user role (admin or sales)
    
    Args:
        user_id (int): Telegram user ID
        
    Returns:
        str: User role ('admin' or 'sales')
    """
    return role_manager.get_user_role(user_id)

def is_admin(user_id: int) -> bool:
    """
    Check if user has admin privileges
    
    Args:
        user_id (int): Telegram user ID
        
    Returns:
        bool: True if user is admin, False otherwise
    """
    return role_manager.is_admin(user_id)

def require_role(required_role: str):
    """
    Decorator for role-based access control
    
    Args:
        required_role (str): Required role ('admin' or 'sales')
        
    Returns:
        Decorator function
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs) -> Any:
            if not update.effective_user:
                logger.warning("No effective user in update")
                return
            
            user_id = update.effective_user.id
            user_role = get_user_role(user_id)
            
            # Check role access
            if required_role == 'admin' and user_role != 'admin':
                await handle_access_denied(update, context, 'admin')
                return
            elif required_role == 'sales' and user_role not in ['admin', 'sales']:
                await handle_access_denied(update, context, 'sales')
                return
            
            # Role check passed, execute the function
            return await func(update, context, *args, **kwargs)
        
        return wrapper
    return decorator

def require_admin(func: Callable) -> Callable:
    """
    Decorator to require admin role
    
    Args:
        func: Function to decorate
        
    Returns:
        Decorated function
    """
    return require_role('admin')(func)

def require_sales(func: Callable) -> Callable:
    """
    Decorator to require sales role (or admin)
    
    Args:
        func: Function to decorate
        
    Returns:
        Decorated function
    """
    return require_role('sales')(func)

async def handle_access_denied(update: Update, context: ContextTypes.DEFAULT_TYPE, required_role: str) -> None:
    """
    Handle access denial for unauthorized users
    
    Args:
        update (Update): Telegram update object
        context (ContextTypes.DEFAULT_TYPE): Telegram context
        required_role (str): The role that was required
    """
    try:
        user_id = update.effective_user.id if update.effective_user else "Unknown"
        user_name = update.effective_user.first_name if update.effective_user else "Unknown"
        
        logger.warning(f"Access denied for user {user_id} ({user_name}) - required role: {required_role}")
        
        # Prepare access denied message
        if required_role == 'admin':
            message = (
                "🚫 **Access Denied**\n\n"
                "This command is only available to administrators.\n"
                "If you believe this is an error, please contact your system administrator."
            )
        else:
            message = (
                "🚫 **Access Denied**\n\n"
                "You don't have permission to use this command.\n"
                "Please register first using /register or contact your administrator."
            )
        
        # Send access denied message
        if update.message:
            await update.message.reply_text(message, parse_mode='Markdown')
        elif update.callback_query:
            await update.callback_query.answer("Access denied", show_alert=True)
            await update.callback_query.message.reply_text(message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error handling access denied: {e}")

def initialize_auth_system() -> bool:
    """
    Initialize the authentication system
    
    Returns:
        bool: True if initialization successful, False otherwise
    """
    try:
        # Initialize Google Sheets authentication
        if not google_sheets.authenticate_google_sheets():
            logger.error("Failed to authenticate Google Sheets")
            return False
        
        # Initialize admin cache
        if not role_manager._initialize_admin_cache():
            logger.warning("Failed to initialize admin cache, continuing with empty cache")
        
        logger.info("Authentication system initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to initialize authentication system: {e}")
        return False

# Utility functions for admin management
def add_admin(user_id: int, name: str = "") -> bool:
    """Add a new admin user"""
    return role_manager.add_admin(user_id, name)

def remove_admin(user_id: int) -> bool:
    """Remove an admin user"""
    return role_manager.remove_admin(user_id)

def refresh_admin_cache() -> bool:
    """Refresh admin cache from Google Sheets"""
    return role_manager.refresh_admin_cache()

def get_all_admins() -> List[dict]:
    """Get all admin users"""
    return role_manager.get_all_admins()