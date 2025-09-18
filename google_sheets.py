# Google Sheets integration module
# This module handles all Google Sheets operations for data persistence

"""
Google Sheets Integration Module

This module manages all Google Sheets operations including:
- User registration data storage
- KPI target management
- KPI record tracking
- Progress calculation and retrieval
- Service Account authentication
"""

import os
import logging
from typing import Optional, Dict, Any, List, Union
from datetime import datetime
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json

# Configure logging
logger = logging.getLogger(__name__)

# Import error handling utilities (avoid circular imports)
try:
    from error_handler import error_handler, retry_google_api, ErrorContext, log_system_event
    ERROR_HANDLER_AVAILABLE = True
except ImportError:
    ERROR_HANDLER_AVAILABLE = False
    # Fallback implementations
    def retry_google_api(max_retries=3):
        def decorator(func):
            return func
        return decorator
    
    class ErrorContext:
        def __init__(self, operation, user_id=None, error_type='system'):
            pass
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            return False
    
    def log_system_event(event, details=None, level='INFO'):
        pass
    
    class MockErrorHandler:
        def handle_google_api_error(self, error, operation, user_id=None):
            return f"❌ {operation} failed. Please try again."
        def get_error_statistics(self):
            return {}
    
    error_handler = MockErrorHandler()

# Google Sheets API configuration
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = 'service_account.json'  # Path to service account JSON file
TOKEN_FILE = 'token.json'  # Path to OAuth token file
CREDENTIALS_FILE = 'credentials.json'  # Path to OAuth credentials file

class GoogleSheetsService:
    """Google Sheets service class for handling authentication and basic operations"""
    
    def __init__(self):
        self.service = None
        self.credentials = None
        
    @retry_google_api(max_retries=2)
    def authenticate_google_sheets(self) -> bool:
        """
        Initialize Google Sheets service with OAuth or Service Account authentication
        
        Returns:
            bool: True if authentication successful, False otherwise
        """
        with ErrorContext("google_sheets_authentication") as ctx:
            # Try OAuth authentication first (for user authorization)
            if self._try_oauth_authentication():
                logger.info("Google Sheets OAuth authentication successful")
                log_system_event("google_sheets_authenticated", "OAuth service initialized successfully")
                return True
            
            # Fallback to Service Account authentication
            if self._try_service_account_authentication():
                logger.info("Google Sheets Service Account authentication successful")
                log_system_event("google_sheets_authenticated", "Service Account initialized successfully")
                return True
            
            # Try environment variable authentication (for cloud deployment)
            if self._try_environment_authentication():
                logger.info("Google Sheets environment authentication successful")
                log_system_event("google_sheets_authenticated", "Environment service initialized successfully")
                return True
            
            error_msg = "All authentication methods failed"
            logger.error(error_msg)
            log_system_event("authentication_failed", error_msg, "ERROR")
            return False
    
    def _try_oauth_authentication(self) -> bool:
        """Try OAuth user authentication"""
        try:
            creds = None
            
            # Check if token.json exists (stored OAuth credentials)
            if os.path.exists(TOKEN_FILE):
                creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
            
            # If there are no (valid) credentials available, let the user log in
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    # Try to refresh the token
                    try:
                        creds.refresh(Request())
                        logger.info("OAuth token refreshed successfully")
                    except Exception as e:
                        logger.warning(f"Failed to refresh OAuth token: {e}")
                        creds = None
                
                if not creds:
                    # Check if we have credentials.json for OAuth flow
                    if not os.path.exists(CREDENTIALS_FILE):
                        logger.info("OAuth credentials file not found, skipping OAuth authentication")
                        return False
                    
                    # For cloud deployment, we can't do interactive OAuth
                    # So we'll skip this if no valid token exists
                    logger.info("No valid OAuth token found and interactive flow not available in cloud environment")
                    return False
            
            # Save the credentials for the next run
            if creds and creds.valid:
                with open(TOKEN_FILE, 'w') as token:
                    token.write(creds.to_json())
                
                self.credentials = creds
                self.service = build('sheets', 'v4', credentials=self.credentials)
                return True
            
            return False
            
        except Exception as e:
            logger.warning(f"OAuth authentication failed: {e}")
            return False
    
    def _try_service_account_authentication(self) -> bool:
        """Try Service Account authentication"""
        try:
            if not os.path.exists(SERVICE_ACCOUNT_FILE):
                logger.info("Service account file not found, skipping Service Account authentication")
                return False
            
            # Load service account credentials
            self.credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, 
                scopes=SCOPES
            )
            
            # Build the service
            self.service = build('sheets', 'v4', credentials=self.credentials)
            return True
            
        except Exception as e:
            logger.warning(f"Service Account authentication failed: {e}")
            return False
    
    def _try_environment_authentication(self) -> bool:
        """Try authentication from environment variables"""
        try:
            # Try OAuth token from environment
            oauth_token = os.getenv('GOOGLE_OAUTH_TOKEN_JSON')
            if oauth_token:
                try:
                    token_info = json.loads(oauth_token)
                    self.credentials = Credentials.from_authorized_user_info(token_info, SCOPES)
                    
                    # Refresh if needed
                    if self.credentials.expired and self.credentials.refresh_token:
                        self.credentials.refresh(Request())
                    
                    if self.credentials.valid:
                        self.service = build('sheets', 'v4', credentials=self.credentials)
                        logger.info("Loaded Google Sheets credentials from OAuth environment variable")
                        return True
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON in GOOGLE_OAUTH_TOKEN_JSON: {e}")
            
            # Try Service Account from environment
            service_account_json = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
            if service_account_json:
                try:
                    service_account_info = json.loads(service_account_json)
                    self.credentials = service_account.Credentials.from_service_account_info(
                        service_account_info, 
                        scopes=SCOPES
                    )
                    self.service = build('sheets', 'v4', credentials=self.credentials)
                    logger.info("Loaded Google Sheets credentials from Service Account environment variable")
                    return True
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON in GOOGLE_SERVICE_ACCOUNT_JSON: {e}")
            
            return False
            
        except Exception as e:
            logger.warning(f"Environment authentication failed: {e}")
            return False
    
    @retry_google_api(max_retries=2)
    def test_connection(self, spreadsheet_id: Optional[str] = None) -> bool:
        """
        Test Google Sheets API connection
        
        Args:
            spreadsheet_id (str, optional): Spreadsheet ID to test with. 
                                          If None, creates a test spreadsheet.
        
        Returns:
            bool: True if connection test successful, False otherwise
        """
        with ErrorContext("google_sheets_connection_test") as ctx:
            if not self.service:
                error_msg = "Google Sheets service not initialized. Call authenticate_google_sheets() first."
                logger.error(error_msg)
                log_system_event("connection_test_failed", error_msg, "ERROR")
                return False
            
            if spreadsheet_id:
                # Test with existing spreadsheet
                result = self.service.spreadsheets().get(
                    spreadsheetId=spreadsheet_id
                ).execute()
                title = result.get('properties', {}).get('title', 'Unknown')
                logger.info(f"Connection test successful with spreadsheet: {title}")
                log_system_event("connection_test_success", f"Tested with spreadsheet: {title}")
            else:
                # Create a test spreadsheet to verify write permissions
                spreadsheet_body = {
                    'properties': {
                        'title': 'KPI Bot Connection Test'
                    },
                    'sheets': [{
                        'properties': {
                            'title': 'Test Sheet'
                        }
                    }]
                }
                
                result = self.service.spreadsheets().create(
                    body=spreadsheet_body
                ).execute()
                
                test_spreadsheet_id = result['spreadsheetId']
                logger.info(f"Connection test successful. Created test spreadsheet: {test_spreadsheet_id}")
                log_system_event("connection_test_success", f"Created test spreadsheet: {test_spreadsheet_id}")
                
            return True
    
    def handle_sheets_error(self, error: Exception, operation: str, user_id: Optional[int] = None) -> str:
        """
        Centralized error handling for Google Sheets operations
        
        Args:
            error (Exception): The exception that occurred
            operation (str): Description of the operation that failed
            user_id (int, optional): User ID for context
            
        Returns:
            str: User-friendly error message
        """
        return error_handler.handle_google_api_error(error, operation, user_id)

# Global service instance
sheets_service = GoogleSheetsService()

def authenticate_google_sheets() -> bool:
    """
    Initialize Google Sheets service with Service Account authentication
    
    Returns:
        bool: True if authentication successful, False otherwise
    """
    return sheets_service.authenticate_google_sheets()

def test_sheets_connection(spreadsheet_id: Optional[str] = None) -> bool:
    """
    Test Google Sheets API connection
    
    Args:
        spreadsheet_id (str, optional): Spreadsheet ID to test with
        
    Returns:
        bool: True if connection test successful, False otherwise
    """
    return sheets_service.test_connection(spreadsheet_id)

# Spreadsheet configuration
SPREADSHEET_ID = os.getenv('GOOGLE_SHEETS_ID', '')  # Set via environment variable

# Sheet names
USERS_SHEET = 'Users'
TARGETS_SHEET = 'Targets'
RECORDS_SHEET = 'KPI_Records'
ADMIN_SHEET = 'Admin_Config'

@retry_google_api(max_retries=3)
def register_user(user_data: Dict[str, Any]) -> bool:
    """
    Store new user registration data in Google Sheets
    
    Args:
        user_data (dict): User data dictionary containing user_id, name, nationality, phone, upline, registration_date, role
        
    Returns:
        bool: True if registration successful, False otherwise
    """
    user_id = user_data.get('user_id')
    
    with ErrorContext("user_registration", user_id, "database_error") as ctx:
        if not sheets_service.service:
            error_msg = "Google Sheets service not initialized"
            logger.error(error_msg)
            log_system_event("registration_failed", error_msg, "ERROR")
            return False
        
        # Check for duplicate registration first
        if get_user_by_id(user_data['user_id']):
            logger.warning(f"User {user_data['user_id']} already registered")
            log_system_event("duplicate_registration_attempt", f"User {user_id} attempted duplicate registration", "WARNING")
            return False
        
        # Prepare data for insertion
        values = [[
            user_data['user_id'],
            user_data['name'],
            user_data['nationality'],
            user_data['phone'],
            user_data['upline'],
            user_data['registration_date'],
            user_data['role']
        ]]
        
        # Check if Users sheet exists, create if not
        _ensure_sheet_exists(USERS_SHEET, [
            'User ID', 'Name', 'Nationality', 'Phone', 'Upline', 'Registration Date', 'Role'
        ])
        
        # Insert user data
        range_name = f"'{USERS_SHEET}'!A:G"
        body = {
            'values': values
        }
        
        result = sheets_service.service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        
        logger.info(f"User {user_data['user_id']} registered successfully")
        log_system_event("user_registered", f"User {user_id} ({user_data['name']}) registered successfully")
        return True

def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Retrieve user data by Telegram ID
    
    Args:
        user_id (int): Telegram user ID
        
    Returns:
        dict or None: User data dictionary if found, None otherwise
    """
    try:
        if not sheets_service.service:
            logger.error("Google Sheets service not initialized")
            return None
        
        # Ensure Users sheet exists
        _ensure_sheet_exists(USERS_SHEET, [
            'User ID', 'Name', 'Nationality', 'Phone', 'Upline', 'Registration Date', 'Role'
        ])
        
        # Read all user data
        range_name = f"'{USERS_SHEET}'!A:G"
        result = sheets_service.service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        if not values:
            return None
        
        # Skip header row and search for user
        for row in values[1:]:
            if len(row) >= 7 and str(row[0]) == str(user_id):
                return {
                    'user_id': int(row[0]),
                    'name': row[1],
                    'nationality': row[2],
                    'phone': row[3],
                    'upline': row[4],
                    'registration_date': row[5],
                    'role': row[6]
                }
        
        return None
        
    except HttpError as e:
        logger.error(f"HTTP error during user retrieval: {e}")
        return None
    except Exception as e:
        logger.error(f"Error during user retrieval: {e}")
        return None

def get_all_users() -> list:
    """
    Retrieve all registered users
    
    Returns:
        list: List of user data dictionaries
    """
    try:
        if not sheets_service.service:
            logger.error("Google Sheets service not initialized")
            return []
        
        # Ensure Users sheet exists
        _ensure_sheet_exists(USERS_SHEET, [
            'User ID', 'Name', 'Nationality', 'Phone', 'Upline', 'Registration Date', 'Role'
        ])
        
        # Read all user data
        range_name = f"'{USERS_SHEET}'!A:G"
        result = sheets_service.service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        if not values or len(values) <= 1:  # No data or only header
            return []
        
        users = []
        # Skip header row
        for row in values[1:]:
            if len(row) >= 7:
                users.append({
                    'user_id': int(row[0]),
                    'name': row[1],
                    'nationality': row[2],
                    'phone': row[3],
                    'upline': row[4],
                    'registration_date': row[5],
                    'role': row[6]
                })
        
        return users
        
    except HttpError as e:
        logger.error(f"HTTP error during users retrieval: {e}")
        return []
    except Exception as e:
        logger.error(f"Error during users retrieval: {e}")
        return []

def _ensure_sheet_exists(sheet_name: str, headers: list) -> bool:
    """
    Ensure a sheet exists with proper headers
    
    Args:
        sheet_name (str): Name of the sheet
        headers (list): List of header column names
        
    Returns:
        bool: True if sheet exists or was created successfully
    """
    try:
        # Get spreadsheet metadata
        spreadsheet = sheets_service.service.spreadsheets().get(
            spreadsheetId=SPREADSHEET_ID
        ).execute()
        
        # Check if sheet exists
        sheet_exists = False
        for sheet in spreadsheet.get('sheets', []):
            if sheet['properties']['title'] == sheet_name:
                sheet_exists = True
                break
        
        if not sheet_exists:
            # Create the sheet
            requests = [{
                'addSheet': {
                    'properties': {
                        'title': sheet_name
                    }
                }
            }]
            
            body = {
                'requests': requests
            }
            
            sheets_service.service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body=body
            ).execute()
            
            logger.info(f"Created sheet: {sheet_name}")
        
        # Check if headers exist
        range_name = f"'{sheet_name}'!1:1"
        result = sheets_service.service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        if not values or values[0] != headers:
            # Add or update headers
            body = {
                'values': [headers]
            }
            
            sheets_service.service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
            
            logger.info(f"Added headers to sheet: {sheet_name}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error ensuring sheet exists: {e}")
        return False

def set_monthly_targets(user_id: int, month: int, year: int, meetup_target: int, sales_target: float) -> bool:
    """
    Set monthly KPI targets for users (overwrite existing)
    
    Args:
        user_id (int): Telegram user ID
        month (int): Target month (1-12)
        year (int): Target year
        meetup_target (int): Target number of meetups
        sales_target (float): Target sales amount
        
    Returns:
        bool: True if targets set successfully, False otherwise
    """
    try:
        if not sheets_service.service:
            logger.error("Google Sheets service not initialized")
            return False
        
        # Check if targets already exist for this user/month/year
        existing_target = get_monthly_targets(user_id, month, year)
        
        # Ensure Targets sheet exists
        _ensure_sheet_exists(TARGETS_SHEET, [
            'User ID', 'Month', 'Year', 'Meetup Target', 'Sales Target', 'Created Date'
        ])
        
        current_date = datetime.now().isoformat()
        
        if existing_target:
            # Update existing target
            # Find the row to update
            range_name = f"'{TARGETS_SHEET}'!A:F"
            result = sheets_service.service.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            if not values:
                return False
            
            # Find the row index to update
            row_index = None
            for i, row in enumerate(values[1:], start=2):  # Start from row 2 (skip header)
                if (len(row) >= 6 and 
                    str(row[0]) == str(user_id) and 
                    str(row[1]) == str(month) and 
                    str(row[2]) == str(year)):
                    row_index = i
                    break
            
            if row_index:
                # Update the specific row
                update_range = f"'{TARGETS_SHEET}'!A{row_index}:F{row_index}"
                update_values = [[user_id, month, year, meetup_target, sales_target, current_date]]
                
                body = {
                    'values': update_values
                }
                
                sheets_service.service.spreadsheets().values().update(
                    spreadsheetId=SPREADSHEET_ID,
                    range=update_range,
                    valueInputOption='RAW',
                    body=body
                ).execute()
                
                logger.info(f"Updated targets for user {user_id} for {month}/{year}")
        else:
            # Insert new target
            values = [[user_id, month, year, meetup_target, sales_target, current_date]]
            
            range_name = f"'{TARGETS_SHEET}'!A:F"
            body = {
                'values': values
            }
            
            sheets_service.service.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
            
            logger.info(f"Set new targets for user {user_id} for {month}/{year}")
        
        return True
        
    except HttpError as e:
        logger.error(f"HTTP error during target setting: {e}")
        return False
    except Exception as e:
        logger.error(f"Error during target setting: {e}")
        return False

def get_monthly_targets(user_id: int, month: int, year: int) -> Optional[Dict[str, Any]]:
    """
    Retrieve KPI targets for a specific user and month
    
    Args:
        user_id (int): Telegram user ID
        month (int): Target month (1-12)
        year (int): Target year
        
    Returns:
        dict or None: Target data dictionary if found, None otherwise
    """
    try:
        if not sheets_service.service:
            logger.error("Google Sheets service not initialized")
            return None
        
        # Ensure Targets sheet exists
        _ensure_sheet_exists(TARGETS_SHEET, [
            'User ID', 'Month', 'Year', 'Meetup Target', 'Sales Target', 'Created Date'
        ])
        
        # Read all target data
        range_name = f"'{TARGETS_SHEET}'!A:F"
        result = sheets_service.service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        if not values:
            return None
        
        # Skip header row and search for target
        for row in values[1:]:
            if (len(row) >= 6 and 
                str(row[0]) == str(user_id) and 
                str(row[1]) == str(month) and 
                str(row[2]) == str(year)):
                return {
                    'user_id': int(row[0]),
                    'month': int(row[1]),
                    'year': int(row[2]),
                    'meetup_target': int(row[3]),
                    'sales_target': float(row[4]),
                    'created_date': row[5]
                }
        
        return None
        
    except HttpError as e:
        logger.error(f"HTTP error during target retrieval: {e}")
        return None
    except Exception as e:
        logger.error(f"Error during target retrieval: {e}")
        return None

def get_user_targets(user_id: int) -> List[Dict[str, Any]]:
    """
    Retrieve all KPI targets for a specific user
    
    Args:
        user_id (int): Telegram user ID
        
    Returns:
        list: List of target data dictionaries
    """
    try:
        if not sheets_service.service:
            logger.error("Google Sheets service not initialized")
            return []
        
        # Ensure Targets sheet exists
        _ensure_sheet_exists(TARGETS_SHEET, [
            'User ID', 'Month', 'Year', 'Meetup Target', 'Sales Target', 'Created Date'
        ])
        
        # Read all target data
        range_name = f"'{TARGETS_SHEET}'!A:F"
        result = sheets_service.service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        if not values or len(values) <= 1:  # No data or only header
            return []
        
        targets = []
        # Skip header row
        for row in values[1:]:
            if len(row) >= 6 and str(row[0]) == str(user_id):
                targets.append({
                    'user_id': int(row[0]),
                    'month': int(row[1]),
                    'year': int(row[2]),
                    'meetup_target': int(row[3]),
                    'sales_target': float(row[4]),
                    'created_date': row[5]
                })
        
        return targets
        
    except HttpError as e:
        logger.error(f"HTTP error during user targets retrieval: {e}")
        return []
    except Exception as e:
        logger.error(f"Error during user targets retrieval: {e}")
        return []

def record_kpi_submission(user_id: int, record_type: str, value: Union[int, float], photo_link: str, record_date: Optional[datetime] = None) -> bool:
    """
    Store KPI submissions (meetup/sales) with photo links
    
    Args:
        user_id (int): Telegram user ID
        record_type (str): Type of record ('meetup' or 'sale')
        value (Union[int, float]): Client count for meetups or sales amount for sales
        photo_link (str): Google Drive link to uploaded photo
        record_date (datetime, optional): Date of the KPI activity. Defaults to current datetime.
        
    Returns:
        bool: True if record stored successfully, False otherwise
    """
    try:
        if not sheets_service.service:
            logger.error("Google Sheets service not initialized")
            return False
        
        # Validate record_type
        if record_type not in ['meetup', 'sale']:
            logger.error(f"Invalid record_type: {record_type}")
            return False
        
        # Use current datetime if not provided
        if record_date is None:
            record_date = datetime.now()
        
        # Ensure KPI Records sheet exists
        _ensure_sheet_exists(RECORDS_SHEET, [
            'User ID', 'Record Date', 'Record Type', 'Value', 'Photo Link', 'Submission Date'
        ])
        
        submission_date = datetime.now().isoformat()
        record_date_str = record_date.isoformat()
        
        # Prepare data for insertion
        values = [[user_id, record_date_str, record_type, value, photo_link, submission_date]]
        
        range_name = f"'{RECORDS_SHEET}'!A:F"
        body = {
            'values': values
        }
        
        sheets_service.service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        
        logger.info(f"Recorded {record_type} KPI for user {user_id}: {value}")
        return True
        
    except HttpError as e:
        logger.error(f"HTTP error during KPI record submission: {e}")
        return False
    except Exception as e:
        logger.error(f"Error during KPI record submission: {e}")
        return False

def get_user_kpi_records(user_id: int, month: Optional[int] = None, year: Optional[int] = None, record_type: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Retrieve KPI records for a specific user with optional filtering
    
    Args:
        user_id (int): Telegram user ID
        month (int, optional): Filter by month (1-12)
        year (int, optional): Filter by year
        record_type (str, optional): Filter by record type ('meetup' or 'sale')
        
    Returns:
        list: List of KPI record dictionaries
    """
    try:
        if not sheets_service.service:
            logger.error("Google Sheets service not initialized")
            return []
        
        # Ensure KPI Records sheet exists
        _ensure_sheet_exists(RECORDS_SHEET, [
            'User ID', 'Record Date', 'Record Type', 'Value', 'Photo Link', 'Submission Date'
        ])
        
        # Read all KPI records
        range_name = f"'{RECORDS_SHEET}'!A:F"
        result = sheets_service.service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        if not values or len(values) <= 1:  # No data or only header
            return []
        
        records = []
        # Skip header row
        for row in values[1:]:
            if len(row) >= 6 and str(row[0]) == str(user_id):
                try:
                    # Parse record date
                    record_date = datetime.fromisoformat(row[1])
                    
                    # Apply filters
                    if month is not None and record_date.month != month:
                        continue
                    if year is not None and record_date.year != year:
                        continue
                    if record_type is not None and row[2] != record_type:
                        continue
                    
                    # Convert value to appropriate type
                    if row[2] == 'meetup':
                        value = int(row[3])
                    else:  # sale
                        value = float(row[3])
                    
                    records.append({
                        'user_id': int(row[0]),
                        'record_date': row[1],
                        'record_type': row[2],
                        'value': value,
                        'photo_link': row[4],
                        'submission_date': row[5]
                    })
                except (ValueError, IndexError) as e:
                    logger.warning(f"Skipping invalid record row: {row}, error: {e}")
                    continue
        
        return records
        
    except HttpError as e:
        logger.error(f"HTTP error during KPI records retrieval: {e}")
        return []
    except Exception as e:
        logger.error(f"Error during KPI records retrieval: {e}")
        return []

def calculate_user_progress(user_id: int, month: int, year: int) -> Optional[Dict[str, Any]]:
    """
    Calculate user progress against targets for a specific month
    
    Args:
        user_id (int): Telegram user ID
        month (int): Month for progress calculation (1-12)
        year (int): Year for progress calculation
        
    Returns:
        dict or None: Progress data dictionary if targets exist, None otherwise
    """
    try:
        # Get monthly targets
        targets = get_monthly_targets(user_id, month, year)
        if not targets:
            logger.info(f"No targets found for user {user_id} for {month}/{year}")
            return None
        
        # Get KPI records for the month
        meetup_records = get_user_kpi_records(user_id, month, year, 'meetup')
        sales_records = get_user_kpi_records(user_id, month, year, 'sale')
        
        # Calculate current values
        current_meetups = sum(record['value'] for record in meetup_records)
        current_sales = sum(record['value'] for record in sales_records)
        
        # Calculate percentages
        meetup_percentage = 0.0
        if targets['meetup_target'] > 0:
            meetup_percentage = min((current_meetups / targets['meetup_target']) * 100, 100.0)
        
        sales_percentage = 0.0
        if targets['sales_target'] > 0:
            sales_percentage = min((current_sales / targets['sales_target']) * 100, 100.0)
        
        progress = {
            'user_id': user_id,
            'month': month,
            'year': year,
            'current_meetups': current_meetups,
            'meetup_target': targets['meetup_target'],
            'meetup_percentage': round(meetup_percentage, 2),
            'current_sales': current_sales,
            'sales_target': targets['sales_target'],
            'sales_percentage': round(sales_percentage, 2),
            'meetup_records_count': len(meetup_records),
            'sales_records_count': len(sales_records)
        }
        
        return progress
        
    except Exception as e:
        logger.error(f"Error calculating user progress: {e}")
        return None

def get_monthly_progress_for_all_users(month: int, year: int) -> List[Dict[str, Any]]:
    """
    Get progress for all users for a specific month
    
    Args:
        month (int): Month for progress calculation (1-12)
        year (int): Year for progress calculation
        
    Returns:
        list: List of progress dictionaries for all users with targets
    """
    try:
        # Get all users
        all_users = get_all_users()
        if not all_users:
            return []
        
        progress_list = []
        for user in all_users:
            user_progress = calculate_user_progress(user['user_id'], month, year)
            if user_progress:
                # Add user info to progress
                user_progress['name'] = user['name']
                user_progress['role'] = user['role']
                progress_list.append(user_progress)
        
        return progress_list
        
    except Exception as e:
        logger.error(f"Error getting monthly progress for all users: {e}")
        return []
