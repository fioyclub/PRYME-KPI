# Google Drive integration module
# This module handles photo uploads and file management

"""
Google Drive Integration Module

This module manages all Google Drive operations including:
- Photo uploads with memory-efficient handling
- Organized folder structure creation
- Public access link generation
- Service Account authentication
- Temporary file cleanup
"""

import os
import logging
from typing import Optional, Dict, Any
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
    
    error_handler = MockErrorHandler()

# Google Drive API configuration
SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = 'service_account.json'  # Path to service account JSON file
TOKEN_FILE = 'token.json'  # Path to OAuth token file
CREDENTIALS_FILE = 'credentials.json'  # Path to OAuth credentials file

class GoogleDriveService:
    """Google Drive service class for handling authentication and basic operations"""
    
    def __init__(self):
        self.service = None
        self.credentials = None
        
    @retry_google_api(max_retries=2)
    def authenticate_google_drive(self) -> bool:
        """
        Initialize Google Drive service with OAuth or Service Account authentication
        
        Returns:
            bool: True if authentication successful, False otherwise
        """
        with ErrorContext("google_drive_authentication") as ctx:
            # Try OAuth authentication first (for user authorization)
            if self._try_oauth_authentication():
                logger.info("Google Drive OAuth authentication successful")
                log_system_event("google_drive_authenticated", "OAuth service initialized successfully")
                return True
            
            # Fallback to Service Account authentication
            if self._try_service_account_authentication():
                logger.info("Google Drive Service Account authentication successful")
                log_system_event("google_drive_authenticated", "Service Account initialized successfully")
                return True
            
            # Try environment variable authentication (for cloud deployment)
            if self._try_environment_authentication():
                logger.info("Google Drive environment authentication successful")
                log_system_event("google_drive_authenticated", "Environment service initialized successfully")
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
                self.service = build('drive', 'v3', credentials=self.credentials)
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
            self.service = build('drive', 'v3', credentials=self.credentials)
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
                        self.service = build('drive', 'v3', credentials=self.credentials)
                        logger.info("Loaded Google Drive credentials from OAuth environment variable")
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
                    self.service = build('drive', 'v3', credentials=self.credentials)
                    logger.info("Loaded Google Drive credentials from Service Account environment variable")
                    return True
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON in GOOGLE_SERVICE_ACCOUNT_JSON: {e}")
            
            return False
            
        except Exception as e:
            logger.warning(f"Environment authentication failed: {e}")
            return False
    
    def test_connection(self) -> bool:
        """
        Test Google Drive API connection by listing files in root directory
        
        Returns:
            bool: True if connection test successful, False otherwise
        """
        try:
            if not self.service:
                logger.error("Google Drive service not initialized. Call authenticate_google_drive() first.")
                return False
            
            # Test connection by listing files (limit to 1 for efficiency)
            results = self.service.files().list(
                pageSize=1,
                fields="nextPageToken, files(id, name)"
            ).execute()
            
            files = results.get('files', [])
            logger.info(f"Connection test successful. Drive accessible with {len(files)} file(s) found in test query.")
            
            # Test folder creation capability
            test_folder_metadata = {
                'name': 'KPI_Bot_Connection_Test',
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            test_folder = self.service.files().create(
                body=test_folder_metadata,
                fields='id, name'
            ).execute()
            
            logger.info(f"Connection test successful. Created test folder: {test_folder.get('name')} (ID: {test_folder.get('id')})")
            
            # Clean up test folder
            self.service.files().delete(fileId=test_folder.get('id')).execute()
            logger.info("Test folder cleaned up successfully")
            
            return True
            
        except HttpError as e:
            if e.resp.status == 403:
                logger.error("Permission denied. Check service account permissions for Google Drive.")
            elif e.resp.status == 429:
                logger.error("Rate limit exceeded during connection test.")
            else:
                logger.error(f"HTTP error during connection test: {e}")
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def handle_drive_error(self, error: Exception, operation: str, user_id: Optional[int] = None) -> str:
        """
        Centralized error handling for Google Drive operations
        
        Args:
            error (Exception): The exception that occurred
            operation (str): Description of the operation that failed
            user_id (int, optional): User ID for context
            
        Returns:
            str: User-friendly error message
        """
        return error_handler.handle_google_api_error(error, operation, user_id)

# Global service instance
drive_service = GoogleDriveService()

def authenticate_google_drive() -> bool:
    """
    Initialize Google Drive service with Service Account authentication
    
    Returns:
        bool: True if authentication successful, False otherwise
    """
    return drive_service.authenticate_google_drive()

def test_drive_connection() -> bool:
    """
    Test Google Drive API connection
    
    Returns:
        bool: True if connection test successful, False otherwise
    """
    return drive_service.test_connection()

# Folder structure management
import calendar
from datetime import datetime
from typing import Dict, Optional

# Folder ID cache to avoid repeated API calls
_folder_cache: Dict[str, str] = {}

def get_current_month_folder_name() -> str:
    """
    Generate current month folder name in MM_MonthName format
    
    Returns:
        str: Folder name like "01_January" or "12_December"
    """
    now = datetime.now()
    month_num = now.month
    month_name = calendar.month_name[month_num]
    return f"{month_num:02d}_{month_name}"

def create_folder(name: str, parent_id: Optional[str] = None) -> Optional[str]:
    """
    Create a folder in Google Drive
    
    Args:
        name (str): Name of the folder to create
        parent_id (Optional[str]): Parent folder ID, None for root
        
    Returns:
        Optional[str]: Created folder ID if successful, None otherwise
    """
    try:
        if not drive_service.service:
            logger.error("Google Drive service not initialized")
            return None
        
        folder_metadata = {
            'name': name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        
        if parent_id:
            folder_metadata['parents'] = [parent_id]
        
        folder = drive_service.service.files().create(
            body=folder_metadata,
            fields='id, name'
        ).execute()
        
        folder_id = folder.get('id')
        logger.info(f"Created folder '{name}' with ID: {folder_id}")
        return folder_id
        
    except Exception as e:
        logger.error(f"Failed to create folder '{name}': {e}")
        return None

def find_folder_by_name(name: str, parent_id: Optional[str] = None) -> Optional[str]:
    """
    Find a folder by name in the specified parent directory
    
    Args:
        name (str): Name of the folder to find
        parent_id (Optional[str]): Parent folder ID, None for root
        
    Returns:
        Optional[str]: Folder ID if found, None otherwise
    """
    try:
        if not drive_service.service:
            logger.error("Google Drive service not initialized")
            return None
        
        # Build query
        query = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent_id:
            query += f" and '{parent_id}' in parents"
        
        results = drive_service.service.files().list(
            q=query,
            fields="files(id, name)"
        ).execute()
        
        files = results.get('files', [])
        if files:
            folder_id = files[0]['id']
            logger.info(f"Found folder '{name}' with ID: {folder_id}")
            return folder_id
        
        logger.info(f"Folder '{name}' not found")
        return None
        
    except Exception as e:
        logger.error(f"Failed to find folder '{name}': {e}")
        return None

def get_or_create_folder(name: str, parent_id: Optional[str] = None) -> Optional[str]:
    """
    Get existing folder or create it if it doesn't exist
    
    Args:
        name (str): Name of the folder
        parent_id (Optional[str]): Parent folder ID, None for root
        
    Returns:
        Optional[str]: Folder ID if successful, None otherwise
    """
    # Check cache first
    cache_key = f"{parent_id or 'root'}:{name}"
    if cache_key in _folder_cache:
        logger.info(f"Using cached folder ID for '{name}': {_folder_cache[cache_key]}")
        return _folder_cache[cache_key]
    
    # Try to find existing folder
    folder_id = find_folder_by_name(name, parent_id)
    
    # Create if not found
    if not folder_id:
        folder_id = create_folder(name, parent_id)
    
    # Cache the result
    if folder_id:
        _folder_cache[cache_key] = folder_id
        logger.info(f"Cached folder ID for '{name}': {folder_id}")
    
    return folder_id

def create_monthly_folders(year: int = None, month: int = None) -> Dict[str, Optional[str]]:
    """
    Ensure folder structure exists for specified month (or current month)
    Creates: KPI_Bot_Photos/YYYY/MM_MonthName/meetups and sales folders
    
    Args:
        year (int, optional): Year for folder structure. Defaults to current year.
        month (int, optional): Month for folder structure. Defaults to current month.
        
    Returns:
        Dict[str, Optional[str]]: Dictionary with folder IDs for 'meetups' and 'sales'
    """
    try:
        if not drive_service.service:
            logger.error("Google Drive service not initialized")
            return {'meetups': None, 'sales': None}
        
        # Use current date if not specified
        if year is None or month is None:
            now = datetime.now()
            year = year or now.year
            month = month or now.month
        
        # Generate folder names
        year_folder = str(year)
        month_name = calendar.month_name[month]
        month_folder = f"{month:02d}_{month_name}"
        
        logger.info(f"Creating folder structure for {month_folder} {year}")
        
        # Use the configured root folder from environment variable
        import os
        root_folder_id = os.getenv('GOOGLE_DRIVE_FOLDER_ID')
        if not root_folder_id:
            logger.error("GOOGLE_DRIVE_FOLDER_ID environment variable not set")
            return {'meetups': None, 'sales': None}
        
        # Create year folder: KPI_Bot_Photos/YYYY
        year_folder_id = get_or_create_folder(year_folder, root_folder_id)
        if not year_folder_id:
            logger.error(f"Failed to create/find year folder: {year_folder}")
            return {'meetups': None, 'sales': None}
        
        # Create month folder: KPI_Bot_Photos/YYYY/MM_MonthName
        month_folder_id = get_or_create_folder(month_folder, year_folder_id)
        if not month_folder_id:
            logger.error(f"Failed to create/find month folder: {month_folder}")
            return {'meetups': None, 'sales': None}
        
        # Create meetups and sales subfolders
        meetups_folder_id = get_or_create_folder("meetups", month_folder_id)
        sales_folder_id = get_or_create_folder("sales", month_folder_id)
        
        result = {
            'meetups': meetups_folder_id,
            'sales': sales_folder_id
        }
        
        logger.info(f"Folder structure created successfully for {month_folder} {year}")
        logger.info(f"Meetups folder ID: {meetups_folder_id}")
        logger.info(f"Sales folder ID: {sales_folder_id}")
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to create monthly folders: {e}")
        return {'meetups': None, 'sales': None}

def clear_folder_cache():
    """Clear the folder ID cache"""
    global _folder_cache
    _folder_cache.clear()
    logger.info("Folder cache cleared")

def get_folder_cache_info() -> Dict[str, str]:
    """Get current folder cache contents for debugging"""
    return _folder_cache.copy()

# Photo upload functionality with memory management
import gc
import tempfile
from io import BytesIO
from googleapiclient.http import MediaIoBaseUpload

@retry_google_api(max_retries=3)
def upload_photo(file_data: bytes, filename: str, folder_type: str, year: int = None, month: int = None, user_id: Optional[int] = None) -> Optional[str]:
    """
    Upload photo to Google Drive with memory-efficient handling
    
    Args:
        file_data (bytes): Photo data as bytes
        filename (str): Name for the uploaded file
        folder_type (str): Either 'meetups' or 'sales'
        year (int, optional): Year for folder structure. Defaults to current year.
        month (int, optional): Month for folder structure. Defaults to current month.
        user_id (int, optional): User ID for error context
        
    Returns:
        Optional[str]: Public link to uploaded photo if successful, None otherwise
    """
    file_stream = None
    
    with ErrorContext("photo_upload", user_id, "file_processing_error") as ctx:
        if not drive_service.service:
            error_msg = "Google Drive service not initialized"
            logger.error(error_msg)
            log_system_event("upload_failed", error_msg, "ERROR")
            return None
        
        if folder_type not in ['meetups', 'sales']:
            error_msg = f"Invalid folder_type: {folder_type}. Must be 'meetups' or 'sales'"
            logger.error(error_msg)
            log_system_event("upload_failed", error_msg, "ERROR")
            return None
        
        logger.info(f"Starting photo upload: {filename} to {folder_type} folder")
        log_system_event("photo_upload_started", f"User {user_id} uploading {filename} to {folder_type}")
        
        # Create monthly folder structure
        folders = create_monthly_folders(year, month)
        target_folder_id = folders.get(folder_type)
        
        if not target_folder_id:
            error_msg = f"Failed to create/find {folder_type} folder"
            logger.error(error_msg)
            log_system_event("upload_failed", error_msg, "ERROR")
            return None
        
        # Create BytesIO stream from file data
        file_stream = BytesIO(file_data)
        
        # Prepare file metadata
        file_metadata = {
            'name': filename,
            'parents': [target_folder_id]
        }
        
        # Create media upload object
        media = MediaIoBaseUpload(
            file_stream,
            mimetype='image/jpeg',  # Assuming JPEG images
            resumable=True
        )
        
        # Upload the file
        logger.info(f"Uploading {filename} to Google Drive...")
        file_result = drive_service.service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, webViewLink'
        ).execute()
        
        file_id = file_result.get('id')
        if not file_id:
            error_msg = "Failed to get file ID from upload response"
            logger.error(error_msg)
            log_system_event("upload_failed", error_msg, "ERROR")
            return None
        
        # Generate public link
        public_link = generate_public_link(file_id)
        
        logger.info(f"Photo uploaded successfully: {filename} (ID: {file_id})")
        log_system_event("photo_upload_success", f"User {user_id} uploaded {filename} successfully")
        
        # Immediate memory cleanup after successful upload
        release_file_memory(file_data, file_stream)
        
        return public_link

def generate_public_link(file_id: str) -> Optional[str]:
    """
    Generate public access link for uploaded file
    
    Args:
        file_id (str): Google Drive file ID
        
    Returns:
        Optional[str]: Public link if successful, None otherwise
    """
    try:
        if not drive_service.service:
            logger.error("Google Drive service not initialized")
            return None
        
        # Make file publicly readable
        permission = {
            'type': 'anyone',
            'role': 'reader'
        }
        
        drive_service.service.permissions().create(
            fileId=file_id,
            body=permission
        ).execute()
        
        # Get the public link
        file_info = drive_service.service.files().get(
            fileId=file_id,
            fields='webViewLink, webContentLink'
        ).execute()
        
        # Return direct download link (webContentLink) if available, otherwise view link
        public_link = file_info.get('webContentLink') or file_info.get('webViewLink')
        
        logger.info(f"Generated public link for file {file_id}: {public_link}")
        return public_link
        
    except Exception as e:
        logger.error(f"Failed to generate public link for file {file_id}: {e}")
        return None

# Import memory management functions
from memory_management import release_file_memory as memory_release_file_memory

def release_file_memory(file_data, file_stream=None):
    """
    Safely release file-related memory after successful upload
    This function now delegates to the centralized memory management module
    
    Args:
        file_data: File data to be released
        file_stream: Optional file stream to be closed and released
    """
    try:
        # Use centralized memory management
        return memory_release_file_memory(file_data, file_stream)
        
    except Exception as e:
        logger.error(f"[MEMORY] Error during memory cleanup: {e}")
        return False

def cleanup_temp_files():
    """
    Remove temporary files and release memory
    This function now delegates to the centralized memory management module
    """
    try:
        # Import here to avoid circular imports
        from memory_management import cleanup_temp_files as memory_cleanup_temp_files
        
        return memory_cleanup_temp_files()
        
    except Exception as e:
        logger.error(f"[MEMORY] Error during temp file cleanup: {e}")
        return 0

async def upload_photo_from_telegram(telegram_file, filename: str, folder_type: str, year: int = None, month: int = None) -> Optional[str]:
    """
    Upload photo from Telegram file object with memory-efficient handling
    
    Args:
        telegram_file: Telegram file object from bot
        filename (str): Name for the uploaded file
        folder_type (str): Either 'meetups' or 'sales'
        year (int, optional): Year for folder structure. Defaults to current year.
        month (int, optional): Month for folder structure. Defaults to current month.
        
    Returns:
        Optional[str]: Public link to uploaded photo if successful, None otherwise
    """
    try:
        logger.info(f"Processing Telegram file for upload: {filename}")
        
        # Download file data from Telegram
        file_data = await telegram_file.download_as_bytearray()
        
        # Convert to bytes and upload
        file_bytes = bytes(file_data)
        
        # Upload the photo
        result = upload_photo(file_bytes, filename, folder_type, year, month)
        
        # Clean up the downloaded data immediately
        del file_data
        del file_bytes
        gc.collect()
        logger.info("[MEMORY] Cleaned up Telegram file data after processing")
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to process Telegram file {filename}: {e}")
        return None
