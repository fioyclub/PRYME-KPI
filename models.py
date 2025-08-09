"""
Data Models Module

This module contains all data model classes for the Telegram KPI Bot:
- User: User registration and profile information
- KPITarget: Monthly KPI targets for users
- KPIRecord: Individual KPI submissions (meetups/sales)
- UserProgress: Calculated progress against targets

Each model includes validation methods to ensure data integrity.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Union, Optional
import re
import logging

logger = logging.getLogger(__name__)


@dataclass
class User:
    """
    User model for storing registration and profile information
    
    Attributes:
        user_id (int): Telegram user ID (unique identifier)
        name (str): User's full name
        nationality (str): User's nationality
        phone (str): User's phone number
        upline (str): Name of user's upline/manager
        registration_date (datetime): When the user registered
        role (str): User role ('admin' or 'sales')
    """
    user_id: int
    name: str
    nationality: str
    phone: str
    upline: str
    registration_date: datetime = field(default_factory=datetime.now)
    role: str = "sales"
    
    def __post_init__(self):
        """Validate data after initialization"""
        self.validate()
    
    def validate(self) -> bool:
        """
        Validate user data
        
        Returns:
            bool: True if all validation passes
            
        Raises:
            ValueError: If validation fails
        """
        # Validate user_id
        if not isinstance(self.user_id, int) or self.user_id <= 0:
            raise ValueError("user_id must be a positive integer")
        
        # Validate name
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("name must be a non-empty string")
        if len(self.name.strip()) < 2:
            raise ValueError("name must be at least 2 characters long")
        if len(self.name.strip()) > 100:
            raise ValueError("name must be less than 100 characters")
        
        # Validate nationality
        if not isinstance(self.nationality, str) or not self.nationality.strip():
            raise ValueError("nationality must be a non-empty string")
        if len(self.nationality.strip()) < 2:
            raise ValueError("nationality must be at least 2 characters long")
        if len(self.nationality.strip()) > 50:
            raise ValueError("nationality must be less than 50 characters")
        
        # Validate phone
        if not isinstance(self.phone, str) or not self.phone.strip():
            raise ValueError("phone must be a non-empty string")
        # Basic phone validation - should contain only digits, spaces, +, -, (, )
        phone_pattern = r'^[\d\s\+\-\(\)]+$'
        if not re.match(phone_pattern, self.phone.strip()):
            raise ValueError("phone contains invalid characters")
        if len(self.phone.strip()) < 7:
            raise ValueError("phone must be at least 7 characters long")
        if len(self.phone.strip()) > 20:
            raise ValueError("phone must be less than 20 characters")
        
        # Validate upline
        if not isinstance(self.upline, str) or not self.upline.strip():
            raise ValueError("upline must be a non-empty string")
        if len(self.upline.strip()) < 2:
            raise ValueError("upline must be at least 2 characters long")
        if len(self.upline.strip()) > 100:
            raise ValueError("upline must be less than 100 characters")
        
        # Validate registration_date
        if not isinstance(self.registration_date, datetime):
            raise ValueError("registration_date must be a datetime object")
        if self.registration_date > datetime.now():
            raise ValueError("registration_date cannot be in the future")
        
        # Validate role
        if not isinstance(self.role, str) or self.role not in ['admin', 'sales']:
            raise ValueError("role must be either 'admin' or 'sales'")
        
        # Clean up string fields
        self.name = self.name.strip()
        self.nationality = self.nationality.strip()
        self.phone = self.phone.strip()
        self.upline = self.upline.strip()
        
        return True
    
    def to_dict(self) -> dict:
        """Convert user to dictionary for storage"""
        return {
            'user_id': self.user_id,
            'name': self.name,
            'nationality': self.nationality,
            'phone': self.phone,
            'upline': self.upline,
            'registration_date': self.registration_date.isoformat(),
            'role': self.role
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'User':
        """Create User instance from dictionary"""
        # Convert registration_date string back to datetime
        if isinstance(data.get('registration_date'), str):
            data['registration_date'] = datetime.fromisoformat(data['registration_date'])
        
        return cls(**data)


@dataclass
class KPITarget:
    """
    KPI Target model for storing monthly targets
    
    Attributes:
        user_id (int): Telegram user ID
        month (int): Target month (1-12)
        year (int): Target year
        meetup_target (int): Target number of meetups
        sales_target (float): Target sales amount
        created_date (datetime): When the target was set
    """
    user_id: int
    month: int
    year: int
    meetup_target: int
    sales_target: float
    created_date: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        """Validate data after initialization"""
        self.validate()
    
    def validate(self) -> bool:
        """
        Validate KPI target data
        
        Returns:
            bool: True if all validation passes
            
        Raises:
            ValueError: If validation fails
        """
        # Validate user_id
        if not isinstance(self.user_id, int) or self.user_id <= 0:
            raise ValueError("user_id must be a positive integer")
        
        # Validate month
        if not isinstance(self.month, int) or not (1 <= self.month <= 12):
            raise ValueError("month must be an integer between 1 and 12")
        
        # Validate year
        if not isinstance(self.year, int) or not (2020 <= self.year <= 2030):
            raise ValueError("year must be an integer between 2020 and 2030")
        
        # Validate meetup_target
        if not isinstance(self.meetup_target, int) or self.meetup_target < 0:
            raise ValueError("meetup_target must be a non-negative integer")
        if self.meetup_target > 1000:
            raise ValueError("meetup_target must be less than 1000")
        
        # Validate sales_target
        if not isinstance(self.sales_target, (int, float)) or self.sales_target < 0:
            raise ValueError("sales_target must be a non-negative number")
        if self.sales_target > 1000000:
            raise ValueError("sales_target must be less than 1,000,000")
        
        # Validate created_date
        if not isinstance(self.created_date, datetime):
            raise ValueError("created_date must be a datetime object")
        if self.created_date > datetime.now():
            raise ValueError("created_date cannot be in the future")
        
        return True
    
    def to_dict(self) -> dict:
        """Convert target to dictionary for storage"""
        return {
            'user_id': self.user_id,
            'month': self.month,
            'year': self.year,
            'meetup_target': self.meetup_target,
            'sales_target': self.sales_target,
            'created_date': self.created_date.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'KPITarget':
        """Create KPITarget instance from dictionary"""
        # Convert created_date string back to datetime
        if isinstance(data.get('created_date'), str):
            data['created_date'] = datetime.fromisoformat(data['created_date'])
        
        return cls(**data)


@dataclass
class KPIRecord:
    """
    KPI Record model for individual submissions
    
    Attributes:
        user_id (int): Telegram user ID
        record_date (datetime): Date of the KPI activity
        record_type (str): Type of record ('meetup' or 'sale')
        value (Union[int, float]): Client count for meetups or sales amount for sales
        photo_link (str): Google Drive link to uploaded photo
        submission_date (datetime): When the record was submitted
    """
    user_id: int
    record_date: datetime
    record_type: str
    value: Union[int, float]
    photo_link: str
    submission_date: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        """Validate data after initialization"""
        self.validate()
    
    def validate(self) -> bool:
        """
        Validate KPI record data
        
        Returns:
            bool: True if all validation passes
            
        Raises:
            ValueError: If validation fails
        """
        # Validate user_id
        if not isinstance(self.user_id, int) or self.user_id <= 0:
            raise ValueError("user_id must be a positive integer")
        
        # Validate record_date
        if not isinstance(self.record_date, datetime):
            raise ValueError("record_date must be a datetime object")
        if self.record_date > datetime.now():
            raise ValueError("record_date cannot be in the future")
        
        # Validate record_type
        if not isinstance(self.record_type, str) or self.record_type not in ['meetup', 'sale']:
            raise ValueError("record_type must be either 'meetup' or 'sale'")
        
        # Validate value based on record_type
        if self.record_type == 'meetup':
            if not isinstance(self.value, int) or self.value <= 0:
                raise ValueError("value for meetup must be a positive integer (client count)")
            if self.value > 100:
                raise ValueError("meetup client count must be less than 100")
        elif self.record_type == 'sale':
            if not isinstance(self.value, (int, float)) or self.value <= 0:
                raise ValueError("value for sale must be a positive number (sales amount)")
            if self.value > 100000:
                raise ValueError("sales amount must be less than 100,000")
        
        # Validate photo_link
        if not isinstance(self.photo_link, str) or not self.photo_link.strip():
            raise ValueError("photo_link must be a non-empty string")
        # Basic URL validation
        if not (self.photo_link.startswith('http://') or self.photo_link.startswith('https://')):
            raise ValueError("photo_link must be a valid URL starting with http:// or https://")
        if len(self.photo_link) > 500:
            raise ValueError("photo_link must be less than 500 characters")
        
        # Validate submission_date
        if not isinstance(self.submission_date, datetime):
            raise ValueError("submission_date must be a datetime object")
        if self.submission_date > datetime.now():
            raise ValueError("submission_date cannot be in the future")
        
        # Clean up string fields
        self.photo_link = self.photo_link.strip()
        
        return True
    
    def to_dict(self) -> dict:
        """Convert record to dictionary for storage"""
        return {
            'user_id': self.user_id,
            'record_date': self.record_date.isoformat(),
            'record_type': self.record_type,
            'value': self.value,
            'photo_link': self.photo_link,
            'submission_date': self.submission_date.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'KPIRecord':
        """Create KPIRecord instance from dictionary"""
        # Convert datetime strings back to datetime objects
        if isinstance(data.get('record_date'), str):
            data['record_date'] = datetime.fromisoformat(data['record_date'])
        if isinstance(data.get('submission_date'), str):
            data['submission_date'] = datetime.fromisoformat(data['submission_date'])
        
        return cls(**data)


@dataclass
class UserProgress:
    """
    User Progress model for calculated progress against targets
    
    Attributes:
        user_id (int): Telegram user ID
        current_meetups (int): Current meetup count for the month
        meetup_target (int): Target meetup count
        meetup_percentage (float): Meetup completion percentage
        current_sales (float): Current sales amount for the month
        sales_target (float): Target sales amount
        sales_percentage (float): Sales completion percentage
        month (int): Progress month (1-12)
        year (int): Progress year
    """
    user_id: int
    current_meetups: int
    meetup_target: int
    meetup_percentage: float
    current_sales: float
    sales_target: float
    sales_percentage: float
    month: int
    year: int
    
    def __post_init__(self):
        """Validate data after initialization"""
        self.validate()
    
    def validate(self) -> bool:
        """
        Validate user progress data
        
        Returns:
            bool: True if all validation passes
            
        Raises:
            ValueError: If validation fails
        """
        # Validate user_id
        if not isinstance(self.user_id, int) or self.user_id <= 0:
            raise ValueError("user_id must be a positive integer")
        
        # Validate current_meetups
        if not isinstance(self.current_meetups, int) or self.current_meetups < 0:
            raise ValueError("current_meetups must be a non-negative integer")
        
        # Validate meetup_target
        if not isinstance(self.meetup_target, int) or self.meetup_target < 0:
            raise ValueError("meetup_target must be a non-negative integer")
        
        # Validate meetup_percentage
        if not isinstance(self.meetup_percentage, (int, float)) or self.meetup_percentage < 0:
            raise ValueError("meetup_percentage must be a non-negative number")
        
        # Validate current_sales
        if not isinstance(self.current_sales, (int, float)) or self.current_sales < 0:
            raise ValueError("current_sales must be a non-negative number")
        
        # Validate sales_target
        if not isinstance(self.sales_target, (int, float)) or self.sales_target < 0:
            raise ValueError("sales_target must be a non-negative number")
        
        # Validate sales_percentage
        if not isinstance(self.sales_percentage, (int, float)) or self.sales_percentage < 0:
            raise ValueError("sales_percentage must be a non-negative number")
        
        # Validate month
        if not isinstance(self.month, int) or not (1 <= self.month <= 12):
            raise ValueError("month must be an integer between 1 and 12")
        
        # Validate year
        if not isinstance(self.year, int) or not (2020 <= self.year <= 2030):
            raise ValueError("year must be an integer between 2020 and 2030")
        
        return True
    
    def calculate_percentages(self) -> None:
        """Recalculate completion percentages based on current values and targets"""
        # Calculate meetup percentage
        if self.meetup_target > 0:
            self.meetup_percentage = min((self.current_meetups / self.meetup_target) * 100, 100.0)
        else:
            self.meetup_percentage = 0.0
        
        # Calculate sales percentage
        if self.sales_target > 0:
            self.sales_percentage = min((self.current_sales / self.sales_target) * 100, 100.0)
        else:
            self.sales_percentage = 0.0
    
    def is_meetup_target_achieved(self) -> bool:
        """Check if meetup target is achieved"""
        return self.current_meetups >= self.meetup_target
    
    def is_sales_target_achieved(self) -> bool:
        """Check if sales target is achieved"""
        return self.current_sales >= self.sales_target
    
    def is_all_targets_achieved(self) -> bool:
        """Check if both targets are achieved"""
        return self.is_meetup_target_achieved() and self.is_sales_target_achieved()
    
    def to_dict(self) -> dict:
        """Convert progress to dictionary for storage"""
        return {
            'user_id': self.user_id,
            'current_meetups': self.current_meetups,
            'meetup_target': self.meetup_target,
            'meetup_percentage': self.meetup_percentage,
            'current_sales': self.current_sales,
            'sales_target': self.sales_target,
            'sales_percentage': self.sales_percentage,
            'month': self.month,
            'year': self.year
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'UserProgress':
        """Create UserProgress instance from dictionary"""
        return cls(**data)
    
    @classmethod
    def create_from_targets_and_records(
        cls, 
        user_id: int, 
        month: int, 
        year: int,
        meetup_target: int = 0,
        sales_target: float = 0.0,
        meetup_records: list = None,
        sales_records: list = None
    ) -> 'UserProgress':
        """
        Create UserProgress instance by calculating from targets and records
        
        Args:
            user_id (int): User ID
            month (int): Month for progress calculation
            year (int): Year for progress calculation
            meetup_target (int): Target meetup count
            sales_target (float): Target sales amount
            meetup_records (list): List of meetup KPIRecord objects
            sales_records (list): List of sales KPIRecord objects
            
        Returns:
            UserProgress: Calculated progress instance
        """
        # Calculate current values from records
        current_meetups = sum(record.value for record in (meetup_records or []))
        current_sales = sum(record.value for record in (sales_records or []))
        
        # Create instance
        progress = cls(
            user_id=user_id,
            current_meetups=current_meetups,
            meetup_target=meetup_target,
            meetup_percentage=0.0,  # Will be calculated
            current_sales=current_sales,
            sales_target=sales_target,
            sales_percentage=0.0,  # Will be calculated
            month=month,
            year=year
        )
        
        # Calculate percentages
        progress.calculate_percentages()
        
        return progress