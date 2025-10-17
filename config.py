"""
Configuration settings for Production Portal v1
Reads sensitive information from environment variables
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Application configuration"""
    
    # Flask settings
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-key-change-in-production')
    SESSION_HOURS = int(os.getenv('SESSION_HOURS', '8'))
    
    # Active Directory settings
    AD_SERVER = os.getenv('AD_SERVER')
    AD_DOMAIN = os.getenv('AD_DOMAIN')
    AD_PORT = int(os.getenv('AD_PORT', '389'))
    
    # Service account for AD queries
    AD_SERVICE_ACCOUNT = os.getenv('AD_SERVICE_ACCOUNT')
    AD_SERVICE_PASSWORD = os.getenv('AD_SERVICE_PASSWORD')
    
    # Security Groups
    AD_ADMIN_GROUP = os.getenv('AD_ADMIN_GROUP', 'DowntimeTracker_Admin')
    AD_USER_GROUP = os.getenv('AD_USER_GROUP', 'DowntimeTracker_User')
    AD_SCHEDULING_ADMIN_GROUP = os.getenv('AD_SCHEDULING_ADMIN_GROUP', 'Scheduling_Admin')
    AD_SCHEDULING_USER_GROUP = os.getenv('AD_SCHEDULING_USER_GROUP', 'Scheduling_User')
    
    # Base DN for searches
    AD_BASE_DN = os.getenv('AD_BASE_DN')
    
    # Test mode - set to True to bypass AD and use test accounts
    TEST_MODE = os.getenv('TEST_MODE', 'False').lower() == 'true'
    
    # --- Main Application Database (ProductionDB) ---
    DB_SERVER = os.getenv('DB_SERVER')
    DB_NAME = os.getenv('DB_NAME', 'ProductionDB')
    DB_USE_WINDOWS_AUTH = os.getenv('DB_USE_WINDOWS_AUTH', 'False').lower() == 'true'
    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    
    # --- ERP Database (Deacom) ---
    ERP_DB_SERVER = os.getenv('ERP_DB_SERVER')
    ERP_DB_NAME = os.getenv('ERP_DB_NAME')
    ERP_DB_USERNAME = os.getenv('ERP_DB_USERNAME')
    ERP_DB_PASSWORD = os.getenv('ERP_DB_PASSWORD')
    ERP_DB_PORT = os.getenv('ERP_DB_PORT', '1433')
    ERP_DB_DRIVER = os.getenv('ERP_DB_DRIVER', 'ODBC Driver 17 for SQL Server')
    ERP_DB_TIMEOUT = int(os.getenv('ERP_DB_TIMEOUT', '30'))

    # Email settings (Optional)
    SMTP_SERVER = os.getenv('SMTP_SERVER', 'mail.wepackitall.local')
    SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
    SMTP_USE_TLS = os.getenv('SMTP_USE_TLS', 'True').lower() == 'true'
    EMAIL_FROM = os.getenv('EMAIL_FROM', 'downtime@wepackitall.local')
    
    EMAIL_NOTIFICATIONS = {
        'Mechanical': ['Maintenance Team', 'Facility Manager'],
        'Electrical': ['Maintenance Team', 'Facility Manager'],
        'Material Shortage': ['Supply Chain', 'Production Manager'],
        'Quality Hold': ['Quality Team', 'Production Manager'],
        'No Operator': ['HR Team', 'Production Manager'],
        'Changeover': ['Production Manager'],
        'Break Time': [],
        'Cleaning': ['Production Manager'],
        'Planned Maintenance': ['Maintenance Team'],
        'Other': ['Production Manager', 'Facility Manager']
    }
    
    @classmethod
    def validate(cls):
        """Validate required configuration"""
        errors = []
        
        if not cls.TEST_MODE:
            if not cls.AD_SERVER: errors.append("AD_SERVER is required")
            if not cls.AD_DOMAIN: errors.append("AD_DOMAIN is required")
            if not cls.AD_BASE_DN: errors.append("AD_BASE_DN is required")
        
        if not cls.DB_SERVER: errors.append("DB_SERVER is required")
        
        if not cls.DB_USE_WINDOWS_AUTH:
            if not cls.DB_USERNAME: errors.append("DB_USERNAME is required when not using Windows Auth")
            if not cls.DB_PASSWORD: errors.append("DB_PASSWORD is required when not using Windows Auth")
        
        if errors:
            print("Configuration errors:")
            for error in errors:
                print(f"  - {error}")
            return False
        
        return True