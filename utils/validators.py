"""
Input validation functions
Validate user input and data integrity
"""

import re
from datetime import datetime

def validate_facility_name(name):
    """
    Validate facility name
    
    Args:
        name: facility name to validate
    
    Returns:
        tuple: (is_valid, error_message)
    """
    if not name or not name.strip():
        return False, "Facility name is required"
    
    name = name.strip()
    
    if len(name) < 2:
        return False, "Facility name must be at least 2 characters"
    
    if len(name) > 100:
        return False, "Facility name must be less than 100 characters"
    
    # Check for valid characters (alphanumeric, spaces, hyphens, underscores)
    if not re.match(r'^[\w\s\-\.]+$', name):
        return False, "Facility name contains invalid characters"
    
    return True, None

def validate_line_name(name):
    """
    Validate production line name
    
    Args:
        name: line name to validate
    
    Returns:
        tuple: (is_valid, error_message)
    """
    if not name or not name.strip():
        return False, "Line name is required"
    
    name = name.strip()
    
    if len(name) < 2:
        return False, "Line name must be at least 2 characters"
    
    if len(name) > 100:
        return False, "Line name must be less than 100 characters"
    
    # Check for valid characters
    if not re.match(r'^[\w\s\-\.]+$', name):
        return False, "Line name contains invalid characters"
    
    return True, None

def validate_line_code(code):
    """
    Validate production line code
    
    Args:
        code: line code to validate (optional)
    
    Returns:
        tuple: (is_valid, error_message)
    """
    if not code:  # Code is optional
        return True, None
    
    code = code.strip()
    
    if len(code) > 20:
        return False, "Line code must be less than 20 characters"
    
    # Check for valid characters (alphanumeric, hyphens, underscores)
    if not re.match(r'^[\w\-]+$', code):
        return False, "Line code can only contain letters, numbers, hyphens, and underscores"
    
    return True, None

def validate_datetime_range(start_time, end_time):
    """
    Validate datetime range
    
    Args:
        start_time: start datetime (string or datetime)
        end_time: end datetime (string or datetime)
    
    Returns:
        tuple: (is_valid, error_message)
    """
    try:
        if isinstance(start_time, str):
            start_time = datetime.fromisoformat(start_time)
        if isinstance(end_time, str):
            end_time = datetime.fromisoformat(end_time)
        
        if not start_time or not end_time:
            return False, "Both start and end times are required"
        
        if end_time <= start_time:
            return False, "End time must be after start time"
        
        # Check if duration is reasonable (max 24 hours)
        duration_hours = (end_time - start_time).total_seconds() / 3600
        if duration_hours > 24:
            return False, "Downtime duration cannot exceed 24 hours"
        
        # Check if not in future
        if start_time > datetime.now():
            return False, "Cannot record downtime in the future"
        
        return True, None
        
    except (ValueError, TypeError) as e:
        return False, f"Invalid datetime format: {str(e)}"

def validate_category_code(code):
    """
    Validate category code
    
    Args:
        code: category code to validate
    
    Returns:
        tuple: (is_valid, error_message)
    """
    if not code or not code.strip():
        return False, "Category code is required"
    
    code = code.strip().upper()
    
    if len(code) < 2 or len(code) > 4:
        return False, "Category code must be 2-4 characters"
    
    # Main category: 2 uppercase letters
    # Subcategory: 2 letters + 2 numbers (optional)
    if not re.match(r'^[A-Z]{2}(\d{2})?$', code):
        return False, "Category code must be 2 letters (XX) or 2 letters + 2 numbers (XX01)"
    
    return True, None

def validate_email(email):
    """
    Validate email address
    
    Args:
        email: email address to validate
    
    Returns:
        tuple: (is_valid, error_message)
    """
    if not email:
        return False, "Email is required"
    
    email = email.strip().lower()
    
    # Basic email regex pattern
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    if not re.match(pattern, email):
        return False, "Invalid email format"
    
    return True, None