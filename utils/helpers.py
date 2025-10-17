"""
Helper utility functions
Common functions used across the application
"""

from datetime import datetime
from flask import request

def get_client_info():
    """
    Get client IP address and user agent for audit logging
    
    Returns:
        tuple: (ip_address, user_agent)
    """
    ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    user_agent = request.environ.get('HTTP_USER_AGENT', '')[:500]  # Limit to 500 chars
    return ip, user_agent

def format_datetime(dt, format_string='%Y-%m-%d %H:%M:%S'):
    """
    Format datetime object to string
    
    Args:
        dt: datetime object or string
        format_string: desired output format
    
    Returns:
        str: formatted datetime string
    """
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    
    if dt:
        return dt.strftime(format_string)
    return ''

def calculate_duration(start_time, end_time):
    """
    Calculate duration in minutes between two times
    
    Args:
        start_time: start datetime (string or datetime)
        end_time: end datetime (string or datetime)
    
    Returns:
        int: duration in minutes
    """
    if isinstance(start_time, str):
        start_time = datetime.fromisoformat(start_time)
    if isinstance(end_time, str):
        end_time = datetime.fromisoformat(end_time)
    
    if start_time and end_time:
        duration = (end_time - start_time).total_seconds() / 60
        return int(duration)
    return 0

def format_duration(minutes):
    """
    Format duration from minutes to human-readable string
    
    Args:
        minutes: duration in minutes
    
    Returns:
        str: formatted duration (e.g., "2h 30m")
    """
    if not minutes:
        return "0m"
    
    hours = minutes // 60
    mins = minutes % 60
    
    if hours > 0:
        if mins > 0:
            return f"{hours}h {mins}m"
        return f"{hours}h"
    return f"{mins}m"

def safe_str(value, default=''):
    """
    Safely convert value to string
    
    Args:
        value: any value to convert
        default: default value if conversion fails
    
    Returns:
        str: string representation of value
    """
    if value is None:
        return default
    return str(value)

def safe_int(value, default=0):
    """
    Safely convert value to integer
    
    Args:
        value: any value to convert
        default: default value if conversion fails
    
    Returns:
        int: integer value
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return default