# database/__init__.py
"""
Database package initialization
Provides centralized access to all database modules
"""

from .connection import DatabaseConnection, get_db
# Remove ERP connection import from here
# from .erp_connection_base import get_erp_db_connection # Keep if needed elsewhere, but service is preferred
from .erp_service import get_erp_service # Import the main service getter

from .facilities import FacilitiesDB
from .production_lines import ProductionLinesDB
from .categories import CategoriesDB
from .downtimes import DowntimesDB
from .audit import AuditDB
from .shifts import ShiftsDB
from .users import UsersDB
from .sessions import SessionsDB
from .reports import reports_db
from .scheduling import scheduling_db
from .capacity import ProductionCapacityDB
from .mrp_service import mrp_service
from .sales_service import sales_service

# Create singleton instances for local DB operations
facilities_db = FacilitiesDB()
lines_db = ProductionLinesDB()
categories_db = CategoriesDB()
downtimes_db = DowntimesDB()
audit_db = AuditDB()
shifts_db = ShiftsDB()
users_db = UsersDB()
sessions_db = SessionsDB()
capacity_db = ProductionCapacityDB()

# Export main database functions and service getters
__all__ = [
    'DatabaseConnection',
    'get_db',
    'get_erp_service', # Export the service getter
    'facilities_db',
    'lines_db',
    'categories_db',
    'downtimes_db',
    'audit_db',
    'shifts_db',
    'users_db',
    'sessions_db',
    'reports_db',
    'scheduling_db',
    'capacity_db',
    'mrp_service',
    'sales_service'
]