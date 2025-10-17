# dangquyenbui-dotcom/downtime_tracker/downtime_tracker-5bb4163f1c166071f5c302dee6ed03e0344576eb/database/__init__.py
"""
Database package initialization
Provides centralized access to all database modules
"""

from .connection import DatabaseConnection, get_db
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
from .sales_service import sales_service # <-- ADD THIS IMPORT

# Create singleton instances
facilities_db = FacilitiesDB()
lines_db = ProductionLinesDB()
categories_db = CategoriesDB()
downtimes_db = DowntimesDB()
audit_db = AuditDB()
shifts_db = ShiftsDB()
users_db = UsersDB()
sessions_db = SessionsDB()
capacity_db = ProductionCapacityDB() 

# Export main database functions
__all__ = [
    'DatabaseConnection',
    'get_db',
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
    'sales_service' # <-- ADD THIS
]