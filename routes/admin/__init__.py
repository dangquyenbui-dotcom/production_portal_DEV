"""
Admin routes package
Contains all administrative route blueprints
"""

from .panel import admin_panel_bp
from .facilities import admin_facilities_bp
from .production_lines import admin_lines_bp
from .categories import admin_categories_bp
from .audit import admin_audit_bp
from .shifts import admin_shifts_bp
from .users import admin_users_bp

__all__ = [
    'admin_panel_bp',
    'admin_facilities_bp',
    'admin_lines_bp',
    'admin_categories_bp',
    'admin_audit_bp',
    'admin_shifts_bp',
    'admin_users_bp'
]