# dangquyenbui-dotcom/downtime_tracker/downtime_tracker-5bb4163f1c166071f5c302dee6ed03e0344576eb/routes/__init__.py
"""
Routes package initialization
All route blueprints are imported and registered in app.py
"""

from .main import main_bp
from .downtime import downtime_bp
from .erp_routes import erp_bp
from .scheduling import scheduling_bp
from .reports import reports_bp
from .bom import bom_bp
from .po import po_bp
from .mrp import mrp_bp
from .sales import sales_bp # <-- ADD THIS IMPORT
from . import admin

__all__ = [
    'main_bp',
    'downtime_bp',
    'erp_bp',
    'scheduling_bp',
    'reports_bp',
    'admin',
    'bom_bp',
    'po_bp',
    'mrp_bp',
    'sales_bp' # <-- ADD THIS
]