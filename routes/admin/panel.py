"""
Admin panel main page
"""
    
from flask import Blueprint, render_template, redirect, url_for, session, flash
from auth import require_login, require_admin
from routes.main import validate_session

admin_panel_bp = Blueprint('admin_panel', __name__)

@admin_panel_bp.route('/')
@validate_session
def panel():
    if not require_login(session):
        return redirect(url_for('main.login'))
    
    if not require_admin(session):
        flash('Admin privileges required', 'error')
        return redirect(url_for('main.dashboard'))
    
    return render_template('admin/panel.html', user=session['user'])
