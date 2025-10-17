"""
Production lines management routes with full database integration
"""

from flask import Blueprint, render_template, redirect, url_for, session, jsonify, request, flash
from auth import require_login, require_admin
from routes.main import validate_session
from database import lines_db, facilities_db, audit_db
from utils import get_client_info

    
admin_lines_bp = Blueprint('admin_lines', __name__)

@admin_lines_bp.route('/lines')
@validate_session
def production_lines():
    if not require_login(session):
        return redirect(url_for('main.login'))
    
    if not require_admin(session):
        flash('Admin privileges required', 'error')
        return redirect(url_for('main.dashboard'))
    
    # Get data from database
    lines = lines_db.get_all(active_only=False)
    facilities = facilities_db.get_all(active_only=True)
    
    # Add safe defaults for template
    for line in lines:
        if 'line_code' not in line:
            line['line_code'] = None
        if 'created_date' not in line:
            line['created_date'] = None
        if 'created_by' not in line:
            line['created_by'] = None
        if 'modified_date' not in line:
            line['modified_date'] = None
        if 'modified_by' not in line:
            line['modified_by'] = None
    
    return render_template('admin/production_lines.html', 
                         lines=lines, 
                         facilities=facilities,
                         user=session['user'])

@admin_lines_bp.route('/lines/add', methods=['POST'])
@validate_session
def add_line():
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        facility_id = request.form.get('facility_id')
        line_name = request.form.get('line_name', '').strip()
        line_code = request.form.get('line_code', '').strip()
        
        if not facility_id or not line_name:
            return jsonify({'success': False, 'message': 'Facility and line name are required'})
        
        # Create line
        success, message, line_id = lines_db.create(
            facility_id, line_name, line_code, session['user']['username']
        )
        
        if success and line_id:
            # Log in audit
            ip, user_agent = get_client_info()
            audit_db.log(
                table_name='ProductionLines',
                record_id=line_id,
                action_type='INSERT',
                username=session['user']['username'],
                ip=ip,
                user_agent=user_agent,
                notes=f"Created production line: {line_name}"
            )
        
        return jsonify({'success': success, 'message': message})
        
    except Exception as e:
        print(f"Error adding line: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred'})

@admin_lines_bp.route('/lines/edit/<int:line_id>', methods=['POST'])
@validate_session
def edit_line(line_id):
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        line_name = request.form.get('line_name', '').strip()
        line_code = request.form.get('line_code', '').strip()
        
        if not line_name:
            return jsonify({'success': False, 'message': 'Line name is required'})
        
        # Update line
        success, message, changes = lines_db.update(
            line_id, line_name, line_code, session['user']['username']
        )
        
        if success and changes:
            # Log in audit
            ip, user_agent = get_client_info()
            audit_db.log(
                table_name='ProductionLines',
                record_id=line_id,
                action_type='UPDATE',
                changes=changes,
                username=session['user']['username'],
                ip=ip,
                user_agent=user_agent
            )
        
        return jsonify({'success': success, 'message': message})
        
    except Exception as e:
        print(f"Error editing line: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred'})

@admin_lines_bp.route('/lines/delete/<int:line_id>', methods=['POST'])
@validate_session
def delete_line(line_id):
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        # Deactivate line
        success, message = lines_db.deactivate(
            line_id, session['user']['username']
        )
        
        if success:
            # Log in audit
            ip, user_agent = get_client_info()
            audit_db.log(
                table_name='ProductionLines',
                record_id=line_id,
                action_type='DEACTIVATE',
                changes={'is_active': {'old': '1', 'new': '0'}},
                username=session['user']['username'],
                ip=ip,
                user_agent=user_agent
            )
        
        return jsonify({'success': success, 'message': message})
        
    except Exception as e:
        print(f"Error deleting line: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred'})

@admin_lines_bp.route('/lines/history/<int:line_id>')
@validate_session
def line_history(line_id):
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        history = audit_db.get_record_history('ProductionLines', line_id)
        line = lines_db.get_by_id(line_id)
        
        return jsonify({
            'success': True,
            'line': line,
            'history': history
        })
    except Exception as e:
        print(f"Error getting line history: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred'})
