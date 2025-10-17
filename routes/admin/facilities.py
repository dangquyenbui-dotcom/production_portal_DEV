"""
Facilities management routes with full database integration
"""

from flask import Blueprint, render_template, redirect, url_for, session, jsonify, request, flash
from auth import require_login, require_admin
from routes.main import validate_session
from database import facilities_db, audit_db
from utils import get_client_info


admin_facilities_bp = Blueprint('admin_facilities', __name__)

@admin_facilities_bp.route('/facilities')
@validate_session
def facilities():
    if not require_login(session):
        return redirect(url_for('main.login'))
    
    if not require_admin(session):
        flash('Admin privileges required', 'error')
        return redirect(url_for('main.dashboard'))
    
    # Get facilities from database
    facilities = facilities_db.get_all(active_only=False)
    
    # Add safe defaults for template
    for facility in facilities:
        if 'created_date' not in facility:
            facility['created_date'] = None
        if 'created_by' not in facility:
            facility['created_by'] = None
        if 'modified_date' not in facility:
            facility['modified_date'] = None
        if 'modified_by' not in facility:
            facility['modified_by'] = None
    
    return render_template('admin/facilities.html', 
                         facilities=facilities, 
                         user=session['user'])

@admin_facilities_bp.route('/facilities/add', methods=['POST'])
@validate_session
def add_facility():
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        name = request.form.get('name', '').strip()
        location = request.form.get('location', '').strip()
        
        if not name:
            return jsonify({'success': False, 'message': 'Facility name is required'})
        
        # Create facility
        success, message, facility_id = facilities_db.create(
            name, location, session['user']['username']
        )
        
        if success and facility_id:
            # Log in audit
            ip, user_agent = get_client_info()
            audit_db.log(
                table_name='Facilities',
                record_id=facility_id,
                action_type='INSERT',
                username=session['user']['username'],
                ip=ip,
                user_agent=user_agent,
                notes=f"Created new facility: {name}"
            )
        
        return jsonify({'success': success, 'message': message})
        
    except Exception as e:
        print(f"Error adding facility: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred'})

@admin_facilities_bp.route('/facilities/edit/<int:facility_id>', methods=['POST'])
@validate_session
def edit_facility(facility_id):
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        name = request.form.get('name', '').strip()
        location = request.form.get('location', '').strip()
        
        if not name:
            return jsonify({'success': False, 'message': 'Facility name is required'})
        
        # Update facility
        success, message, changes = facilities_db.update(
            facility_id, name, location, session['user']['username']
        )
        
        if success and changes:
            # Log in audit
            ip, user_agent = get_client_info()
            audit_db.log(
                table_name='Facilities',
                record_id=facility_id,
                action_type='UPDATE',
                changes=changes,
                username=session['user']['username'],
                ip=ip,
                user_agent=user_agent
            )
        
        return jsonify({'success': success, 'message': message})
        
    except Exception as e:
        print(f"Error editing facility: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred'})

@admin_facilities_bp.route('/facilities/delete/<int:facility_id>', methods=['POST'])
@validate_session
def delete_facility(facility_id):
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        # Deactivate facility
        success, message = facilities_db.deactivate(
            facility_id, session['user']['username']
        )
        
        if success:
            # Log in audit
            ip, user_agent = get_client_info()
            audit_db.log(
                table_name='Facilities',
                record_id=facility_id,
                action_type='DEACTIVATE',
                changes={'is_active': {'old': '1', 'new': '0'}},
                username=session['user']['username'],
                ip=ip,
                user_agent=user_agent
            )
        
        return jsonify({'success': success, 'message': message})
        
    except Exception as e:
        print(f"Error deleting facility: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred'})

@admin_facilities_bp.route('/facilities/history/<int:facility_id>')
@validate_session
def facility_history(facility_id):
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        history = audit_db.get_record_history('Facilities', facility_id)
        facility = facilities_db.get_by_id(facility_id)
        
        return jsonify({
            'success': True,
            'facility': facility,
            'history': history
        })
    except Exception as e:
        print(f"Error getting facility history: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred'})
