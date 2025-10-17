"""
Shifts management routes with full database integration
"""

from flask import Blueprint, render_template, redirect, url_for, session, jsonify, request, flash
from auth import require_login, require_admin
from routes.main import validate_session
from database import audit_db
from database.shifts import ShiftsDB
from utils import get_client_info


admin_shifts_bp = Blueprint('admin_shifts', __name__)

# Initialize shifts database
shifts_db = ShiftsDB()

@admin_shifts_bp.route('/shifts')
@validate_session
def shifts():
    """Display shifts management page"""
    if not require_login(session):
        return redirect(url_for('main.login'))
    
    if not require_admin(session):
        flash('Admin privileges required', 'error')
        return redirect(url_for('main.dashboard'))
    
    # Get shifts from database
    all_shifts = shifts_db.get_all(active_only=False)
    
    return render_template('admin/shifts.html', 
                         shifts=all_shifts, 
                         user=session['user'])

@admin_shifts_bp.route('/shifts/add', methods=['POST'])
@validate_session
def add_shift():
    """Add new shift"""
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        shift_name = request.form.get('shift_name', '').strip()
        shift_code = request.form.get('shift_code', '').strip()
        start_time = request.form.get('start_time', '').strip()
        end_time = request.form.get('end_time', '').strip()
        description = request.form.get('description', '').strip()
        
        if not shift_name or not start_time or not end_time:
            return jsonify({'success': False, 'message': 'Shift name, start time, and end time are required'})
        
        # Create shift
        success, message, shift_id = shifts_db.create(
            shift_name, shift_code, start_time, end_time,
            description, session['user']['username']
        )
        
        if success and shift_id:
            # Log in audit
            ip, user_agent = get_client_info()
            audit_db.log(
                table_name='Shifts',
                record_id=shift_id,
                action_type='INSERT',
                username=session['user']['username'],
                ip=ip,
                user_agent=user_agent,
                notes=f"Created shift: {shift_name} ({start_time} - {end_time})"
            )
        
        return jsonify({'success': success, 'message': message})
        
    except Exception as e:
        print(f"Error adding shift: {str(e)}")
        return jsonify({'success': False, 'message': f'An error occurred: {str(e)}'})

@admin_shifts_bp.route('/shifts/edit/<int:shift_id>', methods=['POST'])
@validate_session
def edit_shift(shift_id):
    """Edit existing shift"""
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        shift_name = request.form.get('shift_name', '').strip()
        shift_code = request.form.get('shift_code', '').strip()
        start_time = request.form.get('start_time', '').strip()
        end_time = request.form.get('end_time', '').strip()
        description = request.form.get('description', '').strip()
        
        if not shift_name or not start_time or not end_time:
            return jsonify({'success': False, 'message': 'Shift name, start time, and end time are required'})
        
        # Update shift
        success, message, changes = shifts_db.update(
            shift_id, shift_name, shift_code, start_time, end_time,
            description, session['user']['username']
        )
        
        if success and changes:
            # Log in audit
            ip, user_agent = get_client_info()
            audit_db.log(
                table_name='Shifts',
                record_id=shift_id,
                action_type='UPDATE',
                changes=changes,
                username=session['user']['username'],
                ip=ip,
                user_agent=user_agent
            )
        
        return jsonify({'success': success, 'message': message})
        
    except Exception as e:
        print(f"Error editing shift: {str(e)}")
        return jsonify({'success': False, 'message': f'An error occurred: {str(e)}'})

@admin_shifts_bp.route('/shifts/delete/<int:shift_id>', methods=['POST'])
@validate_session
def delete_shift(shift_id):
    """Deactivate shift"""
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        # Deactivate shift
        success, message = shifts_db.deactivate(
            shift_id, session['user']['username']
        )
        
        if success:
            # Log in audit
            ip, user_agent = get_client_info()
            audit_db.log(
                table_name='Shifts',
                record_id=shift_id,
                action_type='DEACTIVATE',
                changes={'is_active': {'old': '1', 'new': '0'}},
                username=session['user']['username'],
                ip=ip,
                user_agent=user_agent
            )
        
        return jsonify({'success': success, 'message': message})
        
    except Exception as e:
        print(f"Error deactivating shift: {str(e)}")
        return jsonify({'success': False, 'message': f'An error occurred: {str(e)}'})

@admin_shifts_bp.route('/shifts/reactivate/<int:shift_id>', methods=['POST'])
@validate_session
def reactivate_shift(shift_id):
    """Reactivate an inactive shift"""
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        # Reactivate shift
        success, message = shifts_db.reactivate(
            shift_id, session['user']['username']
        )
        
        if success:
            # Log in audit
            ip, user_agent = get_client_info()
            audit_db.log(
                table_name='Shifts',
                record_id=shift_id,
                action_type='REACTIVATE',
                changes={'is_active': {'old': '0', 'new': '1'}},
                username=session['user']['username'],
                ip=ip,
                user_agent=user_agent,
                notes='Shift reactivated'
            )
        
        return jsonify({'success': success, 'message': message})
        
    except Exception as e:
        print(f"Error reactivating shift: {str(e)}")
        return jsonify({'success': False, 'message': f'An error occurred: {str(e)}'})

@admin_shifts_bp.route('/shifts/history/<int:shift_id>')
@validate_session
def shift_history(shift_id):
    """Get shift change history"""
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        history = audit_db.get_record_history('Shifts', shift_id)
        shift = shifts_db.get_by_id(shift_id)
        
        return jsonify({
            'success': True,
            'shift': shift,
            'history': history
        })
    except Exception as e:
        print(f"Error getting shift history: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred'})