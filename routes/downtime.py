"""
Downtime entry routes - COMPLETE WITH EDIT/DELETE
iPad-optimized interface for production floor use
"""

from flask import Blueprint, render_template, redirect, url_for, session, flash, jsonify, request
from auth import require_login, require_admin, require_user
from routes.main import validate_session
from database import facilities_db, lines_db, categories_db, downtimes_db, shifts_db, audit_db
from utils import get_client_info
from datetime import datetime

downtime_bp = Blueprint('downtime', __name__)

@downtime_bp.route('/downtime')
@validate_session
def entry_form():
    """Display the downtime entry form"""
    if not require_login(session):
        return redirect(url_for('main.login'))
    
    if not (require_admin(session) or require_user(session)):
        flash('You do not have permission to report downtime.', 'error')
        return redirect(url_for('main.dashboard'))
    
    # Get data for dropdowns
    facilities = facilities_db.get_all(active_only=True)
    lines = lines_db.get_all(active_only=True)
    categories = categories_db.get_hierarchical(active_only=True)
    shifts = shifts_db.get_all(active_only=True)
    
    # Auto-detect current shift
    current_time = datetime.now()
    current_shift = None
    
    for shift in shifts:
        start_time = datetime.strptime(shift['start_time'], '%H:%M').time()
        end_time = datetime.strptime(shift['end_time'], '%H:%M').time()
        
        if shift.get('is_overnight'):
            # Overnight shift
            if current_time.time() >= start_time or current_time.time() < end_time:
                current_shift = shift
                break
        else:
            # Regular shift
            if start_time <= current_time.time() < end_time:
                current_shift = shift
                break
    
    return render_template('downtime/entry.html',
                         facilities=facilities,
                         lines=lines,
                         categories=categories,
                         shifts=shifts,
                         current_shift=current_shift,
                         user=session['user'])

@downtime_bp.route('/downtime/submit', methods=['POST'])
@validate_session
def submit_downtime():
    """Submit a new downtime entry or update existing"""
    if not (require_admin(session) or require_user(session)):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        # Get form data
        downtime_id = request.form.get('downtime_id')  # For updates
        facility_id = request.form.get('facility_id')
        line_id = request.form.get('line_id')
        category_id = request.form.get('category_id')
        shift_id = request.form.get('shift_id')
        start_time = request.form.get('start_time')
        end_time = request.form.get('end_time')
        crew_size = request.form.get('crew_size', '1')
        reason_notes = request.form.get('comments', '').strip()
        
        # === ADDED: Get ERP data from form ===
        erp_job_number = request.form.get('erp_job_number')
        erp_part_number = request.form.get('erp_part_number')
        erp_part_description = request.form.get('erp_part_description')
        
        # Validate required fields
        if not all([facility_id, line_id, category_id, start_time, end_time]):
            return jsonify({'success': False, 'message': 'All required fields must be filled'})
        
        # Validate crew size
        try:
            crew_size = int(crew_size)
            if crew_size < 1 or crew_size > 10:
                return jsonify({'success': False, 'message': 'Crew size must be between 1 and 10'})
        except ValueError:
            return jsonify({'success': False, 'message': 'Crew size must be a number'})
        
        # Data for create/update
        data = {
            'line_id': line_id,
            'category_id': category_id,
            'shift_id': shift_id,
            'start_time': start_time,
            'end_time': end_time,
            'crew_size': crew_size,
            'reason_notes': reason_notes,
            'entered_by': session['user']['username'],
            # === ADDED: Pass ERP fields to database layer ===
            'erp_job_number': erp_job_number,
            'erp_part_number': erp_part_number,
            'erp_part_description': erp_part_description
        }
        
        if downtime_id:
            # Update existing entry
            success, message = downtimes_db.update(
                downtime_id, data, session['user']['username']
            )
            action = 'UPDATE'
        else:
            # Create new entry
            success, message, downtime_id = downtimes_db.create(data)
            action = 'INSERT'
        
        if success:
            # Log in audit
            ip, user_agent = get_client_info()
            audit_db.log(
                table_name='Downtimes',
                record_id=downtime_id,
                action_type=action,
                username=session['user']['username'],
                ip=ip,
                user_agent=user_agent,
                notes=f"Downtime {'updated' if action == 'UPDATE' else 'reported'} for line {line_id}"
            )
            
            return jsonify({
                'success': True,
                'message': message,
                'downtime_id': downtime_id,
                'action': action
            })
        else:
            return jsonify({'success': False, 'message': message})
        
    except Exception as e:
        print(f"Error submitting downtime: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred while submitting'})

@downtime_bp.route('/downtime/get/<int:downtime_id>')
@validate_session
def get_downtime(downtime_id):
    """Get a specific downtime entry for editing"""
    if not (require_admin(session) or require_user(session)):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    entry = downtimes_db.get_by_id(downtime_id)
    
    if not entry:
        return jsonify({'success': False, 'message': 'Entry not found'})
    
    # Check ownership
    if entry['entered_by'] != session['user']['username']:
        if not session['user'].get('is_admin'):
            return jsonify({'success': False, 'message': 'You can only edit your own entries'})
    
    # Format datetime for HTML input
    entry['start_time_formatted'] = entry['start_time'].strftime('%Y-%m-%dT%H:%M')
    entry['end_time_formatted'] = entry['end_time'].strftime('%Y-%m-%dT%H:%M')
    
    return jsonify({
        'success': True,
        'entry': entry
    })

@downtime_bp.route('/downtime/delete/<int:downtime_id>', methods=['POST'])
@validate_session
def delete_downtime(downtime_id):
    """Delete a downtime entry"""
    if not (require_admin(session) or require_user(session)):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    success, message = downtimes_db.delete(
        downtime_id, session['user']['username']
    )
    
    if success:
        # Log in audit
        ip, user_agent = get_client_info()
        audit_db.log(
            table_name='Downtimes',
            record_id=downtime_id,
            action_type='DELETE',
            username=session['user']['username'],
            ip=ip,
            user_agent=user_agent
        )
    
    return jsonify({'success': success, 'message': message})

@downtime_bp.route('/downtime/api/lines/<int:facility_id>')
@validate_session
def get_facility_lines(facility_id):
    """API endpoint to get lines for a specific facility"""
    if not require_login(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    lines = lines_db.get_by_facility(facility_id, active_only=True)
    return jsonify({
        'success': True,
        'lines': [{'id': l['line_id'], 'name': l['line_name']} for l in lines]
    })

@downtime_bp.route('/downtime/api/subcategories/<int:parent_id>')
@validate_session
def get_subcategories(parent_id):
    """API endpoint to get subcategories for a main category"""
    if not require_login(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    categories = categories_db.get_all(active_only=True)
    subcategories = [c for c in categories if c.get('parent_id') == parent_id]
    
    return jsonify({
        'success': True,
        'subcategories': [{'id': c['category_id'], 'name': c['category_name'], 'code': c['category_code']} 
                         for c in subcategories]
    })

# In routes/downtime.py, find the get_today_entries function and replace it with:

@downtime_bp.route('/downtime/api/today-entries/<int:line_id>')
@validate_session
def get_today_entries(line_id):
    """Get ALL entries for a specific line today (from all users)"""
    if not require_login(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    # Get ALL entries for this line today (not just current user's)
    entries = downtimes_db.get_all_entries_for_line_today(line_id)
    
    # Format for display
    for entry in entries:
        entry['start_time_str'] = entry['start_time'].strftime('%H:%M')
        entry['end_time_str'] = entry['end_time'].strftime('%H:%M')
        entry['start_time_formatted'] = entry['start_time'].strftime('%Y-%m-%dT%H:%M')
        entry['end_time_formatted'] = entry['end_time'].strftime('%Y-%m-%dT%H:%M')
        # Mark if this entry belongs to current user
        entry['is_own_entry'] = (entry['entered_by'] == session['user']['username'])
    
    return jsonify({
        'success': True,
        'entries': entries
    })