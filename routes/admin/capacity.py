# dangquyenbui-dotcom/downtime_tracker/downtime_tracker-953d9e6915ad7fa465db9a8f87b8a56d713b0537/routes/admin/capacity.py
"""
Admin routes for managing Production Capacity.
"""

from flask import Blueprint, render_template, redirect, url_for, session, jsonify, request, flash
from auth import require_login, require_admin
from routes.main import validate_session
from database import lines_db
from database.capacity import ProductionCapacityDB

admin_capacity_bp = Blueprint('admin_capacity', __name__)
capacity_db = ProductionCapacityDB()

@admin_capacity_bp.route('/capacity')
@validate_session
def capacity_management():
    if not require_admin(session):
        flash('Admin privileges required.', 'error')
        return redirect(url_for('main.dashboard'))

    capacities = capacity_db.get_all()
    lines = lines_db.get_all(active_only=True)
    
    # Filter out lines that already have a capacity set
    lines_with_capacity = {c['line_id'] for c in capacities}
    available_lines = [line for line in lines if line['line_id'] not in lines_with_capacity]

    return render_template(
        'admin/capacity.html',
        user=session['user'],
        capacities=capacities,
        available_lines=available_lines
    )

@admin_capacity_bp.route('/capacity/save', methods=['POST'])
@validate_session
def save_capacity():
    if not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 403

    try:
        data = request.get_json()
        line_id = data.get('line_id')
        capacity_per_shift = data.get('capacity_per_shift')
        unit = data.get('unit', 'units')
        notes = data.get('notes', '')
        username = session['user']['username']

        if not all([line_id, capacity_per_shift]):
            return jsonify({'success': False, 'message': 'Line and Capacity are required.'}), 400
        
        success, message = capacity_db.create_or_update(
            line_id, capacity_per_shift, unit, notes, username
        )

        return jsonify({'success': success, 'message': message})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@admin_capacity_bp.route('/capacity/delete', methods=['POST'])
@validate_session
def delete_capacity():
    if not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 403
        
    capacity_id = request.get_json().get('capacity_id')
    success, message = capacity_db.delete(capacity_id)
    return jsonify({'success': success, 'message': message})