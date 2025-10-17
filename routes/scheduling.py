"""
Production Scheduling routes
Handles display and updates for the production scheduling grid.
"""

from flask import Blueprint, render_template, jsonify, request, session, redirect, url_for, flash, send_file
from auth import require_login, require_scheduling_admin, require_scheduling_user
from routes.main import validate_session
from database import scheduling_db
import traceback
import openpyxl
from io import BytesIO
from datetime import datetime

# The url_prefix makes this blueprint's routes available under '/scheduling'
scheduling_bp = Blueprint('scheduling', __name__, url_prefix='/scheduling')

@scheduling_bp.route('/')
@validate_session
def index():
    """Renders the main production scheduling grid page."""
    if not require_login(session):
        return redirect(url_for('main.login'))
    
    if not (require_scheduling_admin(session) or require_scheduling_user(session)):
        flash('Scheduling privileges are required to access this module.', 'error')
        return redirect(url_for('main.dashboard'))

    # Fetch data from ERP joined with local projections
    data = scheduling_db.get_schedule_data()
    
    # Unpack the dictionary to pass its contents as separate variables to the template
    return render_template(
        'scheduling/index.html', 
        user=session['user'],
        schedule_data=data.get('grid_data', []),
        fg_on_hand_split=data.get('fg_on_hand_split', {}),
        shipped_current_month=data.get('shipped_current_month', 0),
        now=datetime.now()
    )

@scheduling_bp.route('/api/update-projection', methods=['POST'])
@validate_session
def update_projection():
    """API endpoint to save projection data from the grid."""
    if not require_scheduling_admin(session):
        return jsonify({'success': False, 'message': 'Edit permission required'}), 403

    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': 'Invalid data received'}), 400

        # Extract data from the JSON payload
        so_number = data.get('so_number')
        part_number = data.get('part_number')
        risk_type = data.get('risk_type')
        quantity = data.get('quantity')
        username = session.get('user', {}).get('username', 'unknown')

        # Basic validation
        if not all([so_number, part_number, risk_type]):
             return jsonify({'success': False, 'message': 'Missing required fields: so_number, part_number, or risk_type'}), 400

        try:
            quantity = float(quantity) if quantity is not None else 0.0
        except (ValueError, TypeError):
            return jsonify({'success': False, 'message': 'Quantity must be a valid number'}), 400

        # Call the database method to perform the upsert
        success, message = scheduling_db.update_projection(
            so_number=so_number,
            part_number=part_number,
            risk_type=risk_type,
            quantity=quantity,
            username=username
        )

        if success:
            return jsonify({'success': True, 'message': message})
        else:
            return jsonify({'success': False, 'message': message}), 500
    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'message': 'An internal server error occurred.'}), 500

@scheduling_bp.route('/api/export-xlsx', methods=['POST'])
@validate_session
def export_xlsx():
    """API endpoint to export the visible grid data to an XLSX file."""
    if not (require_scheduling_admin(session) or require_scheduling_user(session)):
        return jsonify({'success': False, 'message': 'Authentication required'}), 401

    try:
        data = request.get_json()
        headers = data.get('headers', [])
        rows = data.get('rows', [])

        if not headers or not rows:
            return jsonify({'success': False, 'message': 'No data to export'}), 400

        # Create a new workbook and select the active sheet
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Schedule Export"

        # Write headers
        ws.append(headers)
        
        # Write data rows, attempting to convert to numbers
        for row_data in rows:
            processed_row = []
            for cell_value in row_data:
                # If the value is a string, try to clean and convert it to a number
                if isinstance(cell_value, str):
                    cleaned_value = cell_value.replace('$', '').replace(',', '')
                    try:
                        # Try converting to float for decimals
                        processed_row.append(float(cleaned_value))
                    except (ValueError, TypeError):
                        # If it fails, it's not a number, so append the original string
                        processed_row.append(cell_value)
                else:
                    # If it's already a number (or None), append it as is
                    processed_row.append(cell_value)
            
            ws.append(processed_row)

        # Save the workbook to a BytesIO object
        output = BytesIO()
        wb.save(output)
        output.seek(0)

        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"schedule_export_{timestamp}.xlsx"

        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'message': 'An error occurred during export.'}), 500