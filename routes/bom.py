# dangquyenbui-dotcom/downtime_tracker/downtime_tracker-953d9e6915ad7fa465db9a8f87b8a56d713b0537/routes/bom.py
"""
Bill of Materials (BOM) Viewer routes.
"""

from flask import Blueprint, render_template, request, session, redirect, url_for, flash, jsonify, send_file
from auth import require_login
from routes.main import validate_session
from database.erp_connection import get_erp_service
import openpyxl
from io import BytesIO
from datetime import datetime

bom_bp = Blueprint('bom', __name__, url_prefix='/bom')
erp_service = get_erp_service()

@bom_bp.route('/')
@validate_session
def view_boms():
    """Renders the main BOM viewer page."""
    if not require_login(session):
        return redirect(url_for('main.login'))

    # Allow filtering by a specific parent part number via query parameter
    parent_part_number = request.args.get('part_number', None)
    
    # Fetch all BOM data from the ERP
    try:
        boms = erp_service.get_bom_data(parent_part_number)
    except Exception as e:
        flash(f'Error fetching BOM data from ERP: {e}', 'error')
        boms = []

    return render_template(
        'bom/index.html',
        user=session['user'],
        boms=boms,
        filter_part_number=parent_part_number
    )

@bom_bp.route('/api/export-xlsx', methods=['POST'])
@validate_session
def export_boms_xlsx():
    """API endpoint to export the visible BOM data to an XLSX file."""
    if not require_login(session):
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
        ws.title = "BOM Export"

        # Write headers
        ws.append(headers)
        
        # Write data rows
        for row_data in rows:
            ws.append(row_data)

        # Save the workbook to a BytesIO object
        output = BytesIO()
        wb.save(output)
        output.seek(0)

        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"bom_export_{timestamp}.xlsx"

        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        print(f"Error exporting BOMs: {e}")
        return jsonify({'success': False, 'message': 'An error occurred during export.'}), 500