# routes/bom.py
"""
Bill of Materials (BOM) Viewer routes.
"""

from flask import Blueprint, render_template, request, session, redirect, url_for, flash, jsonify, send_file
from auth import require_login
from routes.main import validate_session
# UPDATED IMPORT:
from database import get_erp_service
import openpyxl
from io import BytesIO
from datetime import datetime

bom_bp = Blueprint('bom', __name__, url_prefix='/bom')
erp_service = get_erp_service() # This now gets the refactored service instance

@bom_bp.route('/')
@validate_session
def view_boms():
    """Renders the main BOM viewer page."""
    if not require_login(session):
        return redirect(url_for('main.login'))

    parent_part_number = request.args.get('part_number', None)

    try:
        boms = erp_service.get_bom_data(parent_part_number) # Call remains the same
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

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "BOM Export"
        ws.append(headers)

        for row_data in rows:
            ws.append(row_data)

        output = BytesIO()
        wb.save(output)
        output.seek(0)

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