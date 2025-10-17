# routes/po.py
"""
Purchase Order (PO) Viewer routes.
"""

from flask import Blueprint, render_template, request, session, redirect, url_for, flash, jsonify, send_file
from auth import require_login
from routes.main import validate_session
# UPDATED IMPORT:
from database import get_erp_service
import openpyxl
from io import BytesIO
from datetime import datetime

po_bp = Blueprint('po', __name__, url_prefix='/po')
erp_service = get_erp_service() # This now gets the refactored service instance

@po_bp.route('/')
@validate_session
def view_pos():
    """Renders the main PO viewer page."""
    if not require_login(session):
        return redirect(url_for('main.login'))

    try:
        purchase_orders = erp_service.get_detailed_purchase_order_data() # Call remains the same
    except Exception as e:
        flash(f'Error fetching PO data from ERP: {e}', 'error')
        purchase_orders = []

    return render_template(
        'po/index.html',
        user=session['user'],
        purchase_orders=purchase_orders
    )

@po_bp.route('/api/export-xlsx', methods=['POST'])
@validate_session
def export_pos_xlsx():
    """API endpoint to export the visible PO data to an XLSX file."""
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
        ws.title = "PO Export"
        ws.append(headers)

        for row_data in rows:
            ws.append(row_data)

        output = BytesIO()
        wb.save(output)
        output.seek(0)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"po_export_{timestamp}.xlsx"

        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        print(f"Error exporting POs: {e}")
        return jsonify({'success': False, 'message': 'An error occurred during export.'}), 500