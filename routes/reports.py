# routes/reports.py
"""
Reporting routes for generating and viewing system reports.
"""
# <<< MODIFICATION: Added send_file
from flask import (
    Blueprint, render_template, redirect, url_for, session, request, flash, send_file
)
from auth import require_login, require_admin
from routes.main import validate_session
from database import facilities_db, lines_db, get_erp_service
from database.reports import reports_db
from datetime import datetime, timedelta
import traceback
from collections import OrderedDict
import io # <<< MODIFICATION: Added io
from utils.pdf_generator import generate_coc_pdf # <<< MODIFICATION: Import new PDF generator

# Helper function
def safe_float(value, default=0.0):
    """Safely convert value to float, handling None and potential errors."""
    if value is None: return default
    try: return float(value)
    except (TypeError, ValueError): return default

# Helper function to format dates
def _format_date(date_obj, date_format='%m/%d/%Y', default='N/A'): # Format: MM/DD/YYYY
    """Safely format a datetime object, handling None."""
    if date_obj is None:
        return default
    try:
        return date_obj.strftime(date_format)
    except AttributeError:
        return default

# ***** HELPER FUNCTION for CoC Report (Unchanged from last step) *****
def _get_single_job_details(job_number_str):
    """Fetches and processes data for a single job for the CoC report."""
    if not job_number_str:
        return None

    erp_service = get_erp_service()
    raw_data = erp_service.get_coc_report_data(job_number_str)

    if not raw_data or not raw_data.get("header"):
        return {'error': f"Job '{job_number_str}' not found in the ERP system."}

    header = raw_data["header"]
    fifo_details = raw_data.get("fifo_details", [])
    relieve_details = raw_data.get("relieve_details", [])

    job_data = {
        'job_number': str(header['jo_jobnum']),
        'part_number': header.get('part_number', ''),
        'part_description': header.get('part_description', ''),
        'customer_name': header.get('customer_name', 'N/A'),
        'sales_order': str(header.get('sales_order_number', '')) if header.get('sales_order_number') else '',
        'required_qty': safe_float(header.get('required_quantity')),
        'completed_qty': 0.0,
        'aggregated_transactions': {}
    }

    finish_job_entries = []
    other_fifo_entries = []
    
    fi_id_to_details_map = {
        row.get('fi_id'): {
            'lot_number': row.get('lot_number', ''),
            'exp_date': _format_date(row.get('fi_expires'))
        }
        for row in fifo_details if row.get('fi_id')
    }

    for row in fifo_details:
        action = row.get('fi_action')
        timestamp = row.get('fi_recdate')
        quantity = safe_float(row.get('fi_quant'))

        if action == 'Finish Job' and timestamp:
            finish_job_entries.append({'timestamp': timestamp, 'quantity': quantity})
            job_data['completed_qty'] += quantity
        else:
            other_fifo_entries.append(row)

    finish_job_entries.sort(key=lambda x: x['timestamp'])

    for row in other_fifo_entries:
        part_num = row.get('part_number', '')
        part_desc = row.get('part_description', '')
        lot_num = row.get('lot_number', '') 
        exp_date = _format_date(row.get('fi_expires')) 
        action = row.get('fi_action')
        quantity = safe_float(row.get('fi_quant'))

        if not part_num: continue 

        agg_key = (part_num, lot_num, exp_date) 

        if agg_key not in job_data['aggregated_transactions']:
            job_data['aggregated_transactions'][agg_key] = {
                'part_number': part_num,
                'part_description': part_desc,
                'lot_number': lot_num,
                'exp_date': exp_date,
                'Starting Lot Qty': 0.0,
                'Ending Inventory': 0.0,
                'Packaged Qty': 0.0,
                'Yield Cost/Scrap': 0.0,
                'Yield Loss': 0.0
            }
        if not job_data['aggregated_transactions'][agg_key].get('part_description') and part_desc:
             job_data['aggregated_transactions'][agg_key]['part_description'] = part_desc

        if action == 'Issued inventory':
            job_data['aggregated_transactions'][agg_key]['Starting Lot Qty'] += quantity
        elif action == 'De-issue':
            job_data['aggregated_transactions'][agg_key]['Ending Inventory'] += quantity

    relieve_pointer = 0 
    processed_relieve_ids = set() 

    for fj_entry in finish_job_entries:
        fj_timestamp = fj_entry['timestamp']

        for i in range(relieve_pointer, len(relieve_details)):
            relieve_row = relieve_details[i]
            relieve_timestamp = relieve_row.get('f2_recdate')
            relieve_id = relieve_row.get('f2_id')

            if relieve_id is None: 
                print(f"Warning: Relieve transaction missing unique ID: {relieve_row}")
                continue

            if relieve_timestamp and relieve_timestamp <= fj_timestamp:
                if relieve_id not in processed_relieve_ids:
                    part_num = relieve_row.get('part_number', '')
                    part_desc = relieve_row.get('part_description', '')
                    quantity = safe_float(relieve_row.get('net_quantity'))
                    
                    linked_fi_id = relieve_row.get('f2_fiid')
                    details = fi_id_to_details_map.get(linked_fi_id, {'lot_number': '', 'exp_date': 'N/A'})
                    lot_num = details['lot_number']
                    exp_date = details['exp_date']

                    if not part_num: continue 
                    
                    agg_key = (part_num, lot_num, exp_date)

                    if agg_key not in job_data['aggregated_transactions']:
                        job_data['aggregated_transactions'][agg_key] = {
                            'part_number': part_num,
                            'part_description': part_desc,
                            'lot_number': lot_num,
                            'exp_date': exp_date,
                            'Starting Lot Qty': 0.0,
                            'Ending Inventory': 0.0,
                            'Packaged Qty': 0.0,
                            'Yield Cost/Scrap': 0.0,
                            'Yield Loss': 0.0
                        }
                    if not job_data['aggregated_transactions'][agg_key].get('part_description') and part_desc:
                         job_data['aggregated_transactions'][agg_key]['part_description'] = part_desc

                    job_data['aggregated_transactions'][agg_key]['Packaged Qty'] += quantity
                    processed_relieve_ids.add(relieve_id) 

                relieve_pointer = i + 1

            elif relieve_timestamp and relieve_timestamp > fj_timestamp:
                break

    for agg_key, summary in job_data['aggregated_transactions'].items():
        issued = summary.get('Starting Lot Qty', 0.0)
        relieve = summary.get('Packaged Qty', 0.0) 
        deissue = summary.get('Ending Inventory', 0.0)
        yield_cost = issued - relieve - deissue
        summary['Yield Cost/Scrap'] = yield_cost
        summary['Yield Loss'] = (yield_cost / relieve) * 100.0 if relieve != 0 else 0.0

    job_data['aggregated_list'] = [
        summary for summary in job_data['aggregated_transactions'].values()
        if not summary.get('part_number', '').startswith('0800-') 
           and summary.get('part_number', '') != job_data['part_number']
    ]
    job_data['aggregated_list'].sort(key=lambda x: (
        x.get('part_number', ''), 
        x.get('lot_number', ''),
        x.get('exp_date', '')
    ))

    grouped_list = OrderedDict()
    for summary in job_data['aggregated_list']:
        part_num = summary.get('part_number', '')
        if part_num not in grouped_list:
            grouped_list[part_num] = {
                'part_description': summary.get('part_description', ''),
                'lots': []
            }
        grouped_list[part_num]['lots'].append(summary)
    
    job_data['grouped_list'] = grouped_list

    return job_data
# ***** END HELPER FUNCTION *****


reports_bp = Blueprint('reports', __name__, url_prefix='/reports')

@reports_bp.route('/')
# ... (hub route is unchanged) ...
def hub():
    if not require_login(session):
        return redirect(url_for('main.login'))
    if not require_admin(session):
        flash('Admin privileges are required to view reports.', 'error')
        return redirect(url_for('main.dashboard'))
    return render_template('reports/hub.html', user=session['user'])

@reports_bp.route('/downtime-summary')
# ... (downtime_summary route is unchanged) ...
def downtime_summary():
    if not require_login(session):
        return redirect(url_for('main.login'))
    if not require_admin(session):
        flash('Admin privileges are required to view reports.', 'error')
        return redirect(url_for('main.dashboard'))

    today = datetime.now()
    start_date_str = request.args.get('start_date', (today - timedelta(days=7)).strftime('%Y-%m-%d'))
    end_date_str = request.args.get('end_date', today.strftime('%Y-%m-%d'))
    facility_id = request.args.get('facility_id', type=int)
    line_id = request.args.get('line_id', type=int)

    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)

    report_data = reports_db.get_downtime_summary(
        start_date=start_date, end_date=end_date,
        facility_id=facility_id, line_id=line_id
    )
    facilities = facilities_db.get_all(active_only=True)
    lines = lines_db.get_by_facility(facility_id=facility_id, active_only=True) if facility_id else []

    return render_template(
        'reports/downtime_summary.html',
        user=session['user'], report_data=report_data,
        filters={'start_date': start_date_str, 'end_date': end_date_str, 'facility_id': facility_id, 'line_id': line_id},
        facilities=facilities, lines=lines
    )

@reports_bp.route('/shipment-forecast')
# ... (shipment_forecast route is unchanged) ...
def shipment_forecast():
    if not require_login(session) or not require_admin(session):
        flash('Admin privileges are required to view reports.', 'error')
        return redirect(url_for('main.dashboard'))
    try:
        forecast_data = reports_db.get_shipment_forecast()
    except Exception as e:
        flash(f'An error occurred while generating the forecast: {e}', 'error')
        forecast_data = {'month_name': datetime.now().strftime('%B %Y'), 'likely_total_value': 0, 'at_risk_total_value': 0, 'likely_orders': [], 'at_risk_orders': []}
    return render_template('reports/shipment_forecast.html', user=session['user'], forecast=forecast_data)

@reports_bp.route('/coc', methods=['GET'])
@validate_session
def coc_report():
    if not require_admin(session):
        flash('Admin privileges are required to view this report.', 'error')
        return redirect(url_for('main.dashboard'))

    job_number_param = request.args.get('job_number', '').strip()
    job_details = None
    error_message = None

    if job_number_param:
        try:
            job_details = _get_single_job_details(job_number_param)
            if job_details and 'error' in job_details:
                error_message = job_details['error']
                job_details = None
        except Exception as e:
            flash(f'An error occurred while fetching job details: {e}', 'error')
            traceback.print_exc()
            error_message = f"An unexpected error occurred: {str(e)}"
            job_details = None

    return render_template(
        'reports/coc.html',
        user=session['user'],
        job_number=job_number_param,
        job_details=job_details,
        error_message=error_message
    )

# <<< MODIFICATION: Added new route for PDF export
@reports_bp.route('/coc/pdf', methods=['GET'])
@validate_session
def coc_report_pdf():
    """
    Generates and serves a PDF version of the CoC report.
    """
    if not require_admin(session):
        flash('Admin privileges are required to export reports.', 'error')
        return redirect(url_for('main.dashboard'))

    job_number_param = request.args.get('job_number', '').strip()
    if not job_number_param:
        flash('A Job Number is required to generate a PDF.', 'error')
        return redirect(url_for('reports.coc_report'))

    try:
        # Get the same data as the web page
        job_details = _get_single_job_details(job_number_param)
        
        if not job_details or 'error' in job_details:
            error_message = job_details.get('error', 'Job not found')
            flash(f'Could not generate PDF: {error_message}', 'error')
            return redirect(url_for('reports.coc_report', job_number=job_number_param))
        
        # Generate the PDF
        pdf_buffer, filename = generate_coc_pdf(job_details)
        
        # Send the PDF as a file download
        return send_file(
            pdf_buffer,
            as_attachment=True,
            download_name=filename,
            mimetype='application/pdf'
        )

    except Exception as e:
        flash(f'An error occurred while generating the PDF: {e}', 'error')
        traceback.print_exc()
        return redirect(url_for('reports.coc_report', job_number=job_number_param))