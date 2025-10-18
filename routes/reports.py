# routes/reports.py
"""
Reporting routes for generating and viewing system reports.
"""

from flask import Blueprint, render_template, redirect, url_for, session, request, flash
from auth import require_login, require_admin
from routes.main import validate_session
from database import facilities_db, lines_db, get_erp_service
from database.reports import reports_db
from datetime import datetime, timedelta
import traceback

# Helper function
def safe_float(value, default=0.0):
    """Safely convert value to float, handling None and potential errors."""
    if value is None: return default
    try: return float(value)
    except (TypeError, ValueError): return default

# ***** MODIFIED HELPER FUNCTION for CoC Report *****
def _get_single_job_details(job_number_str):
    """Fetches and processes data for a single job for the CoC report."""
    if not job_number_str:
        return None

    erp_service = get_erp_service() # Get the service instance
    raw_data = erp_service.get_coc_report_data(job_number_str) # Fetches header, fifo, relieve

    if not raw_data or not raw_data.get("header"):
        return {'error': f"Job '{job_number_str}' not found in the ERP system."}

    header = raw_data["header"]
    fifo_details = raw_data.get("fifo_details", []) # These are now sorted by fi_recdate
    relieve_details = raw_data.get("relieve_details", []) # These are now sorted by f2_recdate

    # --- Initialize Job Data Structure ---
    job_data = {
        'job_number': str(header['jo_jobnum']),
        'part_number': header.get('part_number', ''),
        'customer_name': header.get('customer_name', 'N/A'),
        'sales_order': str(header.get('sales_order_number', '')) if header.get('sales_order_number') else '',
        'required_qty': safe_float(header.get('required_quantity')),
        'completed_qty': 0.0,
        'aggregated_transactions': {} # Stores the final calculated values per component
    }

    # --- Separate FIFO transactions and identify Finish Job timestamps ---
    finish_job_entries = []
    other_fifo_entries = []
    for row in fifo_details:
        action = row.get('fi_action')
        timestamp = row.get('fi_recdate') # Get timestamp
        quantity = safe_float(row.get('fi_quant'))

        # Ensure timestamp is valid before using
        if action == 'Finish Job' and timestamp:
            finish_job_entries.append({'timestamp': timestamp, 'quantity': quantity})
            job_data['completed_qty'] += quantity # Sum total completed qty
        else:
            other_fifo_entries.append(row)

    # Sort Finish Job entries just in case the query order wasn't perfect
    finish_job_entries.sort(key=lambda x: x['timestamp'])

    # --- Process FIFO (Issued/De-issue) ---
    for row in other_fifo_entries:
        part_num = row.get('part_number', '')
        part_desc = row.get('part_description', '')
        action = row.get('fi_action')
        quantity = safe_float(row.get('fi_quant'))

        if not part_num: continue # Skip if no part number

        if part_num not in job_data['aggregated_transactions']:
            job_data['aggregated_transactions'][part_num] = {
                'part_number': part_num, 'part_description': part_desc,
                'Issued inventory': 0.0, 'De-issue': 0.0, 'Relieve Job': 0.0, # Initialize Relieve Job here
                'Yield Cost/Scrap': 0.0, 'Yield Loss': 0.0
            }
        # Update description if it was missing initially
        if not job_data['aggregated_transactions'][part_num].get('part_description') and part_desc:
             job_data['aggregated_transactions'][part_num]['part_description'] = part_desc

        if action == 'Issued inventory':
            job_data['aggregated_transactions'][part_num]['Issued inventory'] += quantity
        elif action == 'De-issue':
            job_data['aggregated_transactions'][part_num]['De-issue'] += quantity
        # Ignore 'Finish Job' here as it's handled separately

    # --- Process Relieve Job (dtfifo2) based on Finish Job Timestamps ---
    relieve_pointer = 0 # Index for the next relieve transaction to check
    processed_relieve_ids = set() # Store unique IDs (e.g., f2_id) of processed relieve entries

    # Important: Ensure relieve_details are properly sorted by timestamp ASC
    # The query already does this with ORDER BY f2_recdate ASC

    for fj_entry in finish_job_entries:
        fj_timestamp = fj_entry['timestamp']

        # Iterate through relieve transactions starting from the last pointer
        for i in range(relieve_pointer, len(relieve_details)):
            relieve_row = relieve_details[i]
            relieve_timestamp = relieve_row.get('f2_recdate')
            # *** Use a unique ID from dtfifo2, assuming 'f2_id' ***
            # *** If your table uses a different ID, change 'f2_id' below ***
            relieve_id = relieve_row.get('f2_id')

            if relieve_id is None: # Skip if no unique ID found
                print(f"Warning: Relieve transaction missing unique ID: {relieve_row}")
                continue

            # Check if relieve timestamp is valid and occurred at or before the Finish Job
            if relieve_timestamp and relieve_timestamp <= fj_timestamp:
                # Check if this specific relieve transaction has already been counted
                if relieve_id not in processed_relieve_ids:
                    part_num = relieve_row.get('part_number', '')
                    part_desc = relieve_row.get('part_description', '')
                    quantity = safe_float(relieve_row.get('net_quantity'))

                    if not part_num: continue # Skip if no part number

                    # Initialize component if it hasn't been seen before (e.g., only in dtfifo2)
                    if part_num not in job_data['aggregated_transactions']:
                        job_data['aggregated_transactions'][part_num] = {
                            'part_number': part_num, 'part_description': part_desc,
                            'Issued inventory': 0.0, 'De-issue': 0.0, 'Relieve Job': 0.0,
                            'Yield Cost/Scrap': 0.0, 'Yield Loss': 0.0
                        }
                    if not job_data['aggregated_transactions'][part_num].get('part_description') and part_desc:
                         job_data['aggregated_transactions'][part_num]['part_description'] = part_desc

                    # Add the quantity to the calculated 'Relieve Job' total
                    job_data['aggregated_transactions'][part_num]['Relieve Job'] += quantity
                    processed_relieve_ids.add(relieve_id) # Mark this specific transaction as processed

                # Update the pointer to start next search from the *next* relieve transaction
                relieve_pointer = i + 1

            elif relieve_timestamp and relieve_timestamp > fj_timestamp:
                # Because relieve transactions are sorted, we can stop searching for this Finish Job
                # The relieve_pointer remains where it is for the next Finish Job check
                break
            # else: relieve_timestamp is None, continue checking next relieve entry

    # --- Calculate Yields using the *calculated* Relieve Job totals ---
    for part_num, summary in job_data['aggregated_transactions'].items():
        issued = summary.get('Issued inventory', 0.0)
        relieve = summary.get('Relieve Job', 0.0) # Use the value calculated above
        deissue = summary.get('De-issue', 0.0)
        yield_cost = issued - relieve - deissue
        summary['Yield Cost/Scrap'] = yield_cost
        summary['Yield Loss'] = (yield_cost / relieve) * 100.0 if relieve != 0 else 0.0

    # Filter out unwanted parts (Finished Good itself, 0800 items)
    job_data['aggregated_list'] = [
        summary for part_num, summary in job_data['aggregated_transactions'].items()
        if not part_num.startswith('0800-') and part_num != job_data['part_number']
    ]
    # Sort the final list alphabetically by part number for display consistency
    job_data['aggregated_list'].sort(key=lambda x: x.get('part_number', ''))


    return job_data
# ***** END MODIFIED HELPER FUNCTION *****


reports_bp = Blueprint('reports', __name__, url_prefix='/reports')

@reports_bp.route('/')
@validate_session
def hub():
    if not require_login(session):
        return redirect(url_for('main.login'))
    if not require_admin(session):
        flash('Admin privileges are required to view reports.', 'error')
        return redirect(url_for('main.dashboard'))
    return render_template('reports/hub.html', user=session['user'])

@reports_bp.route('/downtime-summary')
@validate_session
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
@validate_session
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
            job_details = _get_single_job_details(job_number_param) # Use the updated helper
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