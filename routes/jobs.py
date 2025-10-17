# routes/jobs.py
"""
Routes for viewing live job data.
"""

from flask import Blueprint, render_template, session, redirect, url_for, flash, jsonify
from auth import require_login
from routes.main import validate_session
from database import get_erp_service

jobs_bp = Blueprint('jobs', __name__, url_prefix='/jobs')
erp_service = get_erp_service()

def safe_float(value, default=0.0):
    """Safely convert value to float, handling None and potential errors."""
    if value is None: return default
    try: return float(value)
    except (TypeError, ValueError): return default

def _get_job_data(job_numbers):
    """Helper function to fetch and process job data."""
    if not job_numbers:
        return []

    str_job_numbers = [str(jn) for jn in job_numbers] # Ensure strings

    # --- Step 1: Get Primary Header Info ---
    job_headers_raw = erp_service.get_open_job_headers(str_job_numbers)
    jobs = {}
    for header in job_headers_raw:
        job_num = str(header['jo_jobnum']) # Ensure job number is string key
        jobs[job_num] = {
            'job_number': job_num,
            'part_number': header.get('part_number', ''),
            'customer_name': header.get('customer_name', 'N/A'),
            'sales_order': str(header.get('sales_order_number', '')) if header.get('sales_order_number') else '',
            'required_qty': safe_float(header.get('required_quantity')),
            'completed_qty': 0.0, # Initialize completed qty
            'tooltip': '',        # Initialize tooltip
            'transactions': [],   # Store raw transactions (optional)
            'finish_job_transactions': [], # Store formatted finish job strings
            'aggregated_transactions': {}  # Store aggregated component data
        }

    # --- Step 2: Get Transaction Details ---
    job_details_raw = erp_service.get_open_job_details(str_job_numbers)
    relieve_job_raw = erp_service.get_relieve_job_data(str_job_numbers)
    all_transactions = job_details_raw + relieve_job_raw

    # --- Step 3: Process Transactions ---
    for row in all_transactions:
        is_relieve = 'f2_postref' in row
        job_num_str = row['f2_postref'] if is_relieve else row['fi_postref']
        job_num = job_num_str.replace('JJ-', '') if job_num_str and job_num_str.startswith('JJ-') else None

        # Only process transactions for jobs we found header info for
        if not job_num or job_num not in jobs:
            # print(f"Skipping transaction for unknown or invalid job: {job_num_str}")
            continue

        part_num = row.get('part_number', '')
        part_desc = row.get('part_description', '')
        action = row['f2_action'] if is_relieve else row['fi_action']
        quantity = safe_float(row['net_quantity'] if is_relieve else row['fi_quant'])

        # Store raw transaction (optional)
        jobs[job_num]['transactions'].append(row)

        # Aggregate quantities per component
        if part_num not in jobs[job_num]['aggregated_transactions']:
            jobs[job_num]['aggregated_transactions'][part_num] = {
                'part_number': part_num, 'part_description': part_desc,
                'Finish Job': 0.0, 'Issued inventory': 0.0, 'De-issue': 0.0, 'Relieve Job': 0.0
            }
        # Ensure description is populated if missing
        if not jobs[job_num]['aggregated_transactions'][part_num]['part_description'] and part_desc:
            jobs[job_num]['aggregated_transactions'][part_num]['part_description'] = part_desc

        # Aggregate quantities
        if action in jobs[job_num]['aggregated_transactions'][part_num]:
            jobs[job_num]['aggregated_transactions'][part_num][action] += quantity
        elif action == 'Relieve Job' and not is_relieve: # Handle potential 'Relieve Job' from dtfifo
             jobs[job_num]['aggregated_transactions'][part_num]['Relieve Job'] += quantity


        # Sum completed quantity and build tooltip string
        if action == 'Finish Job':
            jobs[job_num]['completed_qty'] += quantity
            jobs[job_num]['finish_job_transactions'].append(f"Finish Job: {'{:,.2f}'.format(quantity)}")

    # --- Step 4: Calculate Yields and finalize list ---
    job_list = []
    for job_num, job_data in jobs.items():
        # Update tooltip string in the main job data
        job_data['tooltip'] = '\n'.join(job_data['finish_job_transactions'])

        for part_num, summary in job_data['aggregated_transactions'].items():
            issued = summary.get('Issued inventory', 0.0)
            relieve = summary.get('Relieve Job', 0.0)
            deissue = summary.get('De-issue', 0.0)
            yield_cost = issued - relieve - deissue
            summary['Yield Cost/Scrap'] = yield_cost
            summary['Yield Loss'] = (yield_cost / relieve) * 100.0 if relieve != 0 else 0.0

        # Filter out unwanted parts *after* calculations
        job_data['aggregated_list'] = [
            summary for part_num, summary in job_data['aggregated_transactions'].items()
            if not part_num.startswith('0800-') and part_num != job_data['part_number']
        ]
        job_list.append(job_data)

    job_list.sort(key=lambda x: x.get('job_number', '')) # Sort by job number string
    return job_list


@jobs_bp.route('/open-jobs')
@validate_session
def view_open_jobs():
    """Renders the open jobs viewer page (Initial Load)."""
    if not require_login(session):
        return redirect(url_for('main.login'))

    job_list = []
    try:
        job_numbers = erp_service.get_all_open_job_numbers() # Get list of job number strings
        print(f"Found {len(job_numbers)} open job numbers.")
        if job_numbers:
            job_list = _get_job_data(job_numbers) # Process these job numbers
        else:
            job_list = []
            print("No open job numbers found.")
    except Exception as e:
        flash(f'Error fetching job data from ERP: {e}', 'error')
        job_list = []
        import traceback
        traceback.print_exc()

    return render_template(
        'jobs/index.html',
        user=session['user'],
        jobs=job_list
    )

@jobs_bp.route('/api/open-jobs-data')
@validate_session
def get_open_jobs_data():
    """API endpoint to fetch live job data as JSON."""
    if not require_login(session):
        return jsonify(success=False, message="Authentication required"), 401

    try:
        job_numbers = erp_service.get_all_open_job_numbers() # Get list of job number strings
        if job_numbers:
             job_list = _get_job_data(job_numbers) # Process these job numbers
        else:
             job_list = []
        return jsonify(success=True, jobs=job_list)
    except Exception as e:
        print(f"Error fetching live job data: {e}")
        import traceback
        traceback.print_exc()
        return jsonify(success=False, message=f"Error fetching data: {str(e)}"), 500