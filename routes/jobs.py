"""
Routes for viewing live job data.
"""

from flask import Blueprint, render_template, session, redirect, url_for, flash, jsonify # Added jsonify
from auth import require_login
from routes.main import validate_session
from database.erp_connection import get_erp_service

jobs_bp = Blueprint('jobs', __name__, url_prefix='/jobs')
erp_service = get_erp_service()

def _get_job_data(job_numbers):
    """Helper function to fetch and process job data."""
    # --- MODIFICATION: Handle potentially large number of jobs ---
    # Fetch details in chunks if necessary to avoid overly large IN clauses,
    # though pyodbc handles parameterization well for many DBs.
    # For simplicity here, assume the driver/DB handles a large list.
    if not job_numbers:
        return []
    # --- END MODIFICATION ---

    job_details_raw = erp_service.get_open_job_details(job_numbers)
    relieve_job_raw = erp_service.get_relieve_job_data(job_numbers)

    jobs = {}

    # Process dtfifo transactions
    for row in job_details_raw:
        job_num_str = row['fi_postref']
        # --- MODIFICATION: Robust job number extraction ---
        job_num = job_num_str.replace('JJ-', '') if job_num_str and job_num_str.startswith('JJ-') else None
        if not job_num:
             print(f"Skipping transaction with unexpected fi_postref: {job_num_str}")
             continue
        # --- END MODIFICATION ---

        part_num = row['part_number']
        action = row['fi_action']
        quantity = row['fi_quant']
        part_desc = row.get('part_description', '') # Get description here

        if job_num not in jobs:
            jobs[job_num] = {
                'job_number': job_num,
                'completed_qty': 0,
                'part_number': '', # Main part number for the job header
                'transactions': [],
                'finish_job_transactions': [],
                'aggregated_transactions': {}
            }

        jobs[job_num]['transactions'].append(row)

        if part_num not in jobs[job_num]['aggregated_transactions']:
            jobs[job_num]['aggregated_transactions'][part_num] = {
                'part_number': part_num,
                'part_description': part_desc, # Store description
                'Finish Job': 0,
                'Issued inventory': 0,
                'De-issue': 0,
                'Relieve Job': 0
            }

        if action in jobs[job_num]['aggregated_transactions'][part_num]:
            jobs[job_num]['aggregated_transactions'][part_num][action] += quantity

        if action == 'Finish Job':
            jobs[job_num]['completed_qty'] += quantity
            jobs[job_num]['finish_job_transactions'].append(f"Finish Job: {'{:,.2f}'.format(quantity)}")
            # Set the main part_number for the job based on the Finish Job transaction
            if not jobs[job_num]['part_number']:
                 jobs[job_num]['part_number'] = part_num

    # Process dtfifo2 transactions for 'Relieve Job'
    for row in relieve_job_raw:
        job_num_str = row['f2_postref']
        # --- MODIFICATION: Robust job number extraction ---
        job_num = job_num_str.replace('JJ-', '') if job_num_str and job_num_str.startswith('JJ-') else None
        if not job_num:
            print(f"Skipping relieve transaction with unexpected f2_postref: {job_num_str}")
            continue
        # --- END MODIFICATION ---

        part_num = row['part_number']
        part_desc = row.get('part_description', '') # Get description here
        action = row['f2_action']
        quantity = row['net_quantity']

        if job_num not in jobs:
            jobs[job_num] = {
                'job_number': job_num,
                'completed_qty': 0,
                'part_number': '',
                'transactions': [],
                'finish_job_transactions': [],
                'aggregated_transactions': {}
            }

        consistent_transaction = {
            'fi_action': action,
            'fi_quant': quantity,
            'part_number': part_num,
            'part_description': part_desc
        }
        jobs[job_num]['transactions'].append(consistent_transaction)

        if part_num not in jobs[job_num]['aggregated_transactions']:
            jobs[job_num]['aggregated_transactions'][part_num] = {
                'part_number': part_num,
                'part_description': part_desc,
                'Finish Job': 0,
                'Issued inventory': 0,
                'De-issue': 0,
                'Relieve Job': 0
            }

        if action in jobs[job_num]['aggregated_transactions'][part_num]:
            jobs[job_num]['aggregated_transactions'][part_num][action] += quantity

    # Calculate Yield Cost/Scrap and Yield Loss, prepare final list
    job_list = []
    for job_num, job_data in jobs.items():
        if job_data['transactions']: # Only include jobs with some activity
            job_data['tooltip'] = '\n'.join(job_data['finish_job_transactions'])

            for part_num, summary in job_data['aggregated_transactions'].items():
                issued = summary.get('Issued inventory', 0)
                relieve = summary.get('Relieve Job', 0)
                deissue = summary.get('De-issue', 0)
                yield_cost = issued - relieve - deissue
                summary['Yield Cost/Scrap'] = yield_cost

                # --- MODIFICATION: Handle potential division by zero ---
                if relieve is not None and relieve != 0:
                    summary['Yield Loss'] = (yield_cost / relieve) * 100 if relieve != 0 else 0
                else:
                    summary['Yield Loss'] = 0 # Define yield loss as 0 if nothing was relieved
                # --- END MODIFICATION ---


            # Filter out unwanted parts *after* calculations
            job_data['aggregated_list'] = [
                summary for part_num, summary in job_data['aggregated_transactions'].items()
                if not part_num.startswith('0800-') and part_num != job_data['part_number']
            ]
            job_list.append(job_data)

    # --- MODIFICATION: Sort final list by job number ---
    job_list.sort(key=lambda x: x.get('job_number', ''))
    # --- END MODIFICATION ---

    return job_list


@jobs_bp.route('/open-jobs')
@validate_session
def view_open_jobs():
    """Renders the open jobs viewer page (Initial Load)."""
    if not require_login(session):
        return redirect(url_for('main.login'))

    job_list = []
    try:
        # --- MODIFICATION: Fetch all open job numbers ---
        job_numbers = erp_service.get_all_open_job_numbers()
        print(f"Found {len(job_numbers)} open job numbers.")
        # --- END MODIFICATION ---
        job_list = _get_job_data(job_numbers)
    except Exception as e:
        flash(f'Error fetching job data from ERP: {e}', 'error')
        job_list = []

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
        # --- MODIFICATION: Fetch all open job numbers ---
        job_numbers = erp_service.get_all_open_job_numbers()
        # --- END MODIFICATION ---
        job_list = _get_job_data(job_numbers)
        return jsonify(success=True, jobs=job_list)
    except Exception as e:
        print(f"Error fetching live job data: {e}") # Log error server-side
        return jsonify(success=False, message=f"Error fetching data: {e}"), 500