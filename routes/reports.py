"""
Reporting routes for generating and viewing system reports.
"""

from flask import Blueprint, render_template, redirect, url_for, session, request, flash
from auth import require_login, require_admin
from routes.main import validate_session
from database import facilities_db, lines_db
from database.reports import reports_db # New reports database module
from datetime import datetime, timedelta

reports_bp = Blueprint('reports', __name__)

@reports_bp.route('/reports')
@validate_session
def hub():
    """Display the main reports hub page."""
    if not require_login(session):
        return redirect(url_for('main.login'))
    
    if not require_admin(session):
        flash('Admin privileges are required to view reports.', 'error')
        return redirect(url_for('main.dashboard'))
    
    return render_template('reports/hub.html', user=session['user'])

@reports_bp.route('/reports/downtime-summary')
@validate_session
def downtime_summary():
    """Display the Downtime Summary report."""
    if not require_login(session):
        return redirect(url_for('main.login'))

    if not require_admin(session):
        flash('Admin privileges are required to view reports.', 'error')
        return redirect(url_for('main.dashboard'))

    # Get filter values from URL query parameters
    today = datetime.now()
    start_date_str = request.args.get('start_date', (today - timedelta(days=7)).strftime('%Y-%m-%d'))
    end_date_str = request.args.get('end_date', today.strftime('%Y-%m-%d'))
    facility_id = request.args.get('facility_id', type=int)
    line_id = request.args.get('line_id', type=int)

    # Convert string dates to datetime objects for the query
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    # Add time component to end_date to include the full day
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)

    # Fetch report data from the new database module
    report_data = reports_db.get_downtime_summary(
        start_date=start_date,
        end_date=end_date,
        facility_id=facility_id,
        line_id=line_id
    )

    # Get data for filter dropdowns
    facilities = facilities_db.get_all(active_only=True)
    lines = lines_db.get_all(active_only=True) if facility_id else []

    return render_template(
        'reports/downtime_summary.html',
        user=session['user'],
        report_data=report_data,
        filters={
            'start_date': start_date_str,
            'end_date': end_date_str,
            'facility_id': facility_id,
            'line_id': line_id
        },
        facilities=facilities,
        lines=lines
    )

@reports_bp.route('/reports/shipment-forecast')
@validate_session
def shipment_forecast():
    """Displays the automated monthly shipment forecast."""
    if not require_login(session) or not require_admin(session):
        flash('Admin privileges are required to view reports.', 'error')
        return redirect(url_for('main.dashboard'))

    try:
        forecast_data = reports_db.get_shipment_forecast()
    except Exception as e:
        flash(f'An error occurred while generating the forecast: {e}', 'error')
        forecast_data = {
            'month_name': datetime.now().strftime('%B %Y'),
            'likely_total_value': 0,
            'at_risk_total_value': 0,
            'likely_orders': [],
            'at_risk_orders': []
        }

    return render_template(
        'reports/shipment_forecast.html',
        user=session['user'],
        forecast=forecast_data
    )