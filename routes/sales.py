"""
Routes for Sales-facing analytics and dashboards.
"""

from flask import Blueprint, render_template, request, session, redirect, url_for, flash
from auth import require_login
from routes.main import validate_session
from database.sales_service import sales_service

sales_bp = Blueprint('sales', __name__, url_prefix='/sales')

@sales_bp.route('/customer-analysis')
@validate_session
def customer_analysis():
    """Renders the main Customer Analysis page."""
    if not require_login(session):
        return redirect(url_for('main.login'))

    try:
        customers = sales_service.get_all_customers()
        selected_customer = request.args.get('customer')
        
        analysis_data = None
        if selected_customer:
            analysis_data = sales_service.get_customer_analysis(selected_customer)

    except Exception as e:
        flash(f'An error occurred while fetching customer data: {e}', 'error')
        customers = []
        analysis_data = None
        selected_customer = None

    return render_template(
        'sales/customer_analysis.html',
        user=session['user'],
        customers=customers,
        selected_customer=selected_customer,
        analysis_data=analysis_data
    )