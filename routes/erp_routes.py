# routes/erp_routes.py
from flask import Blueprint, jsonify, session
# UPDATED IMPORT:
from database import get_erp_service

erp_bp = Blueprint('erp', __name__, url_prefix='/api/erp')
erp_service = get_erp_service() # This now gets the refactored service instance

@erp_bp.route('/open-jobs/<facility>/<line>')
def get_open_jobs(facility, line):
    """
    API endpoint to get open jobs for a given facility and line.
    Simplified authentication - just checks if user is in session.
    """
    if 'user' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401

    try:
        print(f"🔍 [ERP API] Fetching jobs for {facility}/{line}")
        jobs = erp_service.get_open_jobs_by_line(facility, line) # Call remains the same

        if jobs:
            print(f"✅ [ERP API] Found {len(jobs)} jobs")
            return jsonify({'success': True, 'jobs': jobs})
        else:
            print(f"ℹ️ [ERP API] No jobs found")
            return jsonify({'success': True, 'jobs': []})

    except Exception as e:
        print(f"❌ [ERP API] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': 'Error querying ERP database',
            'error': str(e)
        }), 500