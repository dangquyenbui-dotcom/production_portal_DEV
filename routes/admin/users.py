"""
User management routes for viewing user activity and permissions
"""

from flask import Blueprint, render_template, redirect, url_for, session, jsonify, request, flash
from auth import require_login, require_admin
from routes.main import validate_session
from database import audit_db
from database.users import UsersDB
from utils import get_client_info


admin_users_bp = Blueprint('admin_users', __name__)

# Initialize users database
users_db = UsersDB()

@admin_users_bp.route('/users')
@validate_session
def users():
    """Display users management page"""
    if not require_login(session):
        return redirect(url_for('main.login'))
    
    if not require_admin(session):
        flash('Admin privileges required', 'error')
        return redirect(url_for('main.dashboard'))
    
    # Get user summary and statistics
    user_list = users_db.get_user_summary()
    stats = users_db.get_login_statistics()
    recent_logins = users_db.get_recent_logins(hours=24)
    
    return render_template('admin/users.html', 
                         users=user_list,
                         stats=stats,
                         recent_logins=recent_logins,
                         user=session['user'])

@admin_users_bp.route('/users/details/<username>')
@validate_session
def user_details(username):
    """Get detailed information about a specific user"""
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        user_info = users_db.get_user_details(username)
        if not user_info:
            return jsonify({'success': False, 'message': 'User not found'})
        
        # Get recent activity from audit log
        recent_changes = audit_db.get_user_activity(username, days=7)
        
        # Get login history
        login_history = users_db.get_user_activity(username, days=30)
        
        return jsonify({
            'success': True,
            'user': user_info,
            'recent_changes': recent_changes,
            'login_history': login_history
        })
        
    except Exception as e:
        print(f"Error getting user details: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred'})

@admin_users_bp.route('/users/activity/<username>')
@validate_session
def user_activity(username):
    """Get activity history for a user"""
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        # Get activity from various sources
        login_history = users_db.get_user_activity(username, days=30)
        audit_history = audit_db.get_user_activity(username, days=30)
        
        # Combine and sort by date
        activity = []
        
        # Add login events
        for login in login_history:
            activity.append({
                'type': 'login',
                'date': login['login_date'],
                'description': f"Logged in from {login.get('ip_address', 'Unknown IP')}",
                'details': login.get('user_agent', '')
            })
        
        # Add audit events
        for audit in audit_history:
            activity.append({
                'type': 'change',
                'date': audit['changed_date'],
                'description': f"{audit['action_type']} in {audit['table_name']}",
                'details': audit.get('field_name', '')
            })
        
        # Sort by date descending
        activity.sort(key=lambda x: x['date'], reverse=True)
        
        return jsonify({
            'success': True,
            'username': username,
            'activity': activity[:100]  # Limit to 100 most recent
        })
        
    except Exception as e:
        print(f"Error getting user activity: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred'})

@admin_users_bp.route('/users/search')
@validate_session
def search_users():
    """Search for users"""
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    search_term = request.args.get('q', '').strip()
    
    if not search_term:
        return jsonify({'success': False, 'message': 'Search term required'})
    
    try:
        results = users_db.search_users(search_term)
        return jsonify({
            'success': True,
            'results': results,
            'count': len(results)
        })
        
    except Exception as e:
        print(f"Error searching users: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred'})

@admin_users_bp.route('/users/export')
@validate_session
def export_users():
    """Export user list to CSV"""
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        import csv
        from io import StringIO
        from flask import make_response
        
        # Get user data
        users = users_db.get_user_summary()
        
        # Create CSV
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=[
            'username', 'display_name', 'email', 'access_level',
            'login_count', 'first_login', 'last_login', 'last_ip'
        ])
        
        writer.writeheader()
        for user in users:
            writer.writerow({
                'username': user['username'],
                'display_name': user['display_name'],
                'email': user['email'],
                'access_level': user['access_level'],
                'login_count': user['login_count'],
                'first_login': user['first_login'].strftime('%Y-%m-%d %H:%M:%S') if user['first_login'] else '',
                'last_login': user['last_login'].strftime('%Y-%m-%d %H:%M:%S') if user['last_login'] else '',
                'last_ip': user['last_ip']
            })
        
        # Create response
        response = make_response(output.getvalue())
        response.headers["Content-Disposition"] = "attachment; filename=users_export.csv"
        response.headers["Content-type"] = "text/csv"
        
        # Log the export in audit
        ip, user_agent = get_client_info()
        audit_db.log(
            table_name='UserLogins',
            record_id=None,
            action_type='EXPORT',
            username=session['user']['username'],
            ip=ip,
            user_agent=user_agent,
            notes=f"Exported {len(users)} users to CSV"
        )
        
        return response
        
    except Exception as e:
        print(f"Error exporting users: {str(e)}")
        return jsonify({'success': False, 'message': 'Export failed'}), 500

@admin_users_bp.route('/users/stats')
@validate_session
def user_stats():
    """Get user statistics for dashboard"""
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        stats = users_db.get_login_statistics()
        return jsonify({
            'success': True,
            'stats': stats
        })
        
    except Exception as e:
        print(f"Error getting user stats: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred'})