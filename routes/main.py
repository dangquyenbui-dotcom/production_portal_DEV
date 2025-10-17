"""
Main routes for Production Portal
Complete with i18n translation support
"""

import os
from flask import Blueprint, render_template, redirect, url_for, session, request, flash, jsonify
from functools import wraps

# Import authentication
from auth import authenticate_user, require_login, require_admin, test_ad_connection
from config import Config

# Import database modules
from database import facilities_db, lines_db, categories_db, downtimes_db, sessions_db
from database.connection import DatabaseConnection

# Import i18n
from i18n_config import I18nConfig, _

main_bp = Blueprint('main', __name__)

def validate_session(f):
    """Decorator to validate session on each request"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' in session and 'session_id' in session:
            # Validate the session is still active
            if not sessions_db.validate_session(session['session_id'], session['user']['username']):
                session.clear()
                flash(_('Your session has expired or you logged in from another location'), 'error')
                return redirect(url_for('main.login'))
        return f(*args, **kwargs)
    return decorated_function

@main_bp.route('/')
def index():
    if 'user' in session:
        return redirect(url_for('main.dashboard'))
    return redirect(url_for('main.login'))

@main_bp.route('/switch-language/<language>')
def switch_language(language):
    """Switch the user interface language"""
    if I18nConfig.switch_language(language):
        flash(_('Language changed successfully'), 'success')
    else:
        flash(_('Invalid language selection'), 'error')
    
    # Redirect to the referrer or dashboard
    referrer = request.referrer
    if referrer:
        return redirect(referrer)
    return redirect(url_for('main.dashboard'))

@main_bp.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        
        if username and password:
            user_info = authenticate_user(username, password)
            if user_info:
                # Check for existing active session
                existing_session = sessions_db.get_active_session(username)
                
                if existing_session:
                    # Show warning about existing session
                    from datetime import datetime
                    last_activity = existing_session.get('last_activity')
                    ip = existing_session.get('ip_address', 'Unknown')
                    
                    # Calculate time since last activity
                    if last_activity:
                        time_diff = datetime.now() - last_activity
                        minutes_ago = int(time_diff.total_seconds() / 60)
                        
                        if minutes_ago < 1:
                            time_str = _("just now")
                        elif minutes_ago == 1:
                            time_str = _("1 minute ago")
                        elif minutes_ago < 60:
                            time_str = f"{minutes_ago} " + _("minutes ago")
                        else:
                            hours_ago = minutes_ago // 60
                            if hours_ago == 1:
                                time_str = _("1 hour ago")
                            else:
                                time_str = f"{hours_ago} " + _("hours ago")
                    else:
                        time_str = _("just now")
                    
                    # If this is an AJAX request (from confirmation dialog)
                    if request.form.get('force_login') == 'true':
                        # User confirmed they want to proceed
                        pass  # Continue with login below
                    else:
                        # Return info about existing session for confirmation
                        message = _('You have an active session from {ip} ({time}). Logging in here will end that session.').format(
                            ip=ip,
                            time=time_str
                        )
                        return jsonify({
                            'existing_session': True,
                            'message': message,
                            'last_ip': ip,
                            'last_activity': time_str
                        })
                
                # Generate new session ID
                new_session_id = sessions_db.generate_session_id()
                
                # Create session in database (this will invalidate old sessions)
                from utils import get_client_info
                ip, user_agent = get_client_info()
                sessions_db.create_session(new_session_id, username, ip, user_agent)
                
                # Set Flask session
                session.permanent = True
                session['user'] = user_info
                session['session_id'] = new_session_id
                
                print(f"‚úÖ User {username} logged in successfully (session: {new_session_id[:8]}...)")
                
                # Log the login event
                try:
                    from database.users import UsersDB
                    users_db = UsersDB()
                    users_db.log_login(
                        username=user_info['username'],
                        display_name=user_info.get('display_name'),
                        email=user_info.get('email'),
                        groups=user_info.get('groups', []),
                        is_admin=user_info.get('is_admin', False),
                        ip=ip,
                        user_agent=user_agent
                    )
                except Exception as e:
                    print(f"Failed to log login event: {str(e)}")
                
                # Return success for AJAX or redirect for normal POST
                if request.form.get('force_login') == 'true':
                    return jsonify({'success': True, 'redirect': url_for('main.dashboard')})
                else:
                    return redirect(url_for('main.dashboard'))
            else:
                if request.form.get('force_login'):
                    return jsonify({'success': False, 'message': _('Invalid credentials or access denied')})
                else:
                    flash(_('Invalid credentials or access denied'), 'error')
                    print(f"‚ùå Login failed for user: {username}")
        else:
            if request.form.get('force_login'):
                return jsonify({'success': False, 'message': _('Please enter both username and password')})
            else:
                flash(_('Please enter both username and password'), 'error')
    
    return render_template('login.html', config=Config)

@main_bp.route('/dashboard')
@validate_session
def dashboard():
    if not require_login(session):
        return redirect(url_for('main.login'))
    
    # Get real statistics from database
    stats = {
        'facilities': 0,
        'production_lines': 0,
        'recent_downtime_count': 0,
        'categories': 0
    }
    
    try:
        # Get counts from database
        facilities = facilities_db.get_all(active_only=True)
        stats['facilities'] = len(facilities) if facilities else 0
        
        lines = lines_db.get_all(active_only=True)
        stats['production_lines'] = len(lines) if lines else 0
        
        categories = categories_db.get_all(active_only=True)
        stats['categories'] = len(categories) if categories else 0
        
        recent_downtimes = downtimes_db.get_recent(days=7)
        stats['recent_downtime_count'] = len(recent_downtimes) if recent_downtimes else 0
    except Exception as e:
        print(f"Error getting dashboard stats: {str(e)}")
    
    return render_template('dashboard.html', 
                         user=session['user'], 
                         stats=stats, 
                         config=Config)

@main_bp.route('/logout')
def logout():
    username = session.get('user', {}).get('username', 'Unknown')
    session_id = session.get('session_id')
    
    # End the session in database
    if session_id:
        sessions_db.end_session(session_id)
    
    session.clear()
    print(f"User {username} logged out")
    flash(_('You have been successfully logged out'), 'info')
    return redirect(url_for('main.login'))

@main_bp.route('/status')
@validate_session
def status():
    if not require_login(session):
        return redirect(url_for('main.login'))
    
    if not require_admin(session):
        flash(_('Admin privileges required'), 'error')
        return redirect(url_for('main.dashboard'))
    
    status_info = {
        'ad_connected': False,
        'db_connected': False,
        'test_mode': Config.TEST_MODE,
        'facilities_count': 0,
        'lines_count': 0,
        'users_today': 0,
        'active_sessions': 0
    }
    
    # Test connections
    try:
        db = DatabaseConnection()
        status_info['db_connected'] = db.test_connection()
        
        if status_info['db_connected']:
            with db.get_connection() as conn:
                # Get counts
                if conn.check_table_exists('Facilities'):
                    result = conn.execute_query("SELECT COUNT(*) as count FROM Facilities")
                    status_info['facilities_count'] = result[0]['count'] if result else 0
                
                if conn.check_table_exists('ProductionLines'):
                    result = conn.execute_query("SELECT COUNT(*) as count FROM ProductionLines")
                    status_info['lines_count'] = result[0]['count'] if result else 0
                
                # Get active sessions count
                status_info['active_sessions'] = sessions_db.get_active_sessions_count()
        
        status_info['ad_connected'] = test_ad_connection()
    except Exception as e:
        print(f"Error in status check: {str(e)}")
    
    # Simple status page if template doesn't exist
    if os.path.exists('templates/status.html'):
        return render_template('status.html', 
                             user=session['user'], 
                             status=status_info, 
                             config=Config)
    else:
        db_status = '‚úÖ Connected' if status_info['db_connected'] else '‚ùå Disconnected'
        ad_status = '‚úÖ Connected' if status_info['ad_connected'] else '‚ùå Disconnected'
        test_mode = 'Yes' if status_info['test_mode'] else 'No'
        
        return f"""
        <html>
        <body style="font-family: Arial; padding: 20px; background: #f5f5f5;">
            <div style="max-width: 800px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px;">
                <h1>System Status</h1>
                <div>Database: {db_status}</div>
                <div>Active Directory: {ad_status}</div>
                <div>Test Mode: {test_mode}</div>
                <hr>
                <div>Facilities: {status_info['facilities_count']}</div>
                <div>Production Lines: {status_info['lines_count']}</div>
                <div>Active Sessions: {status_info['active_sessions']}</div>
                <p><a href="/dashboard">‚Üê Back to Dashboard</a></p>
            </div>
        </body>
        </html>
        """