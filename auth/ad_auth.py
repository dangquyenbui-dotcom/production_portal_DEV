"""
Active Directory Authentication Module
Handles user authentication and authorization
"""

from ldap3 import Server, Connection, ALL, SIMPLE, SUBTREE
import ldap3.core.exceptions
from config import Config

def get_user_groups(username):
    """Get AD groups for a user using the service account"""
    try:
        server = Server(Config.AD_SERVER, port=Config.AD_PORT, get_info=ALL)
        service_user = f'{Config.AD_SERVICE_ACCOUNT}@{Config.AD_DOMAIN}'
        
        service_conn = Connection(
            server,
            user=service_user,
            password=Config.AD_SERVICE_PASSWORD,
            authentication=SIMPLE,
            auto_bind=True
        )
        
        search_filter = f'(&(objectClass=user)(sAMAccountName={username}))'
        service_conn.search(
            Config.AD_BASE_DN,
            search_filter,
            SUBTREE,
            attributes=['memberOf', 'displayName', 'mail', 'distinguishedName', 'sAMAccountName']
        )
        
        if service_conn.entries:
            user_entry = service_conn.entries[0]
            
            groups = []
            if hasattr(user_entry, 'memberOf') and user_entry.memberOf:
                for group_dn in user_entry.memberOf:
                    group_name = str(group_dn).split(',')[0].replace('CN=', '')
                    groups.append(group_name)
            
            display_name = str(user_entry.displayName) if hasattr(user_entry, 'displayName') else username
            email = str(user_entry.mail) if hasattr(user_entry, 'mail') else f'{username}@{Config.AD_DOMAIN}'
            
            service_conn.unbind()
            
            return {
                'groups': groups,
                'display_name': display_name,
                'email': email
            }
        
        service_conn.unbind()
        return None
        
    except Exception as e:
        print(f"Error getting user groups: {str(e)}")
        return None

def authenticate_user(username, password):
    """Authenticate user against Active Directory"""
    
    # Test mode for development
    if Config.TEST_MODE:
        test_users = {
            'dt_admin': {
                'password': 'password', 'display_name': 'Downtime Admin',
                'groups': [Config.AD_ADMIN_GROUP]
            },
            'dt_user': {
                'password': 'password', 'display_name': 'Downtime User',
                'groups': [Config.AD_USER_GROUP]
            },
            'sched_admin': {
                'password': 'password', 'display_name': 'Scheduling Admin',
                'groups': [Config.AD_SCHEDULING_ADMIN_GROUP]
            },
            'sched_user': {
                'password': 'password', 'display_name': 'Scheduling User',
                'groups': [Config.AD_SCHEDULING_USER_GROUP]
            },
            'super_admin': {
                'password': 'password', 'display_name': 'Super Admin',
                'groups': [Config.AD_ADMIN_GROUP, Config.AD_SCHEDULING_ADMIN_GROUP]
            }
        }
        
        if username in test_users and test_users[username]['password'] == password:
            user = test_users[username]
            is_admin = Config.AD_ADMIN_GROUP in user['groups']
            is_user = Config.AD_USER_GROUP in user['groups']
            is_scheduling_admin = Config.AD_SCHEDULING_ADMIN_GROUP in user['groups']
            is_scheduling_user = Config.AD_SCHEDULING_USER_GROUP in user['groups']

            # User must be in at least one group to be valid
            if not any([is_admin, is_user, is_scheduling_admin, is_scheduling_user]):
                return None
                
            return {
                'username': username,
                'display_name': user['display_name'],
                'email': f'{username}@{Config.AD_DOMAIN}',
                'groups': user['groups'],
                'is_admin': is_admin,
                'is_user': is_user,
                'is_scheduling_admin': is_scheduling_admin,
                'is_scheduling_user': is_scheduling_user,
            }
        return None
    
    # Real AD Authentication
    try:
        server = Server(Config.AD_SERVER, port=Config.AD_PORT, get_info=ALL)
        user_principal = f'{username}@{Config.AD_DOMAIN}'
        
        try:
            # Attempt user authentication
            user_conn = Connection(
                server,
                user=user_principal,
                password=password,
                authentication=SIMPLE,
                auto_bind=True
            )
            user_conn.unbind()
            
            # Get user groups
            user_info = get_user_groups(username)
            
            if user_info:
                is_in_admin = Config.AD_ADMIN_GROUP in user_info['groups']
                is_in_user = Config.AD_USER_GROUP in user_info['groups']
                is_in_scheduling_admin = Config.AD_SCHEDULING_ADMIN_GROUP in user_info['groups']
                is_in_scheduling_user = Config.AD_SCHEDULING_USER_GROUP in user_info['groups']
                
                # User must be in at least one of the groups
                if not any([is_in_admin, is_in_user, is_in_scheduling_admin, is_in_scheduling_user]):
                    print(f"User {username} not in required groups")
                    return None
                
                return {
                    'username': username,
                    'display_name': user_info['display_name'],
                    'email': user_info['email'],
                    'groups': user_info['groups'],
                    'is_admin': is_in_admin,
                    'is_user': is_in_user,
                    'is_scheduling_admin': is_in_scheduling_admin,
                    'is_scheduling_user': is_in_scheduling_user
                }
                
        except ldap3.core.exceptions.LDAPBindError:
            print(f"Invalid credentials for user: {username}")
            return None
            
    except Exception as e:
        print(f"AD Authentication error: {str(e)}")
        return None
    
    return None

def require_login(session):
    """Check if user is logged in"""
    return 'user' in session

def require_admin(session):
    """Check if user is a DowntimeTracker_Admin"""
    if 'user' in session:
        return session['user'].get('is_admin', False)
    return False

def require_user(session):
    """Check if user is a DowntimeTracker_User"""
    if 'user' in session:
        return session['user'].get('is_user', False)
    return False

def require_scheduling_admin(session):
    """Check if user is a Scheduling_Admin"""
    if 'user' in session:
        return session['user'].get('is_scheduling_admin', False)
    return False

def require_scheduling_user(session):
    """Check if user is a Scheduling_User"""
    if 'user' in session:
        return session['user'].get('is_scheduling_user', False)
    return False

def test_ad_connection():
    """Test Active Directory connection"""
    if Config.TEST_MODE:
        return True
    
    try:
        server = Server(Config.AD_SERVER, port=Config.AD_PORT, get_info=ALL)
        service_user = f'{Config.AD_SERVICE_ACCOUNT}@{Config.AD_DOMAIN}'
        
        conn = Connection(
            server,
            user=service_user,
            password=Config.AD_SERVICE_PASSWORD,
            authentication=SIMPLE,
            auto_bind=True
        )
        conn.unbind()
        return True
    except Exception as e:
        print(f"AD connection test failed: {str(e)}")
        return False