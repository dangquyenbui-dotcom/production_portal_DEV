"""
Users database operations
Tracks user logins and activity
INCLUDES USER PREFERENCES FOR LANGUAGE SELECTION
"""

from .connection import get_db
from datetime import datetime, timedelta

class UsersDB:
    """Users database operations"""
    
    def __init__(self):
        self.db = get_db()
    
    def ensure_table(self):
        """Ensure the UserLogins table exists"""
        with self.db.get_connection() as conn:
            if not conn.check_table_exists('UserLogins'):
                print("Creating UserLogins table...")
                create_query = """
                    CREATE TABLE UserLogins (
                        login_id INT IDENTITY(1,1) PRIMARY KEY,
                        username NVARCHAR(100) NOT NULL,
                        display_name NVARCHAR(200),
                        email NVARCHAR(200),
                        ad_groups NVARCHAR(MAX),
                        is_admin BIT DEFAULT 0,
                        login_date DATETIME NOT NULL,
                        ip_address NVARCHAR(50),
                        user_agent NVARCHAR(500)
                    );
                    
                    CREATE INDEX IX_UserLogins_Username ON UserLogins(username);
                    CREATE INDEX IX_UserLogins_Date ON UserLogins(login_date DESC);
                """
                success = conn.execute_query(create_query)
                if success:
                    print("✅ UserLogins table created successfully")
                return success
            return True
    
    def ensure_preferences_table(self):
        """Ensure the UserPreferences table exists"""
        with self.db.get_connection() as conn:
            if not conn.check_table_exists('UserPreferences'):
                print("Creating UserPreferences table...")
                create_query = """
                    CREATE TABLE UserPreferences (
                        preference_id INT IDENTITY(1,1) PRIMARY KEY,
                        username NVARCHAR(100) NOT NULL,
                        preference_key NVARCHAR(50) NOT NULL,
                        preference_value NVARCHAR(500),
                        created_date DATETIME DEFAULT GETDATE(),
                        modified_date DATETIME DEFAULT GETDATE(),
                        CONSTRAINT UQ_UserPref UNIQUE(username, preference_key)
                    );
                    
                    CREATE INDEX IX_UserPreferences_Username ON UserPreferences(username);
                """
                success = conn.execute_query(create_query)
                if success:
                    print("✅ UserPreferences table created successfully")
                return success
            return True

    def log_login(self, username, display_name, email, groups, is_admin, ip=None, user_agent=None):
        """Log a user login event"""
        with self.db.get_connection() as conn:
            # Ensure table exists
            self.ensure_table()
            
            # Convert groups list to comma-separated string
            groups_str = ','.join(groups) if groups else ''
            
            insert_query = """
                INSERT INTO UserLogins (username, display_name, email, ad_groups, 
                                      is_admin, login_date, ip_address, user_agent)
                VALUES (?, ?, ?, ?, ?, GETDATE(), ?, ?)
            """
            
            return conn.execute_query(insert_query, (
                username, display_name, email, groups_str,
                1 if is_admin else 0, ip, user_agent
            ))

    def get_user_preference(self, username, preference_key):
        """Get a specific preference for a user"""
        with self.db.get_connection() as conn:
            # Ensure table exists
            self.ensure_preferences_table()
            
            query = """
                SELECT preference_value 
                FROM UserPreferences 
                WHERE username = ? AND preference_key = ?
            """
            result = conn.execute_query(query, (username, preference_key))
            return result[0]['preference_value'] if result else None

    def set_user_preference(self, username, preference_key, preference_value):
        """Set or update a user preference"""
        with self.db.get_connection() as conn:
            # Ensure table exists
            self.ensure_preferences_table()
            
            # Use MERGE for upsert operation
            query = """
                MERGE UserPreferences AS target
                USING (SELECT ? AS username, ? AS preference_key, ? AS preference_value) AS source
                ON (target.username = source.username AND target.preference_key = source.preference_key)
                WHEN MATCHED THEN
                    UPDATE SET preference_value = source.preference_value, 
                              modified_date = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (username, preference_key, preference_value)
                    VALUES (source.username, source.preference_key, source.preference_value);
            """
            
            return conn.execute_query(query, (username, preference_key, preference_value))

    def get_all_user_preferences(self, username):
        """Get all preferences for a user"""
        with self.db.get_connection() as conn:
            # Ensure table exists
            self.ensure_preferences_table()
            
            query = """
                SELECT preference_key, preference_value, modified_date
                FROM UserPreferences 
                WHERE username = ?
                ORDER BY preference_key
            """
            results = conn.execute_query(query, (username,))
            
            # Convert to dictionary
            preferences = {}
            for row in results:
                preferences[row['preference_key']] = row['preference_value']
            
            return preferences
    
    def get_user_summary(self):
        """Get summary of all users who have logged in"""
        with self.db.get_connection() as conn:
            # Ensure table exists
            self.ensure_table()
            
            # SQL Server 2012 compatible query (no STRING_AGG)
            query = """
                SELECT 
                    username,
                    MAX(display_name) as display_name,
                    MAX(email) as email,
                    MAX(CAST(is_admin as INT)) as is_admin,
                    COUNT(*) as login_count,
                    MIN(login_date) as first_login,
                    MAX(login_date) as last_login,
                    MAX(ip_address) as last_ip,
                    CASE 
                        WHEN MAX(CAST(is_admin as INT)) = 1 THEN 'Admin'
                        ELSE 'User'
                    END as access_level
                FROM UserLogins
                GROUP BY username
                ORDER BY last_login DESC
            """
            
            return conn.execute_query(query)
    
    def get_user_activity(self, username, days=30):
        """Get activity history for a specific user"""
        with self.db.get_connection() as conn:
            # Ensure table exists
            self.ensure_table()
            
            query = """
                SELECT 
                    login_id,
                    login_date,
                    ip_address,
                    user_agent,
                    ad_groups
                FROM UserLogins
                WHERE username = ?
                  AND login_date >= DATEADD(day, ?, GETDATE())
                ORDER BY login_date DESC
            """
            
            return conn.execute_query(query, (username, -days))
    
    def get_recent_logins(self, hours=24):
        """Get users who logged in recently"""
        with self.db.get_connection() as conn:
            # Ensure table exists
            self.ensure_table()
            
            query = """
                SELECT 
                    username,
                    display_name,
                    email,
                    is_admin,
                    login_date,
                    ip_address
                FROM UserLogins
                WHERE login_date >= DATEADD(hour, ?, GETDATE())
                ORDER BY login_date DESC
            """
            
            return conn.execute_query(query, (-hours,))
    
    def get_login_statistics(self):
        """Get login statistics"""
        with self.db.get_connection() as conn:
            # Ensure table exists
            self.ensure_table()
            
            stats = {}
            
            # Total unique users
            query = "SELECT COUNT(DISTINCT username) as total FROM UserLogins"
            result = conn.execute_query(query)
            stats['total_users'] = result[0]['total'] if result else 0
            
            # Active today
            query = """
                SELECT COUNT(DISTINCT username) as total 
                FROM UserLogins 
                WHERE login_date >= CAST(GETDATE() AS DATE)
            """
            result = conn.execute_query(query)
            stats['active_today'] = result[0]['total'] if result else 0
            
            # Active this week
            query = """
                SELECT COUNT(DISTINCT username) as total 
                FROM UserLogins 
                WHERE login_date >= DATEADD(day, -7, GETDATE())
            """
            result = conn.execute_query(query)
            stats['active_week'] = result[0]['total'] if result else 0
            
            # Admin vs User count
            query = """
                SELECT 
                    SUM(CASE WHEN is_admin = 1 THEN 1 ELSE 0 END) as admin_count,
                    SUM(CASE WHEN is_admin = 0 THEN 1 ELSE 0 END) as user_count
                FROM (
                    SELECT username, MAX(CAST(is_admin as INT)) as is_admin
                    FROM UserLogins
                    GROUP BY username
                ) as users
            """
            result = conn.execute_query(query)
            if result:
                stats['admin_count'] = result[0]['admin_count'] or 0
                stats['user_count'] = result[0]['user_count'] or 0
            else:
                stats['admin_count'] = 0
                stats['user_count'] = 0
            
            # Daily login trends (last 7 days)
            query = """
                SELECT 
                    CAST(login_date AS DATE) as login_day,
                    COUNT(DISTINCT username) as unique_users,
                    COUNT(*) as total_logins
                FROM UserLogins
                WHERE login_date >= DATEADD(day, -7, GETDATE())
                GROUP BY CAST(login_date AS DATE)
                ORDER BY login_day DESC
            """
            stats['daily_trends'] = conn.execute_query(query)
            
            return stats
    
    def get_user_details(self, username):
        """Get detailed information about a specific user"""
        with self.db.get_connection() as conn:
            # User info from logins
            query = """
                SELECT TOP 1
                    username,
                    display_name,
                    email,
                    ad_groups,
                    is_admin
                FROM UserLogins
                WHERE username = ?
                ORDER BY login_date DESC
            """
            user_info = conn.execute_query(query, (username,))
            
            if not user_info:
                return None
            
            user = user_info[0]
            
            # Login statistics
            stats_query = """
                SELECT 
                    COUNT(*) as total_logins,
                    MIN(login_date) as first_login,
                    MAX(login_date) as last_login,
                    COUNT(DISTINCT CAST(login_date AS DATE)) as days_active,
                    COUNT(DISTINCT ip_address) as unique_ips
                FROM UserLogins
                WHERE username = ?
            """
            stats = conn.execute_query(stats_query, (username,))
            
            if stats:
                user.update(stats[0])
            
            # Activity from audit log
            if conn.check_table_exists('AuditLog'):
                audit_query = """
                    SELECT 
                        COUNT(*) as total_changes,
                        COUNT(DISTINCT table_name) as tables_modified,
                        MAX(changed_date) as last_change
                    FROM AuditLog
                    WHERE changed_by = ?
                """
                audit_stats = conn.execute_query(audit_query, (username,))
                if audit_stats:
                    user['audit_stats'] = audit_stats[0]
            
            # Downtime entries
            if conn.check_table_exists('Downtimes'):
                downtime_query = """
                    SELECT 
                        COUNT(*) as total_entries,
                        SUM(duration_minutes) as total_minutes_logged,
                        MAX(entered_date) as last_entry
                    FROM Downtimes
                    WHERE entered_by = ?
                    AND is_deleted = 0
                """
                downtime_stats = conn.execute_query(downtime_query, (username,))
                if downtime_stats:
                    user['downtime_stats'] = downtime_stats[0]
            
            return user
    
    def search_users(self, search_term):
        """Search for users by username, display name, or email"""
        with self.db.get_connection() as conn:
            # Ensure table exists
            self.ensure_table()
            
            query = """
                SELECT DISTINCT
                    username,
                    display_name,
                    email,
                    MAX(CAST(is_admin as INT)) as is_admin,
                    MAX(login_date) as last_login
                FROM UserLogins
                WHERE username LIKE ? 
                   OR display_name LIKE ?
                   OR email LIKE ?
                GROUP BY username, display_name, email
                ORDER BY last_login DESC
            """
            
            search_pattern = f'%{search_term}%'
            return conn.execute_query(query, (search_pattern, search_pattern, search_pattern))