"""
Active sessions management
Enforces single session per user
"""

from .connection import get_db
from datetime import datetime, timedelta
import secrets

class SessionsDB:
    """Active sessions database operations"""
    
    def __init__(self):
        self.db = get_db()
        self.ensure_table()
    
    def ensure_table(self):
        """Ensure the ActiveSessions table exists"""
        with self.db.get_connection() as conn:
            if not conn.check_table_exists('ActiveSessions'):
                print("Creating ActiveSessions table...")
                create_query = """
                    CREATE TABLE ActiveSessions (
                        session_id NVARCHAR(100) PRIMARY KEY,
                        username NVARCHAR(100) NOT NULL,
                        login_date DATETIME NOT NULL,
                        last_activity DATETIME NOT NULL,
                        ip_address NVARCHAR(50),
                        user_agent NVARCHAR(500),
                        is_active BIT DEFAULT 1
                    );
                    
                    CREATE INDEX IX_ActiveSessions_Username ON ActiveSessions(username);
                    CREATE INDEX IX_ActiveSessions_Active ON ActiveSessions(is_active);
                """
                success = conn.execute_query(create_query)
                if success:
                    print("✅ ActiveSessions table created successfully")
                return success
            return True
    
    def generate_session_id(self):
        """Generate a unique session ID"""
        return secrets.token_urlsafe(32)
    
    def get_active_session(self, username):
        """Check if user has an active session"""
        with self.db.get_connection() as conn:
            query = """
                SELECT session_id, login_date, ip_address, last_activity
                FROM ActiveSessions 
                WHERE username = ? AND is_active = 1
            """
            results = conn.execute_query(query, (username,))
            return results[0] if results else None
    
    def invalidate_user_sessions(self, username):
        """Invalidate all sessions for a user"""
        with self.db.get_connection() as conn:
            query = """
                UPDATE ActiveSessions 
                SET is_active = 0 
                WHERE username = ? AND is_active = 1
            """
            return conn.execute_query(query, (username,))
    
    def create_session(self, session_id, username, ip=None, user_agent=None):
        """Create a new active session"""
        with self.db.get_connection() as conn:
            # First, invalidate any existing sessions
            self.invalidate_user_sessions(username)
            
            # Create new session
            insert_query = """
                INSERT INTO ActiveSessions 
                (session_id, username, login_date, last_activity, ip_address, user_agent, is_active)
                VALUES (?, ?, GETDATE(), GETDATE(), ?, ?, 1)
            """
            return conn.execute_query(insert_query, (session_id, username, ip, user_agent))
    
    def validate_session(self, session_id, username):
        """Validate if a session is still active"""
        with self.db.get_connection() as conn:
            query = """
                SELECT session_id, last_activity
                FROM ActiveSessions 
                WHERE session_id = ? AND username = ? AND is_active = 1
            """
            results = conn.execute_query(query, (session_id, username))
            
            if results:
                # Update last activity
                update_query = """
                    UPDATE ActiveSessions 
                    SET last_activity = GETDATE() 
                    WHERE session_id = ?
                """
                conn.execute_query(update_query, (session_id,))
                return True
            return False
    
    def end_session(self, session_id):
        """End a session (logout)"""
        with self.db.get_connection() as conn:
            query = """
                UPDATE ActiveSessions 
                SET is_active = 0 
                WHERE session_id = ?
            """
            return conn.execute_query(query, (session_id,))
    
    def cleanup_old_sessions(self, hours=24):
        """Clean up sessions older than specified hours"""
        with self.db.get_connection() as conn:
            query = """
                UPDATE ActiveSessions 
                SET is_active = 0 
                WHERE last_activity < DATEADD(hour, ?, GETDATE()) 
                AND is_active = 1
            """
            return conn.execute_query(query, (-hours,))
    
    def get_active_sessions_count(self):
        """Get count of active sessions"""
        with self.db.get_connection() as conn:
            query = """
                SELECT COUNT(*) as count 
                FROM ActiveSessions 
                WHERE is_active = 1
            """
            result = conn.execute_query(query)
            return result[0]['count'] if result else 0