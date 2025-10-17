"""
Audit logging database operations
Tracks all changes to the system
"""

from .connection import get_db
from datetime import datetime

class AuditDB:
    """Audit logging database operations"""
    
    def __init__(self):
        self.db = get_db()
        self.audit_enabled = True
    
    def ensure_table(self):
        """Ensure the AuditLog table exists"""
        with self.db.get_connection() as conn:
            if not conn.check_table_exists('AuditLog'):
                print("Creating AuditLog table...")
                create_query = """
                    CREATE TABLE AuditLog (
                        audit_id INT IDENTITY(1,1) PRIMARY KEY,
                        table_name NVARCHAR(100) NOT NULL,
                        record_id INT,
                        action_type NVARCHAR(50) NOT NULL,
                        field_name NVARCHAR(100),
                        old_value NVARCHAR(MAX),
                        new_value NVARCHAR(MAX),
                        changed_by NVARCHAR(100) NOT NULL,
                        changed_date DATETIME NOT NULL DEFAULT GETDATE(),
                        user_ip NVARCHAR(50),
                        user_agent NVARCHAR(500),
                        additional_notes NVARCHAR(MAX)
                    );
                    
                    CREATE INDEX IX_AuditLog_Table ON AuditLog(table_name, record_id);
                    CREATE INDEX IX_AuditLog_Date ON AuditLog(changed_date DESC);
                    CREATE INDEX IX_AuditLog_User ON AuditLog(changed_by);
                """
                success = conn.execute_query(create_query)
                if success:
                    print("✅ AuditLog table created successfully")
                return success
            return True
    
    def log(self, table_name, record_id, action_type, changes=None, 
            username=None, ip=None, user_agent=None, notes=None):
        """
        Log an audit entry
        
        Args:
            table_name: Name of the table being modified
            record_id: ID of the record being modified
            action_type: INSERT, UPDATE, DELETE, DEACTIVATE, REACTIVATE
            changes: Dict of {field_name: {'old': old_value, 'new': new_value}}
            username: User making the change
            ip: User's IP address
            user_agent: User's browser info
            notes: Additional notes
        
        Returns:
            bool: Success status
        """
        if not self.audit_enabled:
            return True
        
        with self.db.get_connection() as conn:
            # Ensure table exists
            self.ensure_table()
            
            try:
                if changes:
                    # Log each field change separately
                    for field_name, values in changes.items():
                        old_value = str(values.get('old', '')) if values.get('old') is not None else None
                        new_value = str(values.get('new', '')) if values.get('new') is not None else None
                        
                        query = """
                            INSERT INTO AuditLog (
                                table_name, record_id, action_type, field_name,
                                old_value, new_value, changed_by, changed_date,
                                user_ip, user_agent, additional_notes
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE(), ?, ?, ?)
                        """
                        
                        conn.execute_query(query, (
                            table_name, record_id, action_type, field_name,
                            old_value, new_value, username or 'system',
                            ip, user_agent, notes
                        ))
                else:
                    # For actions without field changes
                    query = """
                        INSERT INTO AuditLog (
                            table_name, record_id, action_type, changed_by, 
                            changed_date, user_ip, user_agent, additional_notes
                        ) VALUES (?, ?, ?, ?, GETDATE(), ?, ?, ?)
                    """
                    
                    conn.execute_query(query, (
                        table_name, record_id, action_type, 
                        username or 'system', ip, user_agent, notes
                    ))
                
                print(f"✅ Audit logged: {action_type} on {table_name} ID {record_id} by {username}")
                return True
                
            except Exception as e:
                print(f"❌ Audit logging failed: {str(e)}")
                return False
    
    def get_history(self, table_name=None, record_id=None, username=None, days=30):
        """
        Get audit history with filters
        
        Args:
            table_name: Filter by table
            record_id: Filter by record ID
            username: Filter by user
            days: Number of days to look back
        
        Returns:
            list: Audit log entries
        """
        with self.db.get_connection() as conn:
            # Ensure table exists
            if not conn.check_table_exists('AuditLog'):
                self.ensure_table()
                return []
            
            query = """
                SELECT 
                    audit_id,
                    table_name,
                    record_id,
                    action_type,
                    field_name,
                    old_value,
                    new_value,
                    changed_by,
                    changed_date,
                    user_ip,
                    additional_notes
                FROM AuditLog
                WHERE changed_date >= DATEADD(day, ?, GETDATE())
            """
            
            params = [-days]
            
            if table_name:
                query += " AND table_name = ?"
                params.append(table_name)
            
            if record_id:
                query += " AND record_id = ?"
                params.append(record_id)
            
            if username:
                query += " AND changed_by = ?"
                params.append(username)
            
            query += " ORDER BY changed_date DESC"
            
            return conn.execute_query(query, params)
    
    def get_record_history(self, table_name, record_id):
        """Get complete history for a specific record"""
        with self.db.get_connection() as conn:
            if not conn.check_table_exists('AuditLog'):
                self.ensure_table()
                return []
            
            query = """
                SELECT 
                    audit_id,
                    action_type,
                    field_name,
                    old_value,
                    new_value,
                    changed_by,
                    changed_date,
                    CASE 
                        WHEN field_name = 'is_active' AND new_value = '0' THEN 'Deactivated'
                        WHEN field_name = 'is_active' AND new_value = '1' THEN 'Reactivated'
                        WHEN action_type = 'INSERT' THEN 'Created'
                        WHEN action_type = 'UPDATE' THEN 'Modified'
                        ELSE action_type
                    END as action_description
                FROM AuditLog
                WHERE table_name = ? AND record_id = ?
                ORDER BY changed_date DESC
            """
            
            return conn.execute_query(query, (table_name, record_id))
    
    def get_user_activity(self, username, days=7):
        """Get all activity for a specific user"""
        with self.db.get_connection() as conn:
            if not conn.check_table_exists('AuditLog'):
                return []
            
            query = """
                SELECT 
                    audit_id,
                    table_name,
                    record_id,
                    action_type,
                    field_name,
                    changed_date,
                    additional_notes
                FROM AuditLog
                WHERE changed_by = ? 
                  AND changed_date >= DATEADD(day, ?, GETDATE())
                ORDER BY changed_date DESC
            """
            
            return conn.execute_query(query, (username, -days))
    
    def get_statistics(self, days=30):
        """Get audit statistics"""
        with self.db.get_connection() as conn:
            if not conn.check_table_exists('AuditLog'):
                return {}
            
            stats = {}
            
            # Total changes
            query = """
                SELECT COUNT(*) as total
                FROM AuditLog
                WHERE changed_date >= DATEADD(day, ?, GETDATE())
            """
            result = conn.execute_query(query, (-days,))
            stats['total_changes'] = result[0]['total'] if result else 0
            
            # Changes by table
            query = """
                SELECT table_name, COUNT(*) as count
                FROM AuditLog
                WHERE changed_date >= DATEADD(day, ?, GETDATE())
                GROUP BY table_name
                ORDER BY count DESC
            """
            stats['by_table'] = conn.execute_query(query, (-days,))
            
            # Changes by action
            query = """
                SELECT action_type, COUNT(*) as count
                FROM AuditLog
                WHERE changed_date >= DATEADD(day, ?, GETDATE())
                GROUP BY action_type
                ORDER BY count DESC
            """
            stats['by_action'] = conn.execute_query(query, (-days,))
            
            # Most active users
            query = """
                SELECT TOP 10 changed_by, COUNT(*) as count
                FROM AuditLog
                WHERE changed_date >= DATEADD(day, ?, GETDATE())
                GROUP BY changed_by
                ORDER BY count DESC
            """
            stats['top_users'] = conn.execute_query(query, (-days,))
            
            return stats