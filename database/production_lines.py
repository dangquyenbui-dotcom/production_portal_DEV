"""
Production lines database operations
Handles all production line-related database interactions
"""

from .connection import get_db
from datetime import datetime

class ProductionLinesDB:
    """Production lines database operations"""
    
    def __init__(self):
        self.db = get_db()
    
    def get_all(self, facility_id=None, active_only=True):
        """Get all production lines, optionally filtered by facility"""
        with self.db.get_connection() as conn:
            # Check which columns exist
            columns_query = """
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'ProductionLines'
            """
            columns_result = conn.execute_query(columns_query)
            existing_columns = [col['COLUMN_NAME'] for col in columns_result] if columns_result else []
            
            # Build select fields based on available columns
            select_fields = ['pl.line_id', 'pl.facility_id', 'pl.line_name', 'pl.is_active', 'f.facility_name']
            if 'line_code' in existing_columns:
                select_fields.insert(3, 'pl.line_code')
            if 'created_date' in existing_columns:
                select_fields.append('pl.created_date')
            if 'created_by' in existing_columns:
                select_fields.append('pl.created_by')
            if 'modified_date' in existing_columns:
                select_fields.append('pl.modified_date')
            if 'modified_by' in existing_columns:
                select_fields.append('pl.modified_by')
            
            fields_str = ', '.join(select_fields)
            
            if facility_id:
                if active_only:
                    query = f"""
                        SELECT {fields_str}
                        FROM ProductionLines pl
                        JOIN Facilities f ON pl.facility_id = f.facility_id
                        WHERE pl.facility_id = ? AND pl.is_active = 1
                        ORDER BY pl.line_name
                    """
                    params = (facility_id,)
                else:
                    query = f"""
                        SELECT {fields_str}
                        FROM ProductionLines pl
                        JOIN Facilities f ON pl.facility_id = f.facility_id
                        WHERE pl.facility_id = ?
                        ORDER BY pl.is_active DESC, pl.line_name
                    """
                    params = (facility_id,)
            else:
                if active_only:
                    query = f"""
                        SELECT {fields_str}
                        FROM ProductionLines pl
                        JOIN Facilities f ON pl.facility_id = f.facility_id
                        WHERE pl.is_active = 1
                        ORDER BY f.facility_name, pl.line_name
                    """
                    params = None
                else:
                    query = f"""
                        SELECT {fields_str}
                        FROM ProductionLines pl
                        JOIN Facilities f ON pl.facility_id = f.facility_id
                        ORDER BY pl.is_active DESC, f.facility_name, pl.line_name
                    """
                    params = None
            
            return conn.execute_query(query, params)
    
    def get_by_id(self, line_id):
        """Get production line by ID"""
        with self.db.get_connection() as conn:
            query = """
                SELECT pl.*, f.facility_name
                FROM ProductionLines pl
                JOIN Facilities f ON pl.facility_id = f.facility_id
                WHERE pl.line_id = ?
            """
            results = conn.execute_query(query, (line_id,))
            return results[0] if results else None
    
    def create(self, facility_id, line_name, line_code, username):
        """Create new production line"""
        with self.db.get_connection() as conn:
            # Check if line name already exists in this facility
            check_query = """
                SELECT line_id FROM ProductionLines 
                WHERE facility_id = ? AND line_name = ?
            """
            existing = conn.execute_query(check_query, (facility_id, line_name))
            
            if existing:
                return False, "Line name already exists in this facility", None
            
            # Get facility name for logging
            facility_query = "SELECT facility_name FROM Facilities WHERE facility_id = ?"
            facility_result = conn.execute_query(facility_query, (facility_id,))
            facility_name = facility_result[0]['facility_name'] if facility_result else 'Unknown'
            
            # Check which columns exist
            columns_query = """
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'ProductionLines'
            """
            columns_result = conn.execute_query(columns_query)
            existing_columns = [col['COLUMN_NAME'] for col in columns_result] if columns_result else []
            
            # Build INSERT query based on available columns
            if 'created_by' in existing_columns:
                if 'line_code' in existing_columns:
                    insert_query = """
                        INSERT INTO ProductionLines (facility_id, line_name, line_code, is_active, created_by, created_date)
                        VALUES (?, ?, ?, 1, ?, GETDATE())
                    """
                    params = (facility_id, line_name, line_code or None, username)
                else:
                    insert_query = """
                        INSERT INTO ProductionLines (facility_id, line_name, is_active, created_by, created_date)
                        VALUES (?, ?, 1, ?, GETDATE())
                    """
                    params = (facility_id, line_name, username)
            else:
                if 'line_code' in existing_columns:
                    insert_query = """
                        INSERT INTO ProductionLines (facility_id, line_name, line_code, is_active)
                        VALUES (?, ?, ?, 1)
                    """
                    params = (facility_id, line_name, line_code or None)
                else:
                    insert_query = """
                        INSERT INTO ProductionLines (facility_id, line_name, is_active)
                        VALUES (?, ?, 1)
                    """
                    params = (facility_id, line_name)
            
            success = conn.execute_query(insert_query, params)
            
            if success:
                # Get the new line ID
                new_line = conn.execute_query(
                    "SELECT TOP 1 line_id FROM ProductionLines WHERE facility_id = ? AND line_name = ? ORDER BY line_id DESC",
                    (facility_id, line_name)
                )
                line_id = new_line[0]['line_id'] if new_line else None
                return True, f"Production line created in {facility_name}", line_id
            
            return False, "Failed to create production line", None
    
    def update(self, line_id, line_name, line_code, username):
        """Update existing production line"""
        with self.db.get_connection() as conn:
            # Get current record for comparison
            current = self.get_by_id(line_id)
            if not current:
                return False, "Production line not found", None
            
            facility_id = current['facility_id']
            
            # Check if new name conflicts
            check_query = """
                SELECT line_id FROM ProductionLines 
                WHERE facility_id = ? AND line_name = ? AND line_id != ?
            """
            existing = conn.execute_query(check_query, (facility_id, line_name, line_id))
            
            if existing:
                return False, "Line name already exists in this facility", None
            
            # Track changes for audit
            changes = {}
            if current.get('line_name') != line_name:
                changes['line_name'] = {'old': current.get('line_name'), 'new': line_name}
            
            # Check which columns exist
            columns_query = """
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'ProductionLines'
            """
            columns_result = conn.execute_query(columns_query)
            existing_columns = [col['COLUMN_NAME'] for col in columns_result] if columns_result else []
            
            # Handle line_code if column exists
            if 'line_code' in existing_columns:
                old_code = current.get('line_code', '')
                new_code = line_code or None
                if old_code != new_code:
                    changes['line_code'] = {'old': old_code, 'new': new_code}
            
            # Only update if there are changes
            if not changes:
                return True, "No changes detected", None
            
            # Build UPDATE query
            if 'modified_by' in existing_columns:
                if 'line_code' in existing_columns:
                    update_query = """
                        UPDATE ProductionLines 
                        SET line_name = ?, line_code = ?, modified_by = ?, modified_date = GETDATE()
                        WHERE line_id = ?
                    """
                    params = (line_name, line_code or None, username, line_id)
                else:
                    update_query = """
                        UPDATE ProductionLines 
                        SET line_name = ?, modified_by = ?, modified_date = GETDATE()
                        WHERE line_id = ?
                    """
                    params = (line_name, username, line_id)
            else:
                if 'line_code' in existing_columns:
                    update_query = """
                        UPDATE ProductionLines 
                        SET line_name = ?, line_code = ?
                        WHERE line_id = ?
                    """
                    params = (line_name, line_code or None, line_id)
                else:
                    update_query = """
                        UPDATE ProductionLines 
                        SET line_name = ?
                        WHERE line_id = ?
                    """
                    params = (line_name, line_id)
            
            success = conn.execute_query(update_query, params)
            
            if success:
                return True, "Production line updated successfully", changes
            
            return False, "Failed to update production line", None
    
    def deactivate(self, line_id, username):
        """Deactivate production line (soft delete)"""
        with self.db.get_connection() as conn:
            # Get current line details
            current = self.get_by_id(line_id)
            if not current:
                return False, "Production line not found"
            
            # Check if already inactive
            if not current.get('is_active'):
                return False, "Production line is already deactivated"
            
            # Check if line has downtime records (optional - for information)
            if conn.check_table_exists('Downtimes'):
                downtimes_query = """
                    SELECT COUNT(*) as count 
                    FROM Downtimes 
                    WHERE line_id = ?
                """
                downtimes = conn.execute_query(downtimes_query, (line_id,))
                has_downtimes = downtimes and downtimes[0]['count'] > 0
            else:
                has_downtimes = False
            
            # Check which columns exist
            columns_query = """
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'ProductionLines'
            """
            columns_result = conn.execute_query(columns_query)
            existing_columns = [col['COLUMN_NAME'] for col in columns_result] if columns_result else []
            
            # Deactivate
            if 'modified_by' in existing_columns:
                update_query = """
                    UPDATE ProductionLines 
                    SET is_active = 0, modified_by = ?, modified_date = GETDATE()
                    WHERE line_id = ?
                """
                params = (username, line_id)
            else:
                update_query = """
                    UPDATE ProductionLines 
                    SET is_active = 0
                    WHERE line_id = ?
                """
                params = (line_id,)
            
            success = conn.execute_query(update_query, params)
            
            if success:
                message = f"Production line '{current.get('line_name')}' deactivated"
                if has_downtimes:
                    message += f" (has historical downtime records)"
                return True, message
            
            return False, "Failed to deactivate production line"
    
    def reactivate(self, line_id, username):
        """Reactivate a deactivated production line"""
        with self.db.get_connection() as conn:
            # Check which columns exist
            columns_query = """
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'ProductionLines'
            """
            columns_result = conn.execute_query(columns_query)
            existing_columns = [col['COLUMN_NAME'] for col in columns_result] if columns_result else []
            
            if 'modified_by' in existing_columns:
                update_query = """
                    UPDATE ProductionLines 
                    SET is_active = 1, modified_by = ?, modified_date = GETDATE()
                    WHERE line_id = ?
                """
                params = (username, line_id)
            else:
                update_query = """
                    UPDATE ProductionLines 
                    SET is_active = 1
                    WHERE line_id = ?
                """
                params = (line_id,)
            
            success = conn.execute_query(update_query, params)
            return success, "Production line reactivated" if success else "Failed to reactivate"
    
    def get_by_facility(self, facility_id, active_only=True):
        """Get all production lines for a specific facility"""
        return self.get_all(facility_id=facility_id, active_only=active_only)