"""
Shifts database operations
Manages work shift definitions and schedules
"""

from .connection import get_db
from datetime import datetime, time

class ShiftsDB:
    """Shifts database operations"""
    
    def __init__(self):
        self.db = get_db()
    
    def ensure_table(self):
        """Ensure the Shifts table exists"""
        with self.db.get_connection() as conn:
            if not conn.check_table_exists('Shifts'):
                print("Creating Shifts table...")
                create_query = """
                    CREATE TABLE Shifts (
                        shift_id INT IDENTITY(1,1) PRIMARY KEY,
                        shift_name NVARCHAR(100) NOT NULL,
                        shift_code NVARCHAR(10),
                        start_time TIME NOT NULL,
                        end_time TIME NOT NULL,
                        duration_hours DECIMAL(4,2),
                        description NVARCHAR(500),
                        is_overnight BIT DEFAULT 0,
                        is_active BIT DEFAULT 1,
                        created_date DATETIME DEFAULT GETDATE(),
                        created_by NVARCHAR(100),
                        modified_date DATETIME,
                        modified_by NVARCHAR(100),
                        CONSTRAINT UQ_Shift_Name UNIQUE(shift_name),
                        CONSTRAINT UQ_Shift_Code UNIQUE(shift_code)
                    );
                    
                    CREATE INDEX IX_Shifts_Active ON Shifts(is_active);
                """
                success = conn.execute_query(create_query)
                if success:
                    print("✅ Shifts table created successfully")
                    self._insert_default_shifts()
                return success
            return True
    
    def _insert_default_shifts(self):
        """Insert default shift definitions"""
        default_shifts = [
            ('Morning Shift', 'MS', '06:00:00', '14:00:00', 'Standard morning shift'),
            ('Evening Shift', 'ES', '14:00:00', '22:00:00', 'Standard evening shift'),
            ('Night Shift', 'NS', '22:00:00', '06:00:00', 'Standard night shift (overnight)'),
        ]
        
        with self.db.get_connection() as conn:
            for name, code, start, end, desc in default_shifts:
                # Calculate if overnight
                is_overnight = 1 if end < start else 0
                
                # Calculate duration
                if is_overnight:
                    # For overnight shifts, calculate through midnight
                    start_dt = datetime.strptime(start, '%H:%M:%S')
                    end_dt = datetime.strptime(end, '%H:%M:%S')
                    duration = (24 - start_dt.hour + end_dt.hour)
                else:
                    start_dt = datetime.strptime(start, '%H:%M:%S')
                    end_dt = datetime.strptime(end, '%H:%M:%S')
                    duration = (end_dt.hour - start_dt.hour)
                
                query = """
                    INSERT INTO Shifts (shift_name, shift_code, start_time, end_time, 
                                      duration_hours, description, is_overnight, created_by)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'system')
                """
                conn.execute_query(query, (name, code, start, end, duration, desc, is_overnight))
            print("✅ Default shifts inserted")
    
    def get_all(self, active_only=True):
        """Get all shifts"""
        with self.db.get_connection() as conn:
            # Ensure table exists
            self.ensure_table()
            
            if active_only:
                query = """
                    SELECT shift_id, shift_name, shift_code, 
                           CONVERT(VARCHAR(5), start_time, 108) as start_time,
                           CONVERT(VARCHAR(5), end_time, 108) as end_time,
                           duration_hours, description, is_overnight, is_active,
                           created_date, created_by, modified_date, modified_by
                    FROM Shifts
                    WHERE is_active = 1
                    ORDER BY start_time
                """
            else:
                query = """
                    SELECT shift_id, shift_name, shift_code,
                           CONVERT(VARCHAR(5), start_time, 108) as start_time,
                           CONVERT(VARCHAR(5), end_time, 108) as end_time,
                           duration_hours, description, is_overnight, is_active,
                           created_date, created_by, modified_date, modified_by
                    FROM Shifts
                    ORDER BY is_active DESC, start_time
                """
            
            return conn.execute_query(query)
    
    def get_by_id(self, shift_id):
        """Get shift by ID"""
        with self.db.get_connection() as conn:
            query = """
                SELECT shift_id, shift_name, shift_code,
                       CONVERT(VARCHAR(5), start_time, 108) as start_time,
                       CONVERT(VARCHAR(5), end_time, 108) as end_time,
                       duration_hours, description, is_overnight, is_active
                FROM Shifts
                WHERE shift_id = ?
            """
            results = conn.execute_query(query, (shift_id,))
            return results[0] if results else None
    
    def create(self, shift_name, shift_code, start_time, end_time, description, username):
        """Create new shift"""
        with self.db.get_connection() as conn:
            # Ensure table exists
            self.ensure_table()
            
            # Check if name already exists
            check_query = "SELECT shift_id FROM Shifts WHERE shift_name = ?"
            existing = conn.execute_query(check_query, (shift_name,))
            
            if existing:
                return False, "Shift name already exists", None
            
            # Check if code already exists
            if shift_code:
                check_code = "SELECT shift_id FROM Shifts WHERE shift_code = ?"
                existing_code = conn.execute_query(check_code, (shift_code,))
                if existing_code:
                    return False, "Shift code already exists", None
            
            # Parse times and calculate duration
            try:
                # Handle both HH:MM and HH:MM:SS formats
                if len(start_time) == 5:  # HH:MM
                    start_time += ':00'
                if len(end_time) == 5:  # HH:MM
                    end_time += ':00'
                
                start_dt = datetime.strptime(start_time, '%H:%M:%S')
                end_dt = datetime.strptime(end_time, '%H:%M:%S')
                
                # Check if overnight shift
                is_overnight = 1 if end_dt < start_dt else 0
                
                # Calculate duration
                if is_overnight:
                    duration = (24 - start_dt.hour + end_dt.hour)
                else:
                    duration = (end_dt - start_dt).total_seconds() / 3600
                
            except ValueError as e:
                return False, f"Invalid time format: {str(e)}", None
            
            # Insert the shift
            insert_query = """
                INSERT INTO Shifts (shift_name, shift_code, start_time, end_time,
                                  duration_hours, description, is_overnight, 
                                  created_by, created_date, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), 1)
            """
            
            success = conn.execute_query(insert_query, (
                shift_name, shift_code or None, start_time, end_time,
                duration, description or None, is_overnight, username
            ))
            
            if success:
                # Get the new shift ID
                new_shift = conn.execute_query(
                    "SELECT TOP 1 shift_id FROM Shifts WHERE shift_name = ? ORDER BY shift_id DESC",
                    (shift_name,)
                )
                shift_id = new_shift[0]['shift_id'] if new_shift else None
                return True, f"Shift '{shift_name}' created successfully", shift_id
            
            return False, "Failed to create shift", None
    
    def update(self, shift_id, shift_name, shift_code, start_time, end_time, description, username):
        """Update existing shift"""
        with self.db.get_connection() as conn:
            # Get current record
            current = self.get_by_id(shift_id)
            if not current:
                return False, "Shift not found", None
            
            # Check if new name conflicts
            check_query = """
                SELECT shift_id FROM Shifts 
                WHERE shift_name = ? AND shift_id != ?
            """
            existing = conn.execute_query(check_query, (shift_name, shift_id))
            
            if existing:
                return False, "Shift name already exists", None
            
            # Check if new code conflicts
            if shift_code:
                check_code = """
                    SELECT shift_id FROM Shifts 
                    WHERE shift_code = ? AND shift_id != ?
                """
                existing_code = conn.execute_query(check_code, (shift_code, shift_id))
                if existing_code:
                    return False, "Shift code already exists", None
            
            # Track changes for audit
            changes = {}
            if current.get('shift_name') != shift_name:
                changes['shift_name'] = {'old': current.get('shift_name'), 'new': shift_name}
            
            if current.get('shift_code') != shift_code:
                changes['shift_code'] = {'old': current.get('shift_code'), 'new': shift_code}
            
            # Parse times and calculate duration
            try:
                # Handle both HH:MM and HH:MM:SS formats
                if len(start_time) == 5:  # HH:MM
                    start_time += ':00'
                if len(end_time) == 5:  # HH:MM
                    end_time += ':00'
                    
                start_dt = datetime.strptime(start_time, '%H:%M:%S')
                end_dt = datetime.strptime(end_time, '%H:%M:%S')
                
                # Check if overnight shift
                is_overnight = 1 if end_dt < start_dt else 0
                
                # Calculate duration
                if is_overnight:
                    duration = (24 - start_dt.hour + end_dt.hour)
                else:
                    duration = (end_dt - start_dt).total_seconds() / 3600
                    
            except ValueError as e:
                return False, f"Invalid time format: {str(e)}", None
            
            if current.get('start_time') != start_time[:5]:
                changes['start_time'] = {'old': current.get('start_time'), 'new': start_time[:5]}
            
            if current.get('end_time') != end_time[:5]:
                changes['end_time'] = {'old': current.get('end_time'), 'new': end_time[:5]}
            
            old_desc = current.get('description', '')
            new_desc = description or None
            if old_desc != new_desc:
                changes['description'] = {'old': old_desc, 'new': new_desc}
            
            # Only update if there are changes
            if not changes:
                return True, "No changes detected", None
            
            # Update the shift
            update_query = """
                UPDATE Shifts 
                SET shift_name = ?, shift_code = ?, start_time = ?, end_time = ?,
                    duration_hours = ?, description = ?, is_overnight = ?,
                    modified_by = ?, modified_date = GETDATE()
                WHERE shift_id = ?
            """
            
            success = conn.execute_query(update_query, (
                shift_name, shift_code or None, start_time, end_time,
                duration, description or None, is_overnight,
                username, shift_id
            ))
            
            if success:
                return True, "Shift updated successfully", changes
            
            return False, "Failed to update shift", None
    
    def deactivate(self, shift_id, username):
        """Deactivate shift (soft delete)"""
        with self.db.get_connection() as conn:
            # Get current shift
            current = self.get_by_id(shift_id)
            if not current:
                return False, "Shift not found"
            
            # Check if already inactive
            if not current.get('is_active'):
                return False, "Shift is already deactivated"
            
            # Check if shift is used in downtime records
            if conn.check_table_exists('Downtimes'):
                downtimes_query = """
                    SELECT COUNT(*) as count 
                    FROM Downtimes 
                    WHERE shift_id = ?
                """
                downtimes = conn.execute_query(downtimes_query, (shift_id,))
                has_downtimes = downtimes and downtimes[0]['count'] > 0
            else:
                has_downtimes = False
            
            # Deactivate
            update_query = """
                UPDATE Shifts 
                SET is_active = 0, modified_by = ?, modified_date = GETDATE()
                WHERE shift_id = ?
            """
            
            success = conn.execute_query(update_query, (username, shift_id))
            
            if success:
                message = f"Shift '{current.get('shift_name')}' deactivated"
                if has_downtimes:
                    message += f" (has historical records)"
                return True, message
            
            return False, "Failed to deactivate shift"
    
    def reactivate(self, shift_id, username):
        """Reactivate a deactivated shift"""
        with self.db.get_connection() as conn:
            # Get current shift
            current = self.get_by_id(shift_id)
            if not current:
                return False, "Shift not found"
            
            # Check if already active
            if current.get('is_active'):
                return False, "Shift is already active"
            
            # Reactivate
            update_query = """
                UPDATE Shifts 
                SET is_active = 1, modified_by = ?, modified_date = GETDATE()
                WHERE shift_id = ?
            """
            
            success = conn.execute_query(update_query, (username, shift_id))
            
            if success:
                return True, f"Shift '{current.get('shift_name')}' reactivated successfully"
            
            return False, "Failed to reactivate shift"
    
    def get_for_dropdown(self):
        """Get active shifts formatted for dropdown selection"""
        shifts = self.get_all(active_only=True)
        return [(shift['shift_id'], f"{shift['shift_name']} ({shift['start_time']} - {shift['end_time']})") 
                for shift in shifts]