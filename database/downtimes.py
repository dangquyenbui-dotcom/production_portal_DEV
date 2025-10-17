# database/downtimes.py - Complete file with ERP job support

"""
Downtimes database operations - WITH ERP JOB INTEGRATION
Enhanced for production downtime tracking with job selection
"""

from .connection import get_db
from datetime import datetime, timedelta

class DowntimesDB:
    """Downtime entries database operations"""
    
    def __init__(self):
        self.db = get_db()
        self.ensure_table_updated()
    
    def ensure_table_updated(self):
        """Ensure the Downtimes table has all required columns"""
        with self.db.get_connection() as conn:
            # Check if crew_size column exists, add if not
            check_column = """
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'Downtimes' 
                AND COLUMN_NAME = 'crew_size'
            """
            result = conn.execute_scalar(check_column)
            
            if result == 0:
                print("Adding crew_size column to Downtimes table...")
                alter_query = """
                    ALTER TABLE Downtimes 
                    ADD crew_size INT DEFAULT 1
                """
                conn.execute_query(alter_query)
                print("✅ crew_size column added successfully")
    
    def get_by_id(self, downtime_id):
        """Get downtime entry by ID"""
        with self.db.get_connection() as conn:
            query = """
                SELECT d.*, 
                       pl.line_name,
                       f.facility_name,
                       f.facility_id,
                       dc.category_name,
                       dc.parent_id,
                       s.shift_name
                FROM Downtimes d
                INNER JOIN ProductionLines pl ON d.line_id = pl.line_id
                INNER JOIN Facilities f ON pl.facility_id = f.facility_id
                INNER JOIN DowntimeCategories dc ON d.category_id = dc.category_id
                LEFT JOIN Shifts s ON d.shift_id = s.shift_id
                WHERE d.downtime_id = ? AND d.is_deleted = 0
            """
            results = conn.execute_query(query, (downtime_id,))
            return results[0] if results else None
    
    def create(self, data):
        """
        Add a new downtime record with optional ERP job information
        
        Args:
            data: dict with keys:
                - facility_id (required)
                - line_id (required)
                - category_id (required)
                - shift_id (optional - can be auto-detected)
                - start_time (required)
                - end_time (required)
                - crew_size (required)
                - reason_notes (optional)
                - entered_by (required)
                - erp_job_number (optional)
                - erp_part_number (optional)
                - erp_part_description (optional)
        
        Returns:
            tuple: (success, message, downtime_id)
        """
        with self.db.get_connection() as conn:
            # Validate required fields
            required = ['line_id', 'category_id', 'start_time', 'end_time', 'entered_by', 'crew_size']
            for field in required:
                if field not in data or data[field] is None:
                    return False, f"Missing required field: {field}", None
            
            # Validate crew size
            crew_size = int(data.get('crew_size', 1))
            if crew_size < 1 or crew_size > 10:
                return False, "Crew size must be between 1 and 10", None
            
            # Calculate duration for validation only (not inserted)
            try:
                if isinstance(data['start_time'], str):
                    start = datetime.fromisoformat(data['start_time'])
                else:
                    start = data['start_time']
                
                if isinstance(data['end_time'], str):
                    end = datetime.fromisoformat(data['end_time'])
                else:
                    end = data['end_time']
                
                duration_minutes = int((end - start).total_seconds() / 60)
                
                if duration_minutes <= 0:
                    return False, "End time must be after start time", None
                
                if duration_minutes > 1440:  # 24 hours
                    return False, "Downtime duration cannot exceed 24 hours", None
                    
            except (ValueError, TypeError) as e:
                return False, f"Invalid datetime format: {str(e)}", None
            
            # Auto-detect shift if not provided
            if not data.get('shift_id'):
                shift_id = self._detect_shift(start)
                data['shift_id'] = shift_id
            
            # Check if ERP columns exist in the database
            check_columns = """
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'Downtimes' 
                AND COLUMN_NAME IN ('erp_job_number', 'erp_part_number', 'erp_part_description')
            """
            erp_columns_exist = conn.execute_scalar(check_columns) == 3
            
            # Build INSERT query based on available columns
            if erp_columns_exist:
                insert_query = """
                    INSERT INTO Downtimes (
                        line_id, category_id, shift_id,
                        start_time, end_time,
                        crew_size, reason_notes, entered_by, entered_date,
                        erp_job_number, erp_part_number, erp_part_description,
                        created_by, created_date, is_deleted
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?, ?, ?, ?, GETDATE(), 0)
                """
                
                params = (
                    data['line_id'],
                    data['category_id'],
                    data.get('shift_id'),
                    start,
                    end,
                    crew_size,
                    data.get('reason_notes', ''),
                    data['entered_by'],
                    data.get('erp_job_number'),
                    data.get('erp_part_number'),
                    data.get('erp_part_description'),
                    data['entered_by']
                )
            else:
                # Fallback for older schema without ERP columns
                insert_query = """
                    INSERT INTO Downtimes (
                        line_id, category_id, shift_id,
                        start_time, end_time,
                        crew_size, reason_notes, entered_by, entered_date,
                        created_by, created_date, is_deleted
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?, GETDATE(), 0)
                """
                
                params = (
                    data['line_id'],
                    data['category_id'],
                    data.get('shift_id'),
                    start,
                    end,
                    crew_size,
                    data.get('reason_notes', ''),
                    data['entered_by'],
                    data['entered_by']
                )
            
            success = conn.execute_query(insert_query, params)
            
            if success:
                # Get the new downtime ID
                new_downtime = conn.execute_query("""
                    SELECT TOP 1 downtime_id 
                    FROM Downtimes 
                    WHERE entered_by = ? 
                    ORDER BY downtime_id DESC
                """, (data['entered_by'],))
                
                downtime_id = new_downtime[0]['downtime_id'] if new_downtime else None
                
                # Create message with job info if applicable
                message = f"Downtime entry created ({duration_minutes} minutes)"
                if data.get('erp_job_number'):
                    message += f" - Job: {data['erp_job_number']}"
                
                return True, message, downtime_id
            
            return False, "Failed to create downtime entry", None
    
    def update(self, downtime_id, data, username):
        """
        Update an existing downtime entry
        
        Args:
            downtime_id: ID of the downtime entry to update
            data: dict with updated fields
            username: user making the update
        
        Returns:
            tuple: (success, message)
        """
        with self.db.get_connection() as conn:
            # Get current record
            current = self.get_by_id(downtime_id)
            if not current:
                return False, "Downtime entry not found"
            
            # Check if user owns this entry or is admin
            if current['entered_by'] != username:
                return False, "You can only edit your own entries"
            
            # Validate times
            try:
                if isinstance(data['start_time'], str):
                    start = datetime.fromisoformat(data['start_time'])
                else:
                    start = data['start_time']
                
                if isinstance(data['end_time'], str):
                    end = datetime.fromisoformat(data['end_time'])
                else:
                    end = data['end_time']
                
                duration_minutes = int((end - start).total_seconds() / 60)
                
                if duration_minutes <= 0:
                    return False, "End time must be after start time"
                
                if duration_minutes > 1440:
                    return False, "Downtime duration cannot exceed 24 hours"
                    
            except (ValueError, TypeError) as e:
                return False, f"Invalid datetime format: {str(e)}"
            
            # Check if ERP columns exist
            check_columns = """
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'Downtimes' 
                AND COLUMN_NAME IN ('erp_job_number', 'erp_part_number', 'erp_part_description')
            """
            erp_columns_exist = conn.execute_scalar(check_columns) == 3
            
            # Build update query
            if erp_columns_exist:
                update_query = """
                    UPDATE Downtimes 
                    SET line_id = ?,
                        category_id = ?,
                        shift_id = ?,
                        start_time = ?,
                        end_time = ?,
                        crew_size = ?,
                        reason_notes = ?,
                        erp_job_number = ?,
                        erp_part_number = ?,
                        erp_part_description = ?,
                        modified_by = ?,
                        modified_date = GETDATE()
                    WHERE downtime_id = ?
                """
                
                params = (
                    data.get('line_id', current['line_id']),
                    data.get('category_id', current['category_id']),
                    data.get('shift_id', current['shift_id']),
                    start,
                    end,
                    data.get('crew_size', current.get('crew_size', 1)),
                    data.get('reason_notes', current.get('reason_notes', '')),
                    data.get('erp_job_number'),
                    data.get('erp_part_number'),
                    data.get('erp_part_description'),
                    username,
                    downtime_id
                )
            else:
                update_query = """
                    UPDATE Downtimes 
                    SET line_id = ?,
                        category_id = ?,
                        shift_id = ?,
                        start_time = ?,
                        end_time = ?,
                        crew_size = ?,
                        reason_notes = ?,
                        modified_by = ?,
                        modified_date = GETDATE()
                    WHERE downtime_id = ?
                """
                
                params = (
                    data.get('line_id', current['line_id']),
                    data.get('category_id', current['category_id']),
                    data.get('shift_id', current['shift_id']),
                    start,
                    end,
                    data.get('crew_size', current.get('crew_size', 1)),
                    data.get('reason_notes', current.get('reason_notes', '')),
                    username,
                    downtime_id
                )
            
            success = conn.execute_query(update_query, params)
            
            if success:
                message = f"Downtime entry updated ({duration_minutes} minutes)"
                if data.get('erp_job_number'):
                    message += f" - Job: {data['erp_job_number']}"
                return True, message
            
            return False, "Failed to update downtime entry"
    
    def delete(self, downtime_id, username):
        """Soft delete a downtime entry"""
        with self.db.get_connection() as conn:
            # Check ownership
            current = self.get_by_id(downtime_id)
            if not current:
                return False, "Downtime entry not found"
            
            if current['entered_by'] != username:
                return False, "You can only delete your own entries"
            
            query = """
                UPDATE Downtimes 
                SET is_deleted = 1,
                    modified_by = ?,
                    modified_date = GETDATE()
                WHERE downtime_id = ?
            """
            
            success = conn.execute_query(query, (username, downtime_id))
            
            if success:
                return True, "Downtime entry deleted"
            
            return False, "Failed to delete downtime entry"
    
    def _detect_shift(self, timestamp):
        """Auto-detect shift based on timestamp"""
        with self.db.get_connection() as conn:
            # Get all active shifts
            query = """
                SELECT shift_id, start_time, end_time, is_overnight
                FROM Shifts
                WHERE is_active = 1
            """
            shifts = conn.execute_query(query)
            
            if not shifts:
                return None
            
            # Get the time portion of the timestamp
            check_time = timestamp.time()
            
            for shift in shifts:
                start_time = shift['start_time']
                end_time = shift['end_time']
                is_overnight = shift['is_overnight']
                
                if is_overnight:
                    # Overnight shift (e.g., 22:00 to 06:00)
                    if check_time >= start_time or check_time < end_time:
                        return shift['shift_id']
                else:
                    # Regular shift
                    if start_time <= check_time < end_time:
                        return shift['shift_id']
            
            return None
    
    def get_recent(self, days=7, facility_id=None, line_id=None, limit=100):
        """Get recent downtime entries"""
        with self.db.get_connection() as conn:
            base_query = f"""
                SELECT TOP {limit}
                    d.downtime_id,
                    d.line_id,
                    d.category_id,
                    d.start_time,
                    d.end_time,
                    d.duration_minutes,
                    d.crew_size,
                    d.reason_notes,
                    d.entered_by,
                    d.entered_date,
                    d.shift_id,
                    pl.facility_id,
                    pl.line_name,
                    f.facility_name,
                    dc.category_name,
                    dc.category_code,
                    s.shift_name
                FROM Downtimes d
                INNER JOIN ProductionLines pl ON d.line_id = pl.line_id
                INNER JOIN Facilities f ON pl.facility_id = f.facility_id
                INNER JOIN DowntimeCategories dc ON d.category_id = dc.category_id
                LEFT JOIN Shifts s ON d.shift_id = s.shift_id
                WHERE d.start_time >= DATEADD(day, ?, GETDATE())
                AND d.is_deleted = 0
            """
            
            params = [-days]
            
            if facility_id:
                base_query += " AND pl.facility_id = ?"
                params.append(facility_id)
            
            if line_id:
                base_query += " AND d.line_id = ?"
                params.append(line_id)
            
            base_query += " ORDER BY d.start_time DESC"
            
            return conn.execute_query(base_query, params)
    
    def get_user_entries_for_line_today(self, username, line_id):
        """Get user's entries for a specific line today"""
        with self.db.get_connection() as conn:
            query = """
                SELECT 
                    d.downtime_id,
                    d.line_id,
                    d.category_id,
                    d.start_time,
                    d.end_time,
                    d.duration_minutes,
                    d.crew_size,
                    d.reason_notes,
                    d.shift_id,
                    pl.line_name,
                    f.facility_name,
                    f.facility_id,
                    dc.category_name,
                    dc.parent_id,
                    s.shift_name
                FROM Downtimes d
                INNER JOIN ProductionLines pl ON d.line_id = pl.line_id
                INNER JOIN Facilities f ON pl.facility_id = f.facility_id
                INNER JOIN DowntimeCategories dc ON d.category_id = dc.category_id
                LEFT JOIN Shifts s ON d.shift_id = s.shift_id
                WHERE d.entered_by = ?
                AND d.line_id = ?
                AND CAST(d.start_time AS DATE) = CAST(GETDATE() AS DATE)
                AND d.is_deleted = 0
                ORDER BY d.start_time DESC
            """
            
            return conn.execute_query(query, (username, line_id))
    
    def get_all_entries_for_line_today(self, line_id):
        """Get ALL entries for a specific line today (from all users)"""
        with self.db.get_connection() as conn:
            query = """
                SELECT 
                    d.downtime_id,
                    d.line_id,
                    d.category_id,
                    d.start_time,
                    d.end_time,
                    d.duration_minutes,
                    d.crew_size,
                    d.reason_notes,
                    d.entered_by,
                    d.shift_id,
                    pl.line_name,
                    f.facility_name,
                    f.facility_id,
                    dc.category_name,
                    dc.parent_id,
                    s.shift_name
                FROM Downtimes d
                INNER JOIN ProductionLines pl ON d.line_id = pl.line_id
                INNER JOIN Facilities f ON pl.facility_id = f.facility_id
                INNER JOIN DowntimeCategories dc ON d.category_id = dc.category_id
                LEFT JOIN Shifts s ON d.shift_id = s.shift_id
                WHERE d.line_id = ?
                AND CAST(d.start_time AS DATE) = CAST(GETDATE() AS DATE)
                AND d.is_deleted = 0
                ORDER BY d.start_time DESC
            """
            
            return conn.execute_query(query, (line_id,))