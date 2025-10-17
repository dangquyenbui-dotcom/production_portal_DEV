"""
Facilities database operations - FULLY IMPLEMENTED
With working update method for editing facilities
"""

from .connection import get_db
from datetime import datetime

class FacilitiesDB:
    """Facilities database operations"""
    
    def __init__(self):
        self.db = get_db()
    
    def get_all(self, active_only=True):
        """Get all facilities"""
        with self.db.get_connection() as conn:
            # Check if table exists
            if not conn.check_table_exists('Facilities'):
                return []
            
            # Check which columns exist
            columns_query = """
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'Facilities'
            """
            columns_result = conn.execute_query(columns_query)
            existing_columns = [col['COLUMN_NAME'] for col in columns_result] if columns_result else []
            
            # Build query based on available columns
            base_fields = ['facility_id', 'facility_name', 'is_active']
            optional_fields = []
            
            if 'location' in existing_columns:
                optional_fields.append('location')
            if 'created_date' in existing_columns:
                optional_fields.append('created_date')
            if 'created_by' in existing_columns:
                optional_fields.append('created_by')
            if 'modified_date' in existing_columns:
                optional_fields.append('modified_date')
            if 'modified_by' in existing_columns:
                optional_fields.append('modified_by')
            
            all_fields = base_fields + optional_fields
            fields_str = ', '.join(all_fields)
            
            if active_only:
                query = f"""
                    SELECT {fields_str}
                    FROM Facilities
                    WHERE is_active = 1
                    ORDER BY facility_name
                """
            else:
                query = f"""
                    SELECT {fields_str}
                    FROM Facilities
                    ORDER BY is_active DESC, facility_name
                """
            
            results = conn.execute_query(query)
            
            # Normalize results to expected format
            normalized = []
            for row in results if results else []:
                normalized_row = {
                    'facility_id': row.get('facility_id', 0),
                    'facility_name': row.get('facility_name', ''),
                    'location': row.get('location', ''),
                    'is_active': row.get('is_active', 1),
                    'created_date': row.get('created_date', None),
                    'created_by': row.get('created_by', None),
                    'modified_date': row.get('modified_date', None),
                    'modified_by': row.get('modified_by', None)
                }
                normalized.append(normalized_row)
            
            return normalized
    
    def get_by_id(self, facility_id):
        """Get facility by ID"""
        with self.db.get_connection() as conn:
            query = "SELECT * FROM Facilities WHERE facility_id = ?"
            results = conn.execute_query(query, (facility_id,))
            
            if results and len(results) > 0:
                row = results[0]
                return {
                    'facility_id': row.get('facility_id', 0),
                    'facility_name': row.get('facility_name', ''),
                    'location': row.get('location', ''),
                    'is_active': row.get('is_active', 1),
                    'created_date': row.get('created_date', None),
                    'created_by': row.get('created_by', None),
                    'modified_date': row.get('modified_date', None),
                    'modified_by': row.get('modified_by', None)
                }
            return None
    
    def create(self, name, location, username):
        """Create new facility"""
        with self.db.get_connection() as conn:
            # Check if facility name already exists
            check_query = "SELECT facility_id FROM Facilities WHERE facility_name = ?"
            existing = conn.execute_query(check_query, (name,))
            
            if existing:
                return False, "Facility name already exists", None
            
            # Check which columns exist
            columns_query = """
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'Facilities'
            """
            columns_result = conn.execute_query(columns_query)
            existing_columns = [col['COLUMN_NAME'] for col in columns_result] if columns_result else []
            
            # Build INSERT query based on available columns
            fields = ['facility_name', 'is_active']
            values = [name, 1]
            placeholders = ['?', '?']
            
            if 'location' in existing_columns:
                fields.append('location')
                values.append(location or None)
                placeholders.append('?')
            
            if 'created_by' in existing_columns:
                fields.extend(['created_by', 'created_date'])
                values.append(username)
                placeholders.extend(['?', 'GETDATE()'])
            
            insert_query = f"""
                INSERT INTO Facilities ({', '.join(fields)})
                VALUES ({', '.join(placeholders)})
            """
            
            success = conn.execute_query(insert_query, values)
            
            if success:
                # Get the new facility ID
                new_facility = conn.execute_query(
                    "SELECT TOP 1 facility_id FROM Facilities WHERE facility_name = ? ORDER BY facility_id DESC",
                    (name,)
                )
                facility_id = new_facility[0]['facility_id'] if new_facility else None
                return True, f"Facility '{name}' created successfully", facility_id
            
            return False, "Failed to create facility", None
    
    def update(self, facility_id, name, location, username):
        """Update existing facility"""
        with self.db.get_connection() as conn:
            # Get current record for comparison
            current = self.get_by_id(facility_id)
            if not current:
                return False, "Facility not found", None
            
            # Check if new name conflicts with another facility
            check_query = """
                SELECT facility_id FROM Facilities 
                WHERE facility_name = ? AND facility_id != ?
            """
            existing = conn.execute_query(check_query, (name, facility_id))
            
            if existing:
                return False, "Facility name already exists", None
            
            # Track changes for audit
            changes = {}
            if current.get('facility_name') != name:
                changes['facility_name'] = {'old': current.get('facility_name'), 'new': name}
            
            # Handle location field
            old_location = current.get('location', '')
            new_location = location or ''
            if old_location != new_location:
                changes['location'] = {'old': old_location, 'new': new_location}
            
            # Only update if there are changes
            if not changes:
                return True, "No changes detected", None
            
            # Check which columns exist
            columns_query = """
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'Facilities'
            """
            columns_result = conn.execute_query(columns_query)
            existing_columns = [col['COLUMN_NAME'] for col in columns_result] if columns_result else []
            
            # Build UPDATE query
            set_fields = ['facility_name = ?']
            params = [name]
            
            if 'location' in existing_columns:
                set_fields.append('location = ?')
                params.append(location or None)
            
            if 'modified_by' in existing_columns:
                set_fields.extend(['modified_by = ?', 'modified_date = GETDATE()'])
                params.append(username)
            
            params.append(facility_id)
            
            update_query = f"""
                UPDATE Facilities 
                SET {', '.join(set_fields)}
                WHERE facility_id = ?
            """
            
            success = conn.execute_query(update_query, params)
            
            if success:
                return True, "Facility updated successfully", changes
            
            return False, "Failed to update facility", None
    
    def deactivate(self, facility_id, username):
        """Deactivate facility (soft delete)"""
        with self.db.get_connection() as conn:
            # Get current facility details
            current = self.get_by_id(facility_id)
            if not current:
                return False, "Facility not found"
            
            # Check if already inactive
            if not current.get('is_active'):
                return False, "Facility is already deactivated"
            
            # Check if facility has production lines
            if conn.check_table_exists('ProductionLines'):
                lines_query = """
                    SELECT COUNT(*) as count 
                    FROM ProductionLines 
                    WHERE facility_id = ? AND is_active = 1
                """
                lines = conn.execute_query(lines_query, (facility_id,))
                
                if lines and lines[0]['count'] > 0:
                    return False, f"Cannot deactivate facility with {lines[0]['count']} active production lines"
            
            # Check which columns exist
            columns_query = """
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'Facilities'
            """
            columns_result = conn.execute_query(columns_query)
            existing_columns = [col['COLUMN_NAME'] for col in columns_result] if columns_result else []
            
            # Deactivate
            if 'modified_by' in existing_columns:
                update_query = """
                    UPDATE Facilities 
                    SET is_active = 0, modified_by = ?, modified_date = GETDATE()
                    WHERE facility_id = ?
                """
                params = (username, facility_id)
            else:
                update_query = """
                    UPDATE Facilities 
                    SET is_active = 0
                    WHERE facility_id = ?
                """
                params = (facility_id,)
            
            success = conn.execute_query(update_query, params)
            
            if success:
                return True, f"Facility '{current.get('facility_name')}' deactivated successfully"
            
            return False, "Failed to deactivate facility"
    
    def reactivate(self, facility_id, username):
        """Reactivate a deactivated facility"""
        with self.db.get_connection() as conn:
            # Get current facility details
            current = self.get_by_id(facility_id)
            if not current:
                return False, "Facility not found"
            
            # Check if already active
            if current.get('is_active'):
                return False, "Facility is already active"
            
            # Check which columns exist
            columns_query = """
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'Facilities'
            """
            columns_result = conn.execute_query(columns_query)
            existing_columns = [col['COLUMN_NAME'] for col in columns_result] if columns_result else []
            
            # Reactivate
            if 'modified_by' in existing_columns:
                update_query = """
                    UPDATE Facilities 
                    SET is_active = 1, modified_by = ?, modified_date = GETDATE()
                    WHERE facility_id = ?
                """
                params = (username, facility_id)
            else:
                update_query = """
                    UPDATE Facilities 
                    SET is_active = 1
                    WHERE facility_id = ?
                """
                params = (facility_id,)
            
            success = conn.execute_query(update_query, params)
            
            if success:
                return True, f"Facility '{current.get('facility_name')}' reactivated successfully"
            
            return False, "Failed to reactivate facility"