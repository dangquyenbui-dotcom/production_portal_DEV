"""
Downtime categories database operations
Handles hierarchical category structure with main and sub-categories
FIXED: Better error handling for deactivation
"""

from .connection import get_db
from datetime import datetime

class CategoriesDB:
    """Downtime categories database operations"""
    
    def __init__(self):
        self.db = get_db()
    
    def get_all(self, active_only=True):
        """Get all categories with parent-child relationship"""
        with self.db.get_connection() as conn:
            # Check if table exists
            if not conn.check_table_exists('DowntimeCategories'):
                return []
            
            # Check which columns exist
            columns_query = """
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'DowntimeCategories'
            """
            columns_result = conn.execute_query(columns_query)
            existing_columns = [col['COLUMN_NAME'] for col in columns_result] if columns_result else []
            
            # Build query based on available columns
            base_fields = ['category_id', 'category_name', 'description', 'is_active']
            optional_fields = []
            
            if 'category_code' in existing_columns:
                optional_fields.append('category_code')
            if 'parent_id' in existing_columns:
                optional_fields.append('parent_id')
            if 'color_code' in existing_columns:
                optional_fields.append('color_code')
            if 'notification_required' in existing_columns:
                optional_fields.append('notification_required')
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
                    FROM DowntimeCategories
                    WHERE is_active = 1
                    ORDER BY category_name
                """
            else:
                query = f"""
                    SELECT {fields_str}
                    FROM DowntimeCategories
                    ORDER BY is_active DESC, category_name
                """
            
            return conn.execute_query(query)
    
    def get_hierarchical(self, active_only=True):
        """Get categories organized hierarchically (main categories with their subcategories)"""
        with self.db.get_connection() as conn:
            # Check if parent_id column exists
            columns_query = """
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'DowntimeCategories' AND COLUMN_NAME = 'parent_id'
            """
            has_hierarchy = len(conn.execute_query(columns_query)) > 0
            
            if not has_hierarchy:
                # Return flat list if no hierarchy support
                return self.get_all(active_only)
            
            # Get all categories with parent information
            query = """
                SELECT 
                    c1.category_id,
                    c1.category_name,
                    c1.category_code,
                    c1.description,
                    c1.parent_id,
                    c1.color_code,
                    c1.notification_required,
                    c1.is_active,
                    c1.created_date,
                    c1.created_by,
                    c1.modified_date,
                    c1.modified_by,
                    c2.category_name as parent_name,
                    c2.category_code as parent_code
                FROM DowntimeCategories c1
                LEFT JOIN DowntimeCategories c2 ON c1.parent_id = c2.category_id
                {} 
                ORDER BY 
                    CASE WHEN c1.parent_id IS NULL THEN c1.category_code ELSE c2.category_code END,
                    c1.parent_id,
                    c1.category_code
            """.format("WHERE c1.is_active = 1" if active_only else "")
            
            categories = conn.execute_query(query)
            
            # Organize hierarchically
            main_categories = {}
            for cat in categories:
                if cat.get('parent_id') is None:
                    cat['subcategories'] = []
                    main_categories[cat['category_id']] = cat
            
            # Add subcategories to their parents
            for cat in categories:
                parent_id = cat.get('parent_id')
                if parent_id and parent_id in main_categories:
                    main_categories[parent_id]['subcategories'].append(cat)
            
            return list(main_categories.values())
    
    def get_by_id(self, category_id):
        """Get category by ID"""
        with self.db.get_connection() as conn:
            query = "SELECT * FROM DowntimeCategories WHERE category_id = ?"
            results = conn.execute_query(query, (category_id,))
            return results[0] if results else None
    
    def create(self, category_name, category_code, description, parent_id, 
              color_code, notification_required, username):
        """Create new category"""
        with self.db.get_connection() as conn:
            # Check if code already exists
            check_query = "SELECT category_id FROM DowntimeCategories WHERE category_code = ?"
            existing = conn.execute_query(check_query, (category_code,))
            
            if existing:
                return False, "Category code already exists", None
            
            # Check which columns exist
            columns_query = """
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'DowntimeCategories'
            """
            columns_result = conn.execute_query(columns_query)
            existing_columns = [col['COLUMN_NAME'] for col in columns_result] if columns_result else []
            
            # Build INSERT query based on available columns
            fields = ['category_name', 'description', 'is_active']
            values = [category_name, description or None, 1]
            placeholders = ['?', '?', '?']
            
            if 'category_code' in existing_columns:
                fields.append('category_code')
                values.append(category_code)
                placeholders.append('?')
            
            if 'parent_id' in existing_columns and parent_id:
                fields.append('parent_id')
                values.append(parent_id)
                placeholders.append('?')
            
            if 'color_code' in existing_columns:
                fields.append('color_code')
                values.append(color_code or '#667eea')
                placeholders.append('?')
            
            if 'notification_required' in existing_columns:
                fields.append('notification_required')
                values.append(1 if notification_required else 0)
                placeholders.append('?')
            
            if 'created_by' in existing_columns:
                fields.extend(['created_by', 'created_date'])
                values.append(username)
                placeholders.extend(['?', 'GETDATE()'])
            
            insert_query = f"""
                INSERT INTO DowntimeCategories ({', '.join(fields)})
                VALUES ({', '.join(placeholders)})
            """
            
            success = conn.execute_query(insert_query, values)
            
            if success:
                # Get the new category ID
                new_category = conn.execute_query(
                    "SELECT TOP 1 category_id FROM DowntimeCategories WHERE category_code = ? ORDER BY category_id DESC",
                    (category_code,)
                )
                category_id = new_category[0]['category_id'] if new_category else None
                return True, f"Category '{category_name}' created successfully", category_id
            
            return False, "Failed to create category", None
    
    def update(self, category_id, category_name, description, color_code, 
              notification_required, username):
        """Update existing category (code cannot be changed)"""
        with self.db.get_connection() as conn:
            # Get current record for comparison
            current = self.get_by_id(category_id)
            if not current:
                return False, "Category not found", None
            
            # Track changes for audit
            changes = {}
            if current.get('category_name') != category_name:
                changes['category_name'] = {'old': current.get('category_name'), 'new': category_name}
            
            old_desc = current.get('description', '')
            new_desc = description or None
            if old_desc != new_desc:
                changes['description'] = {'old': old_desc, 'new': new_desc}
            
            # Check which columns exist
            columns_query = """
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'DowntimeCategories'
            """
            columns_result = conn.execute_query(columns_query)
            existing_columns = [col['COLUMN_NAME'] for col in columns_result] if columns_result else []
            
            # Handle optional columns
            if 'color_code' in existing_columns:
                if current.get('color_code') != color_code:
                    changes['color_code'] = {'old': current.get('color_code'), 'new': color_code}
            
            if 'notification_required' in existing_columns:
                old_notification = bool(current.get('notification_required'))
                if old_notification != notification_required:
                    changes['notification_required'] = {
                        'old': '1' if old_notification else '0',
                        'new': '1' if notification_required else '0'
                    }
            
            # Only update if there are changes
            if not changes:
                return True, "No changes detected", None
            
            # Build UPDATE query
            set_fields = ['category_name = ?', 'description = ?']
            params = [category_name, description or None]
            
            if 'color_code' in existing_columns:
                set_fields.append('color_code = ?')
                params.append(color_code or '#667eea')
            
            if 'notification_required' in existing_columns:
                set_fields.append('notification_required = ?')
                params.append(1 if notification_required else 0)
            
            if 'modified_by' in existing_columns:
                set_fields.extend(['modified_by = ?', 'modified_date = GETDATE()'])
                params.append(username)
            
            params.append(category_id)
            
            update_query = f"""
                UPDATE DowntimeCategories 
                SET {', '.join(set_fields)}
                WHERE category_id = ?
            """
            
            success = conn.execute_query(update_query, params)
            
            if success:
                return True, "Category updated successfully", changes
            
            return False, "Failed to update category", None
    
    def deactivate(self, category_id, username):
        """Deactivate category (soft delete)"""
        try:
            with self.db.get_connection() as conn:
                # First, ensure we have a valid connection
                if not conn:
                    return False, "Database connection failed"
                
                # Get current category details
                current = self.get_by_id(category_id)
                if not current:
                    return False, "Category not found"
                
                # Check if already inactive
                if not current.get('is_active'):
                    return False, "Category is already deactivated"
                
                # Check if category has subcategories (if hierarchy is supported)
                columns_query = """
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'DowntimeCategories' AND COLUMN_NAME = 'parent_id'
                """
                columns_result = conn.execute_query(columns_query)
                has_hierarchy = columns_result and len(columns_result) > 0
                
                if has_hierarchy:
                    subcategories_query = """
                        SELECT COUNT(*) as count 
                        FROM DowntimeCategories 
                        WHERE parent_id = ? AND is_active = 1
                    """
                    subcategories = conn.execute_query(subcategories_query, (category_id,))
                    
                    if subcategories and subcategories[0]['count'] > 0:
                        return False, "Cannot deactivate category with active subcategories"
                
                # Check if category is used in downtime records
                if conn.check_table_exists('Downtimes'):
                    downtimes_query = """
                        SELECT COUNT(*) as count 
                        FROM Downtimes 
                        WHERE category_id = ?
                    """
                    downtimes = conn.execute_query(downtimes_query, (category_id,))
                    has_downtimes = downtimes and downtimes[0]['count'] > 0
                else:
                    has_downtimes = False
                
                # Check which columns exist for update
                columns_result = conn.execute_query("""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'DowntimeCategories'
                """)
                existing_columns = [col['COLUMN_NAME'] for col in columns_result] if columns_result else []
                
                # Deactivate
                if 'modified_by' in existing_columns:
                    update_query = """
                        UPDATE DowntimeCategories 
                        SET is_active = 0, modified_by = ?, modified_date = GETDATE()
                        WHERE category_id = ?
                    """
                    params = (username, category_id)
                else:
                    update_query = """
                        UPDATE DowntimeCategories 
                        SET is_active = 0
                        WHERE category_id = ?
                    """
                    params = (category_id,)
                
                success = conn.execute_query(update_query, params)
                
                if success:
                    message = f"Category '{current.get('category_name')}' deactivated"
                    if has_downtimes:
                        message += f" (has {downtimes[0]['count']} historical records)"
                    return True, message
                
                return False, "Failed to deactivate category"
                
        except Exception as e:
            print(f"Error in deactivate method: {str(e)}")
            import traceback
            traceback.print_exc()
            return False, f"Error deactivating category: {str(e)}"
    
    def reactivate(self, category_id, username):
        """Reactivate a deactivated category"""
        try:
            with self.db.get_connection() as conn:
                # Ensure we have a valid connection
                if not conn:
                    return False, "Database connection failed"
                
                # Get current category details
                current = self.get_by_id(category_id)
                if not current:
                    return False, "Category not found"
                
                # Check if already active
                if current.get('is_active'):
                    return False, "Category is already active"
                
                # Check if parent category is active (if this is a subcategory)
                if current.get('parent_id'):
                    parent_query = """
                        SELECT is_active 
                        FROM DowntimeCategories 
                        WHERE category_id = ?
                    """
                    parent_result = conn.execute_query(parent_query, (current['parent_id'],))
                    if parent_result and not parent_result[0]['is_active']:
                        return False, "Cannot reactivate subcategory when parent category is inactive"
                
                # Check which columns exist for update
                columns_result = conn.execute_query("""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'DowntimeCategories'
                """)
                existing_columns = [col['COLUMN_NAME'] for col in columns_result] if columns_result else []
                
                # Reactivate
                if 'modified_by' in existing_columns:
                    update_query = """
                        UPDATE DowntimeCategories 
                        SET is_active = 1, modified_by = ?, modified_date = GETDATE()
                        WHERE category_id = ?
                    """
                    params = (username, category_id)
                else:
                    update_query = """
                        UPDATE DowntimeCategories 
                        SET is_active = 1
                        WHERE category_id = ?
                    """
                    params = (category_id,)
                
                success = conn.execute_query(update_query, params)
                
                if success:
                    return True, f"Category '{current.get('category_name')}' reactivated successfully"
                
                return False, "Failed to reactivate category"
                
        except Exception as e:
            print(f"Error in reactivate method: {str(e)}")
            import traceback
            traceback.print_exc()
            return False, f"Error reactivating category: {str(e)}"
    
    def get_for_dropdown(self):
        """Get active categories formatted for dropdown selection"""
        categories = self.get_all(active_only=True)
        return [(cat['category_id'], cat['category_name']) for cat in categories]