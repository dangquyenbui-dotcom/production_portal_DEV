# dangquyenbui-dotcom/downtime_tracker/downtime_tracker-953d9e6915ad7fa465db9a8f87b8a56d713b0537/database/capacity.py
"""
Production Capacity database operations
Manages the production output rates for each line.
"""

from .connection import get_db

class ProductionCapacityDB:
    """Production Capacity database operations"""

    def __init__(self):
        self.db = get_db()
        self.ensure_table()

    def ensure_table(self):
        """Ensure the ProductionCapacity table exists."""
        with self.db.get_connection() as conn:
            if not conn.check_table_exists('ProductionCapacity'):
                print("Creating ProductionCapacity table...")
                create_query = """
                    CREATE TABLE ProductionCapacity (
                        capacity_id INT IDENTITY(1,1) PRIMARY KEY,
                        line_id INT NOT NULL,
                        capacity_per_shift INT NOT NULL,
                        unit NVARCHAR(50) DEFAULT 'units',
                        notes NVARCHAR(500),
                        created_by NVARCHAR(100),
                        created_date DATETIME DEFAULT GETDATE(),
                        modified_by NVARCHAR(100),
                        modified_date DATETIME,
                        CONSTRAINT FK_Capacity_Line FOREIGN KEY (line_id) REFERENCES ProductionLines(line_id),
                        CONSTRAINT UQ_Capacity_Line UNIQUE (line_id)
                    );
                """
                if conn.execute_query(create_query):
                    print("✅ ProductionCapacity table created successfully.")

    def get_all(self):
        """Get all capacity settings, joined with line and facility info."""
        with self.db.get_connection() as conn:
            query = """
                SELECT 
                    pc.capacity_id,
                    pc.line_id,
                    pc.capacity_per_shift,
                    pc.unit,
                    pc.notes,
                    pl.line_name,
                    f.facility_name
                FROM ProductionCapacity pc
                JOIN ProductionLines pl ON pc.line_id = pl.line_id
                JOIN Facilities f ON pl.facility_id = f.facility_id
                ORDER BY f.facility_name, pl.line_name;
            """
            return conn.execute_query(query)

    def create_or_update(self, line_id, capacity_per_shift, unit, notes, username):
        """Create a new capacity setting or update it if it exists for the line."""
        with self.db.get_connection() as conn:
            # MERGE statement performs an "upsert" (update or insert)
            query = """
                MERGE ProductionCapacity AS target
                USING (SELECT ? AS line_id) AS source
                ON (target.line_id = source.line_id)
                WHEN MATCHED THEN
                    UPDATE SET 
                        capacity_per_shift = ?,
                        unit = ?,
                        notes = ?,
                        modified_by = ?,
                        modified_date = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (line_id, capacity_per_shift, unit, notes, created_by)
                    VALUES (?, ?, ?, ?, ?);
            """
            params = (
                line_id, 
                capacity_per_shift, unit, notes, username,  # For UPDATE
                line_id, capacity_per_shift, unit, notes, username   # For INSERT
            )
            success = conn.execute_query(query, params)
            if success:
                return True, "Production capacity saved successfully."
            return False, "Failed to save production capacity."

    def delete(self, capacity_id):
        """Delete a capacity setting."""
        with self.db.get_connection() as conn:
            query = "DELETE FROM ProductionCapacity WHERE capacity_id = ?"
            success = conn.execute_query(query, (capacity_id,))
            if success:
                return True, "Capacity setting deleted."
            return False, "Failed to delete capacity setting."