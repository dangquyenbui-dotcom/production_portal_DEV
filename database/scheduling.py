"""
Database operations for the Production Scheduling module.
"""

from .connection import get_db
from .erp_connection import get_erp_service
from datetime import datetime

class SchedulingDB:
    """Handles data for the scheduling grid."""

    def __init__(self):
        self.db = get_db()
        self.erp_service = get_erp_service()
        self.ensure_table()

    def ensure_table(self):
        """Ensures the ScheduleProjections table exists in the local database."""
        with self.db.get_connection() as conn:
            if not conn.check_table_exists('ScheduleProjections'):
                print("Creating ScheduleProjections table...")
                create_query = """
                    CREATE TABLE ScheduleProjections (
                        projection_id INT IDENTITY(1,1) PRIMARY KEY,
                        so_number NVARCHAR(50) NOT NULL,
                        part_number NVARCHAR(100) NOT NULL,
                        can_make_no_risk DECIMAL(18, 2),
                        low_risk DECIMAL(18, 2),
                        high_risk DECIMAL(18, 2),
                        updated_by NVARCHAR(100),
                        updated_date DATETIME,
                        CONSTRAINT UQ_ScheduleProjection UNIQUE (so_number, part_number)
                    );
                """
                if conn.execute_query(create_query):
                    print("✅ ScheduleProjections table created successfully.")

    def get_schedule_data(self):
        """
        Fetches open order data from ERP and joins it with local projections and on-hand inventory.
        Also calculates the total value of all on-hand inventory.
        """
        # Step 1: Get the main sales order data from ERP
        erp_data = self.erp_service.get_open_order_schedule()
        
        # Step 2: Get the on-hand inventory data from ERP for row-level display
        on_hand_data = self.erp_service.get_on_hand_inventory()
        # Create a simple lookup map: { 'PartNumber': TotalOnHand }
        on_hand_map = {item['PartNumber']: item['TotalOnHand'] for item in on_hand_data}

        # Step 3: Get the user-saved projections from the local database
        with self.db.get_connection() as conn:
            local_projections_query = "SELECT so_number, part_number, can_make_no_risk, high_risk FROM ScheduleProjections"
            local_data = conn.execute_query(local_projections_query)
        
        # Create a lookup map for user projections
        projections_map = { f"{row['so_number']}-{row['part_number']}": row for row in local_data }

        # --- Get the split FG On Hand values and labels ---
        fg_on_hand_split = self.erp_service.get_split_fg_on_hand_value()
        
        # --- Get the total shipped value for the current month ---
        shipped_current_month = self.erp_service.get_shipped_for_current_month()

        # Step 4: Combine all data sources and perform final calculations
        for erp_row in erp_data:
            key = f"{erp_row['SO']}-{erp_row['Part']}"
            projection = projections_map.get(key)
            
            # Add On-Hand Quantity from the map for display in the grid
            on_hand_qty = on_hand_map.get(erp_row['Part'], 0) or 0
            erp_row['On hand Qty'] = on_hand_qty

            # --- MODIFIED: 'Net Qty' CALCULATION ---
            ord_qty_00_level = erp_row.get('Ord Qty - (00) Level', 0) or 0
            produced_qty = erp_row.get('Produced Qty', 0) or 0
            net_qty = ord_qty_00_level - produced_qty
            erp_row['Net Qty'] = net_qty if net_qty > 0 else 0 # Ensure Net Qty is not negative

            # Prioritize user-saved projections for editable fields
            if projection:
                erp_row['No/Low Risk Qty'] = projection.get('can_make_no_risk', 0)
                erp_row['High Risk Qty'] = projection.get('high_risk', 0)
            else:
                # Otherwise, use default values from ERP
                no_risk_val = erp_row.get('Can Make - No Risk', 0) or 0
                low_risk_val = erp_row.get('Low Risk', 0) or 0
                erp_row['No/Low Risk Qty'] = no_risk_val + low_risk_val
                erp_row['High Risk Qty'] = erp_row.get('High Risk', 0) or 0
            
            # Recalculate financial columns
            price = erp_row.get('Unit Price', 0) or 0
            erp_row['$ No/Low Risk Qty'] = (erp_row['No/Low Risk Qty'] or 0) * price
            erp_row['$ High Risk'] = (erp_row['High Risk Qty'] or 0) * price
            
            # --- MODIFIED: Calculate 'Ext Qty' column ---
            try:
                qty_per_uom = float(erp_row.get('Qty Per UoM')) if erp_row.get('Qty Per UoM') else 1.0
            except (ValueError, TypeError):
                qty_per_uom = 1.0
            
            # Ensure both operands are floats before multiplying to prevent TypeError
            erp_row['Ext Qty'] = float(erp_row.get('Net Qty') or 0.0) * qty_per_uom
        
        # Combine grid data with all summary card values
        return {
            "grid_data": erp_data,
            "fg_on_hand_split": fg_on_hand_split,
            "shipped_current_month": shipped_current_month
        }

    def update_projection(self, so_number, part_number, risk_type, quantity, username):
        """
        Updates or inserts a projection quantity into the local ScheduleProjections table.
        """
        risk_column_map = {
            'No/Low Risk Qty': 'can_make_no_risk',
            'High Risk Qty': 'high_risk'
        }
        
        column_to_update = risk_column_map.get(risk_type)
        if not column_to_update:
            return False, "Invalid risk type specified."

        with self.db.get_connection() as conn:
            sql = f"""
                MERGE ScheduleProjections AS target
                USING (SELECT ? AS so_number, ? AS part_number) AS source
                ON (target.so_number = source.so_number AND target.part_number = source.part_number)
                WHEN MATCHED THEN
                    UPDATE SET 
                        {column_to_update} = ?,
                        updated_by = ?,
                        updated_date = GETDATE()
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (so_number, part_number, {column_to_update}, updated_by, updated_date)
                    VALUES (?, ?, ?, ?, GETDATE());
            """
            params = (so_number, part_number, quantity, username, so_number, part_number, quantity, username)
            
            success = conn.execute_query(sql, params)
            if success:
                return True, "Projection saved successfully."
            else:
                return False, "Failed to save projection to the local database."

# Singleton instance
scheduling_db = SchedulingDB()