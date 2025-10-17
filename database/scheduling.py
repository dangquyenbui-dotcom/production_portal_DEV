# database/scheduling.py
"""
Database operations for the Production Scheduling module.
"""

from .connection import get_db
from database import get_erp_service
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
        on_hand_map = {item['PartNumber']: item['TotalOnHand'] for item in on_hand_data}

        # Step 3: Get the user-saved projections from the local database
        with self.db.get_connection() as conn:
            local_projections_query = "SELECT so_number, part_number, can_make_no_risk, high_risk FROM ScheduleProjections"
            local_data = conn.execute_query(local_projections_query)
        
        projections_map = { f"{row['so_number']}-{row['part_number']}": row for row in local_data }

        # --- Get the split FG On Hand values and labels ---
        fg_on_hand_split = self.erp_service.get_split_fg_on_hand_value()
        
        # --- Get the total shipped value for the current month ---
        shipped_current_month = self.erp_service.get_shipped_for_current_month()

        # Step 4: Combine all data sources and perform final calculations
        for erp_row in erp_data:
            key = f"{erp_row['SO']}-{erp_row['Part']}"
            projection = projections_map.get(key)
            
            on_hand_qty = on_hand_map.get(erp_row['Part'], 0) or 0
            erp_row['On hand Qty'] = on_hand_qty

            # --- ***** RESTORED "LIVE" LOGIC ***** ---
            # Read the [Net Qty] calculated by the (now correct) SQL query
            sql_net_qty = erp_row.get('Net Qty', 0) or 0
            # Ensure it's not negative
            erp_row['Net Qty'] = float(sql_net_qty) if float(sql_net_qty) > 0 else 0.0
            # --- ***** END RESTORED LOGIC ***** ---
            
            if projection:
                erp_row['No/Low Risk Qty'] = projection.get('can_make_no_risk', 0)
                erp_row['High Risk Qty'] = projection.get('high_risk', 0)
            else:
                no_risk_val = erp_row.get('Can Make - No Risk', 0) or 0
                low_risk_val = erp_row.get('Low Risk', 0) or 0
                erp_row['No/Low Risk Qty'] = no_risk_val + low_risk_val
                erp_row['High Risk Qty'] = erp_row.get('High Risk', 0) or 0
            
            price = erp_row.get('Unit Price', 0) or 0
            erp_row['$ No/Low Risk Qty'] = (float(erp_row['No/Low Risk Qty'] or 0)) * float(price)
            erp_row['$ High Risk'] = (float(erp_row['High Risk Qty'] or 0)) * float(price)
            
            try:
                qty_per_uom = float(erp_row.get('Qty Per UoM')) if erp_row.get('Qty Per UoM') else 1.0
            except (ValueError, TypeError):
                qty_per_uom = 1.0
            
            # This calculation now uses the correct 'Net Qty' from above
            erp_row['Ext Qty'] = float(erp_row.get('Net Qty') or 0.0) * qty_per_uom
        
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