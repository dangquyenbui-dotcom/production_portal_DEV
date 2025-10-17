# database/erp_service.py
"""
ERP Service Layer
Acts as a facade, coordinating calls to specific ERP query modules.
"""
from .erp_connection_base import get_erp_db_connection # Import base connection getter
from .erp_queries import (
    JobQueries,
    InventoryQueries,
    POQueries,
    QCQueries,
    BOMQueries,
    SalesQueries
)

class ErpService:
    """Contains all business logic for querying the ERP database by delegating to query classes."""

    def __init__(self):
        # Instantiate query classes here. They will use the shared connection instance internally.
        self.job_queries = JobQueries()
        self.inventory_queries = InventoryQueries()
        self.po_queries = POQueries()
        self.qc_queries = QCQueries()
        self.bom_queries = BOMQueries()
        self.sales_queries = SalesQueries()

    # --- Job Related Methods ---
    def get_all_open_job_numbers(self):
        return self.job_queries.get_all_open_job_numbers()

    # --- ADD THIS METHOD ---
    def get_open_job_headers(self, job_numbers):
        """Delegates call to JobQueries to get header info."""
        return self.job_queries.get_open_job_headers(job_numbers)
    # --- END ADD ---

    def get_open_production_jobs(self):
        # This might be redundant now, consider removing if get_open_job_headers covers needs
        return self.job_queries.get_open_production_jobs()

    def get_open_job_details(self, job_numbers):
        """Delegates call to JobQueries to get transaction details."""
        return self.job_queries.get_open_job_details(job_numbers)

    def get_relieve_job_data(self, job_numbers):
        """Delegates call to JobQueries to get relieve transaction details."""
        return self.job_queries.get_relieve_job_data(job_numbers)

    def get_open_jobs_by_line(self, facility, line):
        return self.job_queries.get_open_jobs_by_line(facility, line)

    # --- Inventory Related Methods ---
    def get_raw_material_inventory(self):
        return self.inventory_queries.get_raw_material_inventory()

    def get_on_hand_inventory(self):
        return self.inventory_queries.get_on_hand_inventory()

    # --- PO Related Methods ---
    def get_purchase_order_data(self):
        return self.po_queries.get_purchase_order_data()

    def get_detailed_purchase_order_data(self):
        return self.po_queries.get_detailed_purchase_order_data()

    # --- QC Related Methods ---
    def get_qc_pending_data(self):
        return self.qc_queries.get_qc_pending_data()

    # --- BOM Related Methods ---
    def get_bom_data(self, parent_part_number=None):
        return self.bom_queries.get_bom_data(parent_part_number)

    # --- Sales Related Methods ---
    def get_split_fg_on_hand_value(self):
        return self.sales_queries.get_split_fg_on_hand_value()

    def get_shipped_for_current_month(self):
        return self.sales_queries.get_shipped_for_current_month()

    def get_open_order_schedule(self):
        return self.sales_queries.get_open_order_schedule()


# --- Singleton instance management for the Service ---
_erp_service_instance = None

def get_erp_service():
    """Gets the global singleton instance of the ErpService."""
    global _erp_service_instance
    if _erp_service_instance is None:
        print("ℹ️ Creating new ErpService instance.")
        _erp_service_instance = ErpService()
    return _erp_service_instance

def close_erp_connection():
    """Explicitly closes the shared ERP database connection."""
    global _erp_connection_instance
    conn_instance = get_erp_db_connection()
    if conn_instance:
        conn_instance.close()
    _erp_service_instance = None