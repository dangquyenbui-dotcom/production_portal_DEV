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
    SalesQueries,
    CoCQueries # <-- ADD THIS IMPORT
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
        self.coc_queries = CoCQueries() # <-- INSTANTIATE THE NEW CLASS

    # --- Job Related Methods (for Open Jobs Page) ---
    def get_all_open_job_numbers(self):
        return self.job_queries.get_all_open_job_numbers()

    def get_open_job_headers(self, job_numbers):
        """Delegates call to JobQueries to get header info for OPEN jobs."""
        return self.job_queries.get_open_job_headers(job_numbers)

    def get_open_production_jobs(self):
        # Keeping for potential other uses, might be redundant
        return self.job_queries.get_open_production_jobs()

    def get_open_job_details(self, job_numbers):
        """Delegates call to JobQueries to get transaction details (dtfifo) for MULTIPLE jobs."""
        return self.job_queries.get_open_job_details(job_numbers)

    def get_relieve_job_data(self, job_numbers):
        """Delegates call to JobQueries to get relieve transaction details (dtfifo2) for MULTIPLE jobs."""
        return self.job_queries.get_relieve_job_data(job_numbers)

    def get_open_jobs_by_line(self, facility, line):
        return self.job_queries.get_open_jobs_by_line(facility, line)

    # ***** NEW METHOD FOR CoC Report *****
    def get_coc_report_data(self, job_number):
        """
        Fetches all necessary data (header and transactions) for a single job number
        for the Certificate of Compliance report, regardless of job status.
        Returns None if header not found, otherwise returns header + transactions.
        """
        header = self.coc_queries.get_job_header_by_number(job_number)
        if not header:
            return None # Indicate job header wasn't found

        # Pass the single job number in a list as expected by the transaction functions
        job_number_list = [str(job_number)]
        fifo_details = self.coc_queries.get_job_transaction_details(job_number) # Use CoCQueries method
        relieve_details = self.coc_queries.get_job_relieve_data(job_number)     # Use CoCQueries method

        return {
            "header": header,
            "fifo_details": fifo_details,
            "relieve_details": relieve_details
        }
    # ***** END NEW METHOD *****

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


# --- Singleton instance management ---
_erp_service_instance = None

def get_erp_service():
    """Gets the global singleton instance of the ErpService."""
    global _erp_service_instance
    if _erp_service_instance is None:
        print("ℹ️ Creating new ErpService instance.")
        _erp_service_instance = ErpService()
    return _erp_service_instance

# Optional: Function to explicitly close the connection if needed during shutdown/testing
def close_erp_connection():
    """Explicitly closes the shared ERP database connection."""
    conn_instance = get_erp_db_connection()
    if conn_instance:
        conn_instance.close()
    # If using singleton for ErpService, you might want to clear it too
    global _erp_service_instance
    _erp_service_instance = None