# database/erp_queries/qc_queries.py
"""
ERP Queries related to Quality Control.
"""
from database.erp_connection_base import get_erp_db_connection

class QCQueries:
    """Contains ERP query methods specific to Quality Control."""

    def get_qc_pending_data(self):
        """
        Retrieves inventory items currently in 'QC Pending' status.
        """
        db = get_erp_db_connection()
        if not db: return []
        sql = """
            SELECT
                fi.fi_id as "Inventory ID", fi.fi_lotnum as "System Lot", fi.fi_userlot as "User Lot",
                fi.fi_date as "Transaction Date", fi.fi_postref as "Post Reference", fi.fi_action as "Action",
                fi.fi_quant as "Quantity", fi.fi_balance as "Balance",
                pr.pr_codenum as "Part Number", pr.pr_descrip as "Product Description",
                wa.wa_name as "Facility", qa.qa_qfid as "QC Frequency ID", qf.qf_date as "QC Assignment Date"
            FROM dtfifo fi
            INNER JOIN dtqcfreqassgn qa ON fi.fi_lotnum = qa.qa_lotnum
            INNER JOIN dtqcfreq qf ON qa.qa_qfid = qf.qf_id
            LEFT JOIN dmprod pr ON fi.fi_prid = pr.pr_id
            LEFT JOIN dmware wa ON fi.fi_waid = wa.wa_id
            WHERE fi.fi_qc = 'Pending' AND fi.fi_balance > 0 AND fi.fi_quant > 0
            ORDER BY fi.fi_date DESC, fi.fi_lotnum;
        """
        return db.execute_query(sql)