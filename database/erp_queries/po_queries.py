# database/erp_queries/po_queries.py
"""
ERP Queries related to Purchase Orders.
"""
from database.erp_connection_base import get_erp_db_connection

class POQueries:
    """Contains ERP query methods specific to Purchase Orders."""

    def get_purchase_order_data(self):
        """
        Fetches a summary of open PO quantities grouped by part number.
        """
        db = get_erp_db_connection()
        if not db: return []
        sql = """
            SELECT
                pur.pu_ourcode AS "Part Number",
                SUM(ISNULL(pur.pu_quant, 0) - ISNULL(pur.pu_recman, 0)) AS "OpenPOQuantity"
            FROM dtpur AS pur
            INNER JOIN dttpur AS tp ON pur.pu_purnum = tp.tp_purnum
            WHERE (ISNULL(pur.pu_quant, 0) - ISNULL(pur.pu_recman, 0)) > 0
              AND tp.tp_ordtype = 'p' AND tp.tp_recevd IS NULL
            GROUP BY pur.pu_ourcode;
        """
        return db.execute_query(sql)

    def get_detailed_purchase_order_data(self):
        """
        Fetches detailed line information for all open purchase orders.
        """
        db = get_erp_db_connection()
        if not db: return []
        sql = """
            SELECT
                tp.tp_purnum AS "PO Number", pur.pu_ourcode AS "Part Number",
                p.pr_descrip AS "Part Description", v.ve_name AS "Vendor Description",
                pur.pu_quant AS "Ordered Quantity", pur.pu_recman AS "Received Quantity",
                (ISNULL(pur.pu_quant, 0) - ISNULL(pur.pu_recman, 0)) AS "Open Quantity",
                pur.pu_promise AS "Promise Date", pur.pu_wanted AS "Due Date",
                'Open' AS "Line Status", 'N/A' AS "MRP Status"
            FROM dtpur AS pur
            INNER JOIN dttpur AS tp ON pur.pu_purnum = tp.tp_purnum
            LEFT JOIN dmprod p ON pur.pu_ourcode = p.pr_codenum
            LEFT JOIN dmvend v ON tp.tp_veid = v.ve_id
            WHERE (ISNULL(pur.pu_quant, 0) - ISNULL(pur.pu_recman, 0)) > 0
              AND tp.tp_ordtype = 'p' AND tp.tp_recevd IS NULL
            ORDER BY tp.tp_purnum DESC, pur.pu_ourcode ASC;
        """
        return db.execute_query(sql)