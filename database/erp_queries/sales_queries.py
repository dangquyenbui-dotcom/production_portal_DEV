# database/erp_queries/sales_queries.py
"""
ERP Queries related to Sales Orders and Shipments.
"""
from database.erp_connection_base import get_erp_db_connection
from datetime import datetime, timedelta

class SalesQueries:
    """Contains ERP query methods specific to Sales."""

    def get_split_fg_on_hand_value(self):
        """
        Calculates the value of Finished Goods inventory split into three date buckets
        relative to the 21st of the previous and current month.
        """
        db = get_erp_db_connection()
        if not db:
            return {'label1': 'FG On Hand - Prior', 'value1': 0,
                    'label2': 'FG On Hand - Mid', 'value2': 0,
                    'label3': 'FG On Hand - Recent', 'value3': 0}

        today = datetime.now()
        first_of_this_month = today.replace(day=1)
        last_of_previous_month = first_of_this_month - timedelta(days=1)
        prior_cutoff_date = last_of_previous_month.replace(day=21)
        current_cutoff_date = today.replace(day=21)
        current_twentieth_date = today.replace(day=20) # End of middle bucket

        date_format = '%m/%d/%y'
        prior_cutoff_str = prior_cutoff_date.strftime(date_format)
        current_twentieth_str = current_twentieth_date.strftime(date_format)
        current_cutoff_str = current_cutoff_date.strftime(date_format)

        label1 = f"FG On Hand - Prior {prior_cutoff_str}"
        label2 = f"FG On Hand - {prior_cutoff_str} To {current_twentieth_str}"
        label3 = f"FG On Hand - From {current_cutoff_str}"

        sql = """
            SELECT
                SUM(CASE WHEN f.fi_lotdate < ? THEN f.fi_balance * p.pr_lispric ELSE 0 END) AS value1,
                SUM(CASE WHEN f.fi_lotdate >= ? AND f.fi_lotdate < ? THEN f.fi_balance * p.pr_lispric ELSE 0 END) AS value2,
                SUM(CASE WHEN f.fi_lotdate >= ? THEN f.fi_balance * p.pr_lispric ELSE 0 END) AS value3
            FROM dtfifo f
            JOIN dmprod p ON f.fi_prid = p.pr_id
            JOIN dmware w ON f.fi_waid = w.wa_id
            WHERE f.fi_balance > 0 AND p.pr_codenum LIKE 'T%' AND w.wa_name IN ('DUARTE', 'IRWINDALE');
        """
        # Note: The logic requires date objects for comparison in pyodbc
        params = (prior_cutoff_date.date(), prior_cutoff_date.date(), current_cutoff_date.date(), current_cutoff_date.date())
        result = db.execute_query(sql, params)

        if result:
            return {
                'label1': label1, 'value1': result[0]['value1'] or 0,
                'label2': label2, 'value2': result[0]['value2'] or 0,
                'label3': label3, 'value3': result[0]['value3'] or 0
            }
        return {'label1': label1, 'value1': 0, 'label2': label2, 'value2': 0, 'label3': label3, 'value3': 0}

    def get_shipped_for_current_month(self):
        """Calculates the total value of orders shipped in the current calendar month."""
        db = get_erp_db_connection()
        if not db: return 0
        sql = """
            SELECT SUM(det.or_shipquant * det.or_price) AS total_shipped_value
            FROM dttord ord
            INNER JOIN dtord det ON ord.to_id = det.or_toid
            WHERE ord.to_shipped IS NOT NULL AND ord.to_status = 'c'
              AND ord.to_ordtype IN ('s', 'm')
              AND MONTH(ord.to_shipped) = MONTH(GETDATE())
              AND YEAR(ord.to_shipped) = YEAR(GETDATE());
        """
        result = db.execute_query(sql)
        return result[0]['total_shipped_value'] if result and result[0]['total_shipped_value'] is not None else 0

    def get_open_order_schedule(self):
        """Retrieves a detailed list of all open sales orders with related production and risk data."""
        db = get_erp_db_connection()
        if not db: return []
        # This SQL is now corrected to match the original "Live" logic for [Net Qty]
        sql = """
            WITH LatestOrderStatus AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY to_ordnum ORDER BY to_id DESC) as rn
                FROM dttord WHERE to_ordtype IN ('s', 'h', 'd', 'm', 'l')
            ), OpenOrders AS (
                SELECT to_ordnum, to_billpo, to_ordtype, to_id as latest_to_id, to_wanted, to_promise,
                       to_orddate, to_dueship, to_s1id, to_biid, to_notes, to_waid
                FROM LatestOrderStatus
                WHERE rn = 1 AND to_ordtype IN ('s', 'h', 'm', 'l') AND to_shipped IS NULL
            ), PrimarySalesRep AS (
                 SELECT s2_recid, s2_table,
                 COALESCE(
                    MAX(CASE WHEN sm.sm_lname != 'HOUSE ACCOUNT' THEN sm.sm_lname END),
                    MAX(sm.sm_lname)
                 ) as primary_rep
                 FROM dmsman2 s2 INNER JOIN dmsman sm ON s2.s2_smid = sm.sm_id WHERE s2_table = 'dmbill' GROUP BY s2_recid, s2_table
            ), RiskData AS (
                SELECT
                    d2_recid as latest_to_id,
                    MAX(CASE WHEN d1_field = 'u_No_Risk' THEN d2_value END) AS no_risk_value,
                    MAX(CASE WHEN d1_field = 'u_Low_Risk' THEN d2_value END) AS low_risk_value,
                    MAX(CASE WHEN d1_field = 'u_High_Risk' THEN d2_value END) AS high_risk_value,
                    MAX(CASE WHEN d1_field = 'u_Schedule_Note' THEN d2_value END) AS schedule_note_value
                FROM dtd2
                INNER JOIN dmd1 ON dtd2.d2_d1id = dmd1.d1_id
                WHERE dmd1.d1_table = 'dttord'
                  AND dmd1.d1_field IN ('u_No_Risk', 'u_Low_Risk', 'u_High_Risk', 'u_Schedule_Note')
                GROUP BY d2_recid
            ), AggregatedOrderData AS (
                 SELECT oo.to_ordnum, oo.to_billpo, oo.to_ordtype, oo.to_wanted, oo.to_promise, oo.to_orddate,
                        oo.to_dueship, oo.to_s1id, oo.to_biid, oo.to_notes, oo.latest_to_id, oo.to_waid,
                        p.pr_codenum, p.pr_descrip, p.pr_user5, p.pr_caid, p.pr_unid, p.pr_user3, p.pr_id, o.or_price,
                    SUM(o.or_quant) as total_current_qty,
                    CASE WHEN oo.to_ordnum % 100 = 0 THEN SUM(o.or_quant)
                         ELSE COALESCE((SELECT TOP 1 orig_o.or_quant FROM dtord orig_o INNER JOIN dmprod orig_p ON orig_o.or_prid = orig_p.pr_id INNER JOIN dttord orig_t ON orig_o.or_ordnum = orig_t.to_ordnum AND orig_o.or_toid = orig_t.to_id WHERE orig_o.or_ordnum = (oo.to_ordnum - (oo.to_ordnum % 100)) AND orig_p.pr_codenum = p.pr_codenum ORDER BY orig_t.to_id DESC), SUM(o.or_quant))
                    END AS total_original_qty,
                    ROW_NUMBER() OVER (PARTITION BY oo.to_ordnum ORDER BY p.pr_codenum, o.or_price DESC) as line_sequence
                FROM OpenOrders oo
                INNER JOIN dtord o ON oo.to_ordnum = o.or_ordnum AND o.or_toid = oo.latest_to_id
                INNER JOIN dmprod p ON o.or_prid = p.pr_id WHERE p.pr_codenum LIKE 'T%'
                GROUP BY oo.to_ordnum, oo.to_billpo, oo.to_ordtype, oo.to_wanted, oo.to_promise, oo.to_orddate, oo.to_dueship, oo.to_s1id, oo.to_biid, oo.to_notes, oo.latest_to_id, oo.to_waid, p.pr_codenum, p.pr_descrip, p.pr_user5, p.pr_caid, p.pr_unid, p.pr_user3, p.pr_id, o.or_price
            ), ProducedQuantities AS (
                SELECT lj.lj_ordnum as SalesOrder, p.pr_codenum as PartNumber, SUM(COALESCE(j4.j4_quant, 0)) as TotalProducedQty
                FROM dtjob j INNER JOIN dtljob lj ON j.jo_jobnum = lj.lj_jobnum INNER JOIN dmprod p ON lj.lj_prid = p.pr_id LEFT JOIN dtjob4 j4 ON j.jo_jobnum = j4.j4_jobnum AND lj.lj_id = j4.j4_ljid
                WHERE p.pr_codenum LIKE 'T%' GROUP BY lj.lj_ordnum, p.pr_codenum
            ), TotalShippedQuantities AS (
                 SELECT FLOOR(ord.to_ordnum / 100) * 100 AS original_so_num, prod.pr_codenum, SUM(det.or_shipquant) AS total_shipped
                 FROM dttord ord INNER JOIN dtord det ON ord.to_id = det.or_toid INNER JOIN dmprod prod ON det.or_prid = prod.pr_id
                 WHERE ord.to_shipped IS NOT NULL AND ord.to_status = 'c' AND ord.to_ordtype IN ('s', 'm')
                 GROUP BY FLOOR(ord.to_ordnum / 100) * 100, prod.pr_codenum
            )
            SELECT
                aod.latest_to_id, COALESCE(wa.wa_name, 'N/A') AS [Facility], CASE WHEN ca.ca_name = 'Stick Pack' THEN 'SP' ELSE 'BPS' END AS [BU],
                aod.to_ordnum AS [SO], aod.to_billpo AS [Bill to PO],
                CASE aod.to_ordtype WHEN 's' THEN 'Sales Order' WHEN 'h' THEN 'Credit Hold' WHEN 'm' THEN 'ICT Order' WHEN 'l' THEN 'On Hold Order' ELSE 'Other' END AS [SO Type],
                aod.pr_codenum AS [Part], COALESCE(p1.p1_name, 'N/A') AS [Customer Name], aod.pr_descrip AS [Description],
                aod.total_original_qty AS [Ord Qty - (00) Level], COALESCE(tsq.total_shipped, 0) AS [Total Shipped Qty],
                aod.total_current_qty AS [Ord Qty - Cur. Level], COALESCE(pq.TotalProducedQty, 0) AS [Produced Qty],

                -- ***** THIS IS THE CORRECTED "LIVE" SQL LOGIC *****
                (aod.total_original_qty - COALESCE(tsq.total_shipped, 0) - COALESCE(pq.TotalProducedQty, 0)) AS [Net Qty],
                -- ***** END CORRECTION *****

                CASE WHEN aod.line_sequence = 1 AND rd.no_risk_value IS NOT NULL AND ISNUMERIC(rd.no_risk_value) = 1 THEN CAST(rd.no_risk_value AS NUMERIC(18,2)) ELSE 0 END AS [Can Make - No Risk],
                CASE WHEN aod.line_sequence = 1 AND rd.low_risk_value IS NOT NULL AND ISNUMERIC(rd.low_risk_value) = 1 THEN CAST(rd.low_risk_value AS NUMERIC(18,2)) ELSE 0 END AS [Low Risk],
                CASE WHEN aod.line_sequence = 1 AND rd.high_risk_value IS NOT NULL AND ISNUMERIC(rd.high_risk_value) = 1 THEN CAST(rd.high_risk_value AS NUMERIC(18,2)) ELSE 0 END AS [High Risk],
                COALESCE(un.un_name, 'N/A') AS [UoM], COALESCE(aod.pr_user3, '') AS [Qty Per UoM],
                CASE WHEN ISNUMERIC(aod.pr_user3) = 1 AND aod.pr_user3 <> '' THEN aod.total_current_qty * CAST(aod.pr_user3 AS NUMERIC(18,2)) ELSE aod.total_current_qty END AS [Ext Qty (Current x per UoM)],
                aod.or_price AS [Unit Price], aod.total_current_qty * aod.or_price AS [Ext $ (Current x Price)],
                -- This will now use the correctly calculated [Net Qty] from above
                ( (aod.total_original_qty - COALESCE(tsq.total_shipped, 0) - COALESCE(pq.TotalProducedQty, 0)) * aod.or_price ) AS [Ext $ (Net Qty x Price)],
                CASE WHEN aod.line_sequence = 1 AND rd.no_risk_value IS NOT NULL AND ISNUMERIC(rd.no_risk_value) = 1 THEN CAST(rd.no_risk_value AS NUMERIC(18,2)) * aod.or_price ELSE 0 END AS [$ Can Make - No Risk],
                CASE WHEN aod.line_sequence = 1 AND rd.low_risk_value IS NOT NULL AND ISNUMERIC(rd.low_risk_value) = 1 THEN CAST(rd.low_risk_value AS NUMERIC(18,2)) * aod.or_price ELSE 0 END AS [$ Low Risk],
                CASE WHEN aod.line_sequence = 1 AND rd.high_risk_value IS NOT NULL AND ISNUMERIC(rd.high_risk_value) = 1 THEN CAST(rd.high_risk_value AS NUMERIC(18,2)) * aod.or_price ELSE 0 END AS [$ High Risk],
                CONVERT(VARCHAR, aod.to_dueship, 101) AS [Due to Ship], CONVERT(VARCHAR, aod.to_wanted, 101) AS [Requested Date],
                CONVERT(VARCHAR, aod.to_promise, 101) AS [Comp Arrived Date], CONVERT(VARCHAR, aod.to_orddate, 101) AS [Ordered Date],
                COALESCE(psr.primary_rep, sm_order.sm_lname, 'N/A') AS [Sales Rep], COALESCE(rd.schedule_note_value, '') AS [Schedule Note]
            FROM AggregatedOrderData aod
            LEFT JOIN dmpr1 p1 ON aod.pr_user5 = p1.p1_id
            LEFT JOIN dmcats ca ON aod.pr_caid = ca.ca_id
            LEFT JOIN dmunit un ON aod.pr_unid = un.un_id
            LEFT JOIN dmware wa ON aod.to_waid = wa.wa_id
            LEFT JOIN dmsman sm_order ON aod.to_s1id = sm_order.sm_id
            LEFT JOIN PrimarySalesRep psr ON aod.to_biid = psr.s2_recid AND psr.s2_table = 'dmbill'
            LEFT JOIN RiskData rd ON aod.latest_to_id = rd.latest_to_id
            LEFT JOIN ProducedQuantities pq ON CAST(aod.to_ordnum AS VARCHAR) = pq.SalesOrder AND aod.pr_codenum = pq.PartNumber
            LEFT JOIN TotalShippedQuantities tsq ON (FLOOR(aod.to_ordnum / 100) * 100) = tsq.original_so_num AND aod.pr_codenum = tsq.pr_codenum
            ORDER BY aod.to_ordnum DESC, aod.pr_codenum;
        """
        return db.execute_query(sql)