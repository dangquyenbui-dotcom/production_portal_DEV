# dangquyenbui-dotcom/production_portal_dev/production_portal_DEV-35c5b2d7d65c0b0de1b2129d9ecd46a5ad103507/database/erp_connection.py
"""
Dedicated ERP Database Connection Service.
This is separate from the main application's database connection.
"""
import pyodbc
import traceback
from config import Config
from datetime import datetime, timedelta

class ERPConnection:
    # ... (this class remains unchanged) ...
    def __init__(self):
        self.connection = None
        self._connection_string = None # Store the successful connection string

        # Prioritized list of potential drivers to try (braces removed)
        drivers_to_try = [
            Config.ERP_DB_DRIVER,  # First, try the one from .env (e.g., 'ODBC Driver 18 for SQL Server')
            'ODBC Driver 18 for SQL Server', # Recommended for ARM64 and newer systems
            'ODBC Driver 17 for SQL Server', # A common modern driver
            'SQL Server Native Client 11.0', # Another common one
            'SQL Server' # Older, but often present as a fallback
        ]

        # Remove duplicates while preserving order
        drivers = list(dict.fromkeys(drivers_to_try))

        for driver in drivers:
            if not driver:
                continue
            try:
                # The f-string correctly adds the necessary braces around the driver name
                connection_string = (
                    f"DRIVER={{{driver}}};"
                    f"SERVER={Config.ERP_DB_SERVER},{Config.ERP_DB_PORT};"
                    f"DATABASE={Config.ERP_DB_NAME};"
                    f"UID={Config.ERP_DB_USERNAME};"
                    f"PWD={Config.ERP_DB_PASSWORD};"
                    f"TrustServerCertificate=yes;"
                    f"Connection Timeout={Config.ERP_DB_TIMEOUT};"
                )
                self.connection = pyodbc.connect(connection_string, autocommit=True)
                print(f"✅ [ERP_DB] Connection successful using driver: {driver}")
                self._connection_string = connection_string  # Save the working string
                break  # Exit loop on successful connection
            except pyodbc.Error:
                print(f"ℹ️  [ERP_DB] Driver {driver} failed. Trying next...")
                continue  # Try the next driver in the list

        if not self.connection:
            print(f"❌ [ERP_DB] FATAL: Connection failed. All attempted drivers were unsuccessful.")

    def execute_query(self, sql, params=None):
        """Executes a SQL query and returns results as a list of dicts."""
        if not self.connection:
            print("❌ [ERP_DB] Cannot execute query, no active connection.")
            return []

        try:
            cursor = self.connection.cursor()
            cursor.execute(sql, params or [])
            if cursor.description:
                columns = [column[0] for column in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                cursor.close()
                return results
            cursor.close()
            return []
        except pyodbc.Error as e:
            print(f"❌ [ERP_DB] Query Failed: {e}")
            traceback.print_exc()
            return []
        except Exception as e:
            print(f"❌ [ERP_DB] Unexpected error: {e}")
            traceback.print_exc()
            return []

def get_erp_db():
    """
    Gets a fresh instance of the ERP connection to ensure data is not stale.
    This is intentionally NOT a singleton for data refresh purposes.
    """
    return ERPConnection()

class ErpService:
    """Contains all business logic for querying the ERP database."""

    # --- NEW METHOD ---
    def get_all_open_job_numbers(self):
        """ Retrieves just the job numbers for all open production jobs ('a' type). """
        db = get_erp_db()
        sql = """
            SELECT
                j.jo_jobnum
            FROM
                dtjob j
            WHERE
                j.jo_closed IS NULL
                AND j.jo_type = 'a'
            ORDER BY
                j.jo_jobnum ASC;
        """
        results = db.execute_query(sql)
        # Extract just the numbers into a list
        return [row['jo_jobnum'] for row in results] if results else []
    # --- END NEW METHOD ---


    def get_open_production_jobs(self):
        """
        Retrieves all open production jobs ('a' type) that are linked to a sales order,
        including the job's target quantity and its completed quantity.
        """
        db = get_erp_db()
        sql = """
            SELECT
                j.jo_jobnum,
                (SELECT TOP 1 lj.lj_ordnum FROM dtljob lj WHERE lj.lj_jobnum = j.jo_jobnum ORDER BY lj.lj_linenum) as so_number,
                (SELECT TOP 1 lj.lj_quant FROM dtljob lj WHERE lj.lj_jobnum = j.jo_jobnum ORDER BY lj.lj_linenum) as job_quantity,
                (SELECT SUM(ISNULL(j4.j4_quant, 0)) FROM dtjob4 j4 WHERE j4.j4_jobnum = j.jo_jobnum) as completed_quantity
            FROM
                dtjob j
            WHERE
                j.jo_closed IS NULL
                AND j.jo_type = 'a'
                AND EXISTS (SELECT 1 FROM dtljob lj WHERE lj.lj_jobnum = j.jo_jobnum AND lj.lj_ordnum IS NOT NULL);
        """
        return db.execute_query(sql)

    def get_open_job_details(self, job_numbers):
        """
        Retrieves details for a list of open jobs from the dtfifo table.
        """
        if not job_numbers:
            return []

        db = get_erp_db()

        placeholders = ', '.join(['?'] * len(job_numbers))

        sql = f"""
            SELECT
                f.fi_postref,
                f.fi_action,
                f.fi_quant,
                f.fi_prid,
                p.pr_codenum AS part_number,
                p.pr_descrip AS part_description
            FROM dtfifo f
            LEFT JOIN dmprod p ON f.fi_prid = p.pr_id
            WHERE f.fi_postref IN ({placeholders})
        """

        params = [f'JJ-{job_number}' for job_number in job_numbers]

        return db.execute_query(sql, params)

    def get_relieve_job_data(self, job_numbers):
        """
        Retrieves relieve job data from the dtfifo2 table.
        """
        if not job_numbers:
            return []

        db = get_erp_db()

        placeholders = ', '.join(['?'] * len(job_numbers))

        sql = f"""
            SELECT
                f2.f2_postref,
                f2.f2_action,
                f2.f2_prid,
                (f2.f2_oldquan - f2.f2_newquan) AS net_quantity,
                p.pr_codenum AS part_number,
                p.pr_descrip AS part_description
            FROM dtfifo2 f2
            LEFT JOIN dmprod p ON f2.f2_prid = p.pr_id
            WHERE f2.f2_postref IN ({placeholders})
            AND f2.f2_action = 'Relieve Job'
        """

        params = [f'JJ-{job_number}' for job_number in job_numbers]

        return db.execute_query(sql, params)

    # ... (all other existing methods like get_raw_material_inventory, get_bom_data, etc. remain here exactly as they were) ...
    def get_raw_material_inventory(self):
        """
        Retrieves all raw material inventory, categorized by status, based on the provided JS logic.
        """
        db = get_erp_db()
        sql = """
            SELECT
                p.pr_codenum AS PartNumber,
                -- Truly Available (Approved, not tied to job/staging/quarantine)
                SUM(CASE
                    WHEN f.fi_type NOT IN ('quarantine', 'job', 'staging')
                    AND (f.fi_qc IS NULL OR f.fi_qc <> 'Pending')
                    THEN f.fi_balance ELSE 0 END) AS on_hand_approved,
                -- Pending QC
                SUM(CASE WHEN f.fi_qc = 'Pending' THEN f.fi_balance ELSE 0 END) AS on_hand_pending_qc,
                -- Quarantine
                SUM(CASE WHEN f.fi_type = 'quarantine' THEN f.fi_balance ELSE 0 END) AS on_hand_quarantine,
                -- Issued to Jobs
                SUM(CASE WHEN f.fi_type = 'job' AND f.fi_action = 'Issued inventory' THEN f.fi_balance ELSE 0 END) AS issued_to_job,
                -- Staged for Production
                SUM(CASE WHEN f.fi_type = 'staging' THEN f.fi_balance ELSE 0 END) AS staged
            FROM
                dtfifo f
            JOIN
                dmprod p ON f.fi_prid = p.pr_id
            WHERE
                f.fi_balance > 0
                AND p.pr_codenum NOT LIKE 'T%' -- Exclude Finished Goods
            GROUP BY
                p.pr_codenum;
        """
        return db.execute_query(sql)

    def get_purchase_order_data(self):
        """
        Fetches genuinely open purchase order lines, based on the provided JS logic.
        """
        db = get_erp_db()
        # This logic mirrors the s_po Javascript logic:
        # - Joins dtpur and dttpur on the PO number
        # - Checks for open quantity (pu_quant > pu_recman)
        # - Filters for purchase orders (tp_ordtype = 'p')
        # - Filters for POs not marked as fully received (tp_recevd IS NULL)
        sql = """
            SELECT
                pur.pu_ourcode AS "Part Number",
                SUM(ISNULL(pur.pu_quant, 0) - ISNULL(pur.pu_recman, 0)) AS "OpenPOQuantity"
            FROM
                dtpur AS pur
            INNER JOIN
                dttpur AS tp ON pur.pu_purnum = tp.tp_purnum
            WHERE
                (ISNULL(pur.pu_quant, 0) - ISNULL(pur.pu_recman, 0)) > 0
                AND tp.tp_ordtype = 'p'
                AND tp.tp_recevd IS NULL
            GROUP BY
                pur.pu_ourcode;
        """
        return db.execute_query(sql)

    def get_detailed_purchase_order_data(self):
        """
        Fetches detailed information for all genuinely open purchase order lines,
        intended for the PO viewer UI.
        """
        db = get_erp_db()
        sql = """
            SELECT
                tp.tp_purnum AS "PO Number",
                pur.pu_ourcode AS "Part Number",
                p.pr_descrip AS "Part Description",
                v.ve_name AS "Vendor Description",
                pur.pu_quant AS "Ordered Quantity",
                pur.pu_recman AS "Received Quantity",
                (ISNULL(pur.pu_quant, 0) - ISNULL(pur.pu_recman, 0)) AS "Open Quantity",
                pur.pu_promise AS "Promise Date",
                pur.pu_wanted AS "Due Date",
                'Open' AS "Line Status",
                'N/A' AS "MRP Status"
            FROM
                dtpur AS pur
            INNER JOIN
                dttpur AS tp ON pur.pu_purnum = tp.tp_purnum
            LEFT JOIN
                dmprod p ON pur.pu_ourcode = p.pr_codenum
            LEFT JOIN
                dmvend v ON tp.tp_veid = v.ve_id
            WHERE
                (ISNULL(pur.pu_quant, 0) - ISNULL(pur.pu_recman, 0)) > 0
                AND tp.tp_ordtype = 'p'
                AND tp.tp_recevd IS NULL
            ORDER BY
                tp.tp_purnum DESC, pur.pu_ourcode ASC;
        """
        return db.execute_query(sql)

    def get_qc_pending_data(self):
        """
        Retrieves all inventory items that are currently in a 'QC Pending' status.
        """
        db = get_erp_db()
        sql = """
            -- Updated SQL query using fi_balance > 0 to get current QC Pending items
            SELECT
                fi.fi_id as "Inventory ID",
                fi.fi_lotnum as "System Lot",
                fi.fi_userlot as "User Lot",
                fi.fi_date as "Transaction Date",
                fi.fi_postref as "Post Reference",
                fi.fi_action as "Action",
                fi.fi_quant as "Quantity",
                fi.fi_balance as "Balance",
                pr.pr_codenum as "Part Number",
                pr.pr_descrip as "Product Description",
                wa.wa_name as "Facility",
                qa.qa_qfid as "QC Frequency ID",
                qf.qf_date as "QC Assignment Date"
            FROM dtfifo fi
            INNER JOIN dtqcfreqassgn qa ON fi.fi_lotnum = qa.qa_lotnum
            INNER JOIN dtqcfreq qf ON qa.qa_qfid = qf.qf_id
            LEFT JOIN dmprod pr ON fi.fi_prid = pr.pr_id
            LEFT JOIN dmware wa ON fi.fi_waid = wa.wa_id
            WHERE fi.fi_qc = 'Pending'
                AND fi.fi_balance > 0  -- Only current active inventory
                AND fi.fi_quant > 0
            ORDER BY fi.fi_date DESC, fi.fi_lotnum;
        """
        return db.execute_query(sql)

    def get_bom_data(self, parent_part_number=None):
        db = get_erp_db()
        sql = """
            -- Comprehensive BOM Query - All Active BOMs with Latest Revisions
            WITH LatestBOMRevisions AS (
                -- Find the latest revision ID for each parent product
                SELECT
                    bom.bo_bomfor as parent_product_id,
                    MAX(bom.bo_reid) as latest_revision_id
                FROM dmbom bom
                INNER JOIN dmprod parent ON bom.bo_bomfor = parent.pr_id
                WHERE parent.pr_active = 1
                GROUP BY bom.bo_bomfor
            )
            SELECT
                -- Basic BOM Information
                bom.bo_seq as Seq,
                comp.pr_codenum as "Part Number",
                comp.pr_descrip as Description,
                bom_unit.un_name as Unit,
                bom.bo_quant as Quantity,

                -- MRP Critical Information
                bom.bo_scrap as "Scrap %",
                bom.bo_overage as "Overage %",
                bom.bo_overissue as "Overissue %",
                bom.bo_incqty as "Incremental Qty",

                -- Lot and Quality Control
                CASE WHEN bom.bo_uselot = 1 THEN 'Yes' ELSE 'No' END as "Lot Tracking",
                CASE WHEN bom.bo_useexp = 1 THEN 'Yes' ELSE 'No' END as "Expiration Tracking",

                -- Product Category
                cat.ca_name as "Product Category",

                -- BOM Calculation Method
                bom.bo_bomcalc as "Calculation Method",

                -- Flags for MRP Processing
                CASE WHEN bom.bo_costonly = 1 THEN 'Yes' ELSE 'No' END as "Costing Only",
                CASE WHEN bom.bo_byproduct = 1 THEN 'Yes' ELSE 'No' END as "Byproduct",
                CASE WHEN bom.bo_subtot = 1 THEN 'Yes' ELSE 'No' END as "Subtotal",
                CASE WHEN bom.bo_reqseq = 1 THEN 'Yes' ELSE 'No' END as "Sequential",

                -- Shelf Life Requirements
                bom.bo_shelfdays as "Shelf Life Days",
                bom.bo_shelfpct as "Shelf Life %",
                bom.bo_minage as "Min Age Days",
                bom.bo_maxage as "Max Age Days",

                -- Revision Information
                bom.bo_reid as "Revision ID",

                -- Designator and Notes
                bom.bo_desig as Designator,
                bom.bo_notes as Notes,

                -- Component Product ID
                comp.pr_id as "Component ID",

                -- Parent Product Information
                parent.pr_id as "Parent ID",
                parent.pr_codenum as "Parent Part Number",
                parent.pr_descrip as "Parent Description",

                -- Unit Conversion Factors
                bom_unit.un_factor as "Unit Factor",

                -- MRP Planning Parameters
                comp.pr_reorder as "Reorder Point",
                comp.pr_minquant as "Min Order Qty",
                comp.pr_orddays as "Lead Time Days",
                comp.pr_stocked as "Stocked Item",
                comp.pr_make as "Make Item",
                comp.pr_purable as "Purchase Item"

            FROM dmbom bom

            -- Join with component product information
            INNER JOIN dmprod comp ON bom.bo_prid = comp.pr_id

            -- Join with parent product information
            INNER JOIN dmprod parent ON bom.bo_bomfor = parent.pr_id

            -- Join with product category
            INNER JOIN dmcats cat ON comp.pr_caid = cat.ca_id

            -- Join with units of measure
            LEFT JOIN dmunit bom_unit ON bom.bo_unid = bom_unit.un_id

            -- Join with the latest revision CTE
            INNER JOIN LatestBOMRevisions ON bom.bo_bomfor = LatestBOMRevisions.parent_product_id
                                         AND bom.bo_reid = LatestBOMRevisions.latest_revision_id
            WHERE comp.pr_active = 1 AND parent.pr_active = 1
        """
        params = []
        if parent_part_number:
            sql += " AND parent.pr_codenum = ? "
            params.append(parent_part_number)

        sql += " ORDER BY parent.pr_codenum, bom.bo_seq"

        return db.execute_query(sql, params)

    def get_open_jobs_by_line(self, facility, line):
        db = get_erp_db()
        sql = """
            SELECT DISTINCT
                j.jo_jobnum AS JobNumber,
                CASE j.jo_waid
                    WHEN 1 THEN 'IRWINDALE'
                    WHEN 2 THEN 'DUARTE'
                    WHEN 3 THEN 'AREA_3'
                    ELSE 'UNKNOWN'
                END AS Facility,
                ISNULL(p.pr_codenum, 'UNKNOWN') AS PartNumber,
                ISNULL(p.pr_descrip, 'UNKNOWN') AS PartDescription,
                ISNULL(p1.p1_name, 'N/A') AS Customer,
                CASE
                    WHEN ca.ca_name = 'Stick Pack' THEN 'SP'
                    ELSE 'BPS'
                END AS s_BU,
                ISNULL(line.d3_value, 'N/A') AS ProductionLine,
                CASE
                    WHEN jl.lj_ordnum IS NOT NULL AND jl.lj_ordnum != 0
                        THEN CONVERT(VARCHAR, jl.lj_ordnum)
                    WHEN wip_so.d2_value IS NOT NULL AND wip_so.d2_value != '' AND wip_so.d2_value != '0'
                        THEN wip_so.d2_value
                    ELSE ''
                END AS SalesOrder
            FROM dtjob j
            LEFT JOIN dtljob jl ON j.jo_jobnum = jl.lj_jobnum
            LEFT JOIN dmprod p ON jl.lj_prid = p.pr_id
            LEFT JOIN dmpr1 p1 ON p.pr_user5 = p1.p1_id
            LEFT JOIN dmcats ca ON p.pr_caid = ca.ca_id
            LEFT JOIN dtd2 line_link ON j.jo_jobnum = line_link.d2_recid AND line_link.d2_d1id = 5
            LEFT JOIN dmd3 line ON line_link.d2_value = line.d3_id AND line.d3_d1id = 5
            LEFT JOIN dtd2 wip_so ON j.jo_jobnum = wip_so.d2_recid AND wip_so.d2_d1id = 31
            WHERE j.jo_closed IS NULL
              AND j.jo_type = 'a'
              AND UPPER(TRIM(line.d3_value)) = UPPER(?)
              AND UPPER(CASE j.jo_waid
                    WHEN 1 THEN 'IRWINDALE'
                    WHEN 2 THEN 'DUARTE'
                    WHEN 3 THEN 'AREA_3'
                    ELSE 'UNKNOWN'
                  END) = UPPER(?)
            ORDER BY j.jo_jobnum ASC;
        """
        return db.execute_query(sql, (line, facility))

    def get_on_hand_inventory(self):
        db = get_erp_db()
        sql = """
            SELECT
                p.pr_codenum AS PartNumber,
                SUM(CASE
                    WHEN (f.fi_qc IS NULL OR f.fi_qc <> 'Pending') THEN f.fi_balance
                    ELSE 0
                END) AS on_hand_approved,
                SUM(CASE
                    WHEN f.fi_qc = 'Pending' THEN f.fi_balance
                    ELSE 0
                END) AS on_hand_pending_qc,
                SUM(f.fi_balance) AS TotalOnHand
            FROM dtfifo f
            JOIN dmprod p ON f.fi_prid = p.pr_id
            JOIN dmware w ON f.fi_waid = w.wa_id
            WHERE
                f.fi_balance > 0
                AND p.pr_codenum LIKE 'T%'
                AND w.wa_name IN ('DUARTE', 'IRWINDALE')
            GROUP BY
                p.pr_codenum;
        """
        return db.execute_query(sql)

    def get_split_fg_on_hand_value(self):
        today = datetime.now()
        first_of_this_month = today.replace(day=1)
        last_of_previous_month = first_of_this_month - timedelta(days=1)

        # CHANGED: Cutoff day from 19 to 21
        prior_cutoff_date = last_of_previous_month.replace(day=21)
        current_cutoff_date = today.replace(day=21)
        # The end of the middle bucket is the 20th
        current_twentieth_date = today.replace(day=20)

        date_format = '%m/%d/%y'
        prior_cutoff_str = prior_cutoff_date.strftime(date_format)
        current_twentieth_str = current_twentieth_date.strftime(date_format)
        current_cutoff_str = current_cutoff_date.strftime(date_format)

        # CHANGED: Labels updated to reflect new dates
        label1 = f"FG On Hand - Prior {prior_cutoff_str}"
        label2 = f"FG On Hand - {prior_cutoff_str} To {current_twentieth_str}"
        label3 = f"FG On Hand - From {current_cutoff_str}"

        db = get_erp_db()
        sql = """
            SELECT
                SUM(CASE WHEN f.fi_lotdate < ? THEN f.fi_balance * p.pr_lispric ELSE 0 END) AS value1,
                SUM(CASE WHEN f.fi_lotdate >= ? AND f.fi_lotdate < ? THEN f.fi_balance * p.pr_lispric ELSE 0 END) AS value2,
                SUM(CASE WHEN f.fi_lotdate >= ? THEN f.fi_balance * p.pr_lispric ELSE 0 END) AS value3
            FROM dtfifo f
            JOIN dmprod p ON f.fi_prid = p.pr_id
            JOIN dmware w ON f.fi_waid = w.wa_id
            WHERE
                f.fi_balance > 0
                AND p.pr_codenum LIKE 'T%'
                AND w.wa_name IN ('DUARTE', 'IRWINDALE');
        """
        params = (prior_cutoff_date, prior_cutoff_date, current_cutoff_date, current_cutoff_date)
        result = db.execute_query(sql, params)

        if result:
            return {
                'label1': label1, 'value1': result[0]['value1'] or 0,
                'label2': label2, 'value2': result[0]['value2'] or 0,
                'label3': label3, 'value3': result[0]['value3'] or 0
            }
        return {'label1': label1, 'value1': 0, 'label2': label2, 'value2': 0, 'label3': label3, 'value3': 0}

    def get_shipped_for_current_month(self):
        db = get_erp_db()
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
        db = get_erp_db()
        sql = """
            WITH LatestOrderStatus AS (
                SELECT
                    to_ordnum, to_billpo, to_ordtype, to_shipped, to_id, to_wanted, to_promise,
                    to_orddate, to_dueship, to_s1id, to_biid, to_notes, to_waid,
                    ROW_NUMBER() OVER (PARTITION BY to_ordnum ORDER BY to_id DESC) as rn
                FROM dttord
                WHERE to_ordtype IN ('s', 'h', 'd', 'm', 'l')
            ),
            OpenOrders AS (
                SELECT
                    to_ordnum, to_billpo, to_ordtype, to_id as latest_to_id, to_wanted, to_promise,
                    to_orddate, to_dueship, to_s1id, to_biid, to_notes, to_waid
                FROM LatestOrderStatus
                WHERE rn = 1 AND to_ordtype IN ('s', 'h', 'm', 'l') AND to_shipped IS NULL
            ),
            PrimarySalesRep AS (
                SELECT
                    s2_recid, s2_table,
                    CASE
                        WHEN COUNT(CASE WHEN sm.sm_lname != 'HOUSE ACCOUNT' THEN 1 END) > 0
                        THEN MAX(CASE WHEN sm.sm_lname != 'HOUSE ACCOUNT' THEN sm.sm_lname END)
                        ELSE MAX(sm.sm_lname)
                    END as primary_rep
                FROM dmsman2 s2
                INNER JOIN dmsman sm ON s2.s2_smid = sm.sm_id
                WHERE s2_table = 'dmbill'
                GROUP BY s2_recid, s2_table
            ),
            RiskData AS (
                SELECT
                    d2_recid as to_id,
                    MAX(CASE WHEN d1_field = 'u_No_Risk' THEN d2_value END) AS no_risk_value,
                    MAX(CASE WHEN d1_field = 'u_Low_Risk' THEN d2_value END) AS low_risk_value,
                    MAX(CASE WHEN d1_field = 'u_High_Risk' THEN d2_value END) AS high_risk_value,
                    MAX(CASE WHEN d1_field = 'u_Schedule_Note' THEN d2_value END) AS schedule_note_value
                FROM dtd2
                INNER JOIN dmd1 ON dtd2.d2_d1id = dmd1.d1_id
                WHERE dmd1.d1_table = 'dttord'
                AND dmd1.d1_field IN ('u_No_Risk', 'u_Low_Risk', 'u_High_Risk', 'u_Schedule_Note')
                GROUP BY d2_recid
            ),
            AggregatedOrderData AS (
                SELECT
                    oo.to_ordnum, oo.to_billpo, oo.to_ordtype, oo.to_wanted, oo.to_promise, oo.to_orddate,
                    oo.to_dueship, oo.to_s1id, oo.to_biid, oo.to_notes, oo.latest_to_id, oo.to_waid,
                    p.pr_codenum, p.pr_descrip, p.pr_user5, p.pr_caid, p.pr_unid, p.pr_user3, p.pr_id, o.or_price,
                    SUM(o.or_quant) as total_current_qty,
                    CASE
                        WHEN oo.to_ordnum % 100 = 0 THEN SUM(o.or_quant)
                        ELSE
                            COALESCE(
                                (SELECT TOP 1 orig_o.or_quant
                                FROM dtord orig_o
                                INNER JOIN dmprod orig_p ON orig_o.or_prid = orig_p.pr_id
                                INNER JOIN dttord orig_t ON orig_o.or_ordnum = orig_t.to_ordnum AND orig_o.or_toid = orig_t.to_id
                                WHERE orig_o.or_ordnum = (oo.to_ordnum - (oo.to_ordnum % 100))
                                AND orig_p.pr_codenum = p.pr_codenum
                                ORDER BY orig_t.to_id DESC),
                                SUM(o.or_quant))
                    END AS total_original_qty,
                    ROW_NUMBER() OVER (PARTITION BY oo.to_ordnum ORDER BY p.pr_codenum, o.or_price DESC) as line_sequence
                FROM OpenOrders oo
                INNER JOIN dtord o ON oo.to_ordnum = o.or_ordnum AND o.or_toid = oo.latest_to_id
                INNER JOIN dmprod p ON o.or_prid = p.pr_id
                WHERE p.pr_codenum LIKE 'T%'
                GROUP BY oo.to_ordnum, oo.to_billpo, oo.to_ordtype, oo.to_wanted, oo.to_promise, oo.to_orddate, oo.to_dueship,
                         oo.to_s1id, oo.to_biid, oo.to_notes, oo.latest_to_id, oo.to_waid, p.pr_codenum, p.pr_descrip,
                         p.pr_user5, p.pr_caid, p.pr_unid, p.pr_user3, p.pr_id, o.or_price
            ),
            ProducedQuantities AS (
                SELECT
                    lj.lj_ordnum as SalesOrder,
                    p.pr_codenum as PartNumber,
                    SUM(COALESCE(j4.j4_quant, 0)) as TotalProducedQty
                FROM dtjob j
                INNER JOIN dtljob lj ON j.jo_jobnum = lj.lj_jobnum
                INNER JOIN dmprod p ON lj.lj_prid = p.pr_id
                LEFT JOIN dtjob4 j4 ON j.jo_jobnum = j4.j4_jobnum AND lj.lj_id = j4.j4_ljid
                WHERE p.pr_codenum LIKE 'T%'
                GROUP BY lj.lj_ordnum, p.pr_codenum
            ),
            TotalShippedQuantities AS (
                SELECT
                    FLOOR(ord.to_ordnum / 100) * 100 AS original_so_num,
                    prod.pr_codenum,
                    SUM(det.or_shipquant) AS total_shipped
                FROM dttord ord
                INNER JOIN dtord det ON ord.to_id = det.or_toid
                INNER JOIN dmprod prod ON det.or_prid = prod.pr_id
                WHERE ord.to_shipped IS NOT NULL
                AND ord.to_status = 'c'
                AND ord.to_ordtype IN ('s', 'm')
                GROUP BY FLOOR(ord.to_ordnum / 100) * 100, prod.pr_codenum
            )
            SELECT
                aod.latest_to_id,
                COALESCE(wa.wa_name, 'N/A') AS [Facility],
                CASE WHEN ca.ca_name = 'Stick Pack' THEN 'SP' ELSE 'BPS' END AS [BU],
                aod.to_ordnum AS [SO],
                aod.to_billpo AS [Bill to PO],
                CASE
                    WHEN aod.to_ordtype = 's' THEN 'Sales Order'
                    WHEN aod.to_ordtype = 'h' THEN 'Credit Hold'
                    WHEN aod.to_ordtype = 'm' THEN 'ICT Order'
                    WHEN aod.to_ordtype = 'l' THEN 'On Hold Order'
                    ELSE 'Other'
                END AS [SO Type],
                aod.pr_codenum AS [Part],
                COALESCE(p1.p1_name, 'N/A') AS [Customer Name],
                aod.pr_descrip AS [Description],
                aod.total_original_qty AS [Ord Qty - (00) Level],
                COALESCE(tsq.total_shipped, 0) AS [Total Shipped Qty],
                aod.total_current_qty AS [Ord Qty - Cur. Level],
                COALESCE(pq.TotalProducedQty, 0) AS [Produced Qty],

                0 AS [Net Qty], /* Placeholder, will be calculated in Python */

                CASE
                    WHEN aod.line_sequence = 1 AND rd.no_risk_value IS NOT NULL AND ISNUMERIC(rd.no_risk_value) = 1
                    THEN CAST(rd.no_risk_value AS NUMERIC(18,2))
                    ELSE 0
                END AS [Can Make - No Risk],
                CASE
                    WHEN aod.line_sequence = 1 AND rd.low_risk_value IS NOT NULL AND ISNUMERIC(rd.low_risk_value) = 1
                    THEN CAST(rd.low_risk_value AS NUMERIC(18,2))
                    ELSE 0
                END AS [Low Risk],
                CASE
                    WHEN aod.line_sequence = 1 AND rd.high_risk_value IS NOT NULL AND ISNUMERIC(rd.high_risk_value) = 1
                    THEN CAST(rd.high_risk_value AS NUMERIC(18,2))
                    ELSE 0
                END AS [High Risk],
                COALESCE(un.un_name, 'N/A') AS [UoM],
                COALESCE(aod.pr_user3, '') AS [Qty Per UoM],
                CASE
                    WHEN ISNUMERIC(aod.pr_user3) = 1 AND aod.pr_user3 <> ''
                    THEN aod.total_current_qty * CAST(aod.pr_user3 AS NUMERIC(18,2))
                    ELSE aod.total_current_qty
                END AS [Ext Qty (Current x per UoM)],
                aod.or_price AS [Unit Price],
                aod.total_current_qty * aod.or_price AS [Ext $ (Current x Price)],
                (aod.total_original_qty - COALESCE(pq.TotalProducedQty, 0)) * aod.or_price AS [Ext $ (Net Qty x Price)],
                CASE
                    WHEN aod.line_sequence = 1 AND rd.no_risk_value IS NOT NULL AND ISNUMERIC(rd.no_risk_value) = 1
                    THEN CAST(rd.no_risk_value AS NUMERIC(18,2)) * aod.or_price
                    ELSE 0
                END AS [$ Can Make - No Risk],
                CASE
                    WHEN aod.line_sequence = 1 AND rd.low_risk_value IS NOT NULL AND ISNUMERIC(rd.low_risk_value) = 1
                    THEN CAST(rd.low_risk_value AS NUMERIC(18,2)) * aod.or_price
                    ELSE 0
                END AS [$ Low Risk],
                CASE
                    WHEN aod.line_sequence = 1 AND rd.high_risk_value IS NOT NULL AND ISNUMERIC(rd.high_risk_value) = 1
                    THEN CAST(rd.high_risk_value AS NUMERIC(18,2)) * aod.or_price
                    ELSE 0
                END AS [$ High Risk],
                CONVERT(VARCHAR, aod.to_dueship, 101) AS [Due to Ship],
                CONVERT(VARCHAR, aod.to_wanted, 101) AS [Requested Date],
                CONVERT(VARCHAR, aod.to_promise, 101) AS [Comp Arrived Date],
                CONVERT(VARCHAR, aod.to_orddate, 101) AS [Ordered Date],
                COALESCE(psr.primary_rep, sm_order.sm_lname, 'N/A') AS [Sales Rep],
                COALESCE(rd.schedule_note_value, '') AS [Schedule Note]
            FROM AggregatedOrderData aod
            LEFT JOIN dmpr1 p1 ON aod.pr_user5 = p1.p1_id
            LEFT JOIN dmcats ca ON aod.pr_caid = ca.ca_id
            LEFT JOIN dmunit un ON aod.pr_unid = un.un_id
            LEFT JOIN dmware wa ON aod.to_waid = wa.wa_id
            LEFT JOIN dmsman sm_order ON aod.to_s1id = sm_order.sm_id
            LEFT JOIN PrimarySalesRep psr ON aod.to_biid = psr.s2_recid AND psr.s2_table = 'dmbill'
            LEFT JOIN RiskData rd ON aod.latest_to_id = rd.to_id
            LEFT JOIN ProducedQuantities pq ON CAST(aod.to_ordnum AS VARCHAR) = pq.SalesOrder AND aod.pr_codenum = pq.PartNumber
            LEFT JOIN TotalShippedQuantities tsq ON (FLOOR(aod.to_ordnum / 100) * 100) = tsq.original_so_num AND aod.pr_codenum = tsq.pr_codenum
            ORDER BY aod.to_ordnum DESC, aod.pr_codenum;
        """
        return db.execute_query(sql)

# --- Singleton instance management ---
_erp_service_instance = None

def get_erp_service():
    """Gets the global singleton instance of the ErpService."""
    global _erp_service_instance
    if _erp_service_instance is None:
        _erp_service_instance = ErpService()
    return _erp_service_instance