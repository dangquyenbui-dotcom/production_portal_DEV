# database/erp_queries/job_queries.py
"""
ERP Queries related to Production Jobs.
"""
from database.erp_connection_base import get_erp_db_connection

class JobQueries:
    """Contains ERP query methods specific to Jobs."""

    def get_all_open_job_numbers(self):
         """ Retrieves just the job numbers (as strings) for all open production jobs ('a' type). """
         db = get_erp_db_connection()
         if not db: return []
         sql = "SELECT j.jo_jobnum FROM dtjob j WHERE j.jo_closed IS NULL AND j.jo_type = 'a' ORDER BY j.jo_jobnum ASC;"
         results = db.execute_query(sql)
         return [str(row['jo_jobnum']) for row in results] if results else []

    # --- NEW METHOD ---
    def get_open_job_headers(self, job_numbers):
        """
        Retrieves primary header info (Part, SO, Customer, Required Qty)
        for a list of job numbers directly from dtjob and dtljob.
        """
        if not job_numbers:
            return []
        db = get_erp_db_connection()
        if not db: return []

        # Ensure job_numbers are strings for IN clause if necessary, though numbers might be fine
        # str_job_numbers = [str(jn) for jn in job_numbers]
        placeholders = ', '.join(['?'] * len(job_numbers))

        sql = f"""
            WITH PrimaryLine AS (
                SELECT
                    lj.lj_jobnum,
                    lj.lj_ordnum,
                    lj.lj_quant AS required_quantity,
                    lj.lj_prid, -- Product ID from the job line
                    ROW_NUMBER() OVER(PARTITION BY lj.lj_jobnum ORDER BY lj.lj_linenum ASC) as rn
                FROM dtljob lj
                WHERE lj.lj_jobnum IN ({placeholders})
            )
            SELECT
                j.jo_jobnum,
                pl.lj_ordnum AS sales_order_number,
                pl.required_quantity,
                p.pr_codenum AS part_number,
                p.pr_descrip AS part_description,
                ISNULL(cust.p1_name, 'N/A') AS customer_name
            FROM dtjob j
            LEFT JOIN PrimaryLine pl ON j.jo_jobnum = pl.lj_jobnum AND pl.rn = 1
            LEFT JOIN dmprod p ON pl.lj_prid = p.pr_id -- Join Product based on PrimaryLine's prid
            LEFT JOIN dmpr1 cust ON p.pr_user5 = cust.p1_id -- Join Customer based on Product's user5
            WHERE j.jo_jobnum IN ({placeholders}) -- Filter dtjob as well
              AND j.jo_closed IS NULL
              AND j.jo_type = 'a';
        """
        # Pass job numbers twice for the two IN clauses
        params = job_numbers + job_numbers
        return db.execute_query(sql, params)
    # --- END NEW METHOD ---


    def get_open_production_jobs(self):
        """
        Retrieves job number, SO number, job quantity, and completed quantity
        for open jobs linked to sales orders.
        NOTE: This might become redundant if get_open_job_headers provides enough info.
              Keeping it for now as it might be used elsewhere.
        """
        db = get_erp_db_connection()
        if not db: return []
        sql = """
            SELECT
                j.jo_jobnum,
                (SELECT TOP 1 lj.lj_ordnum FROM dtljob lj WHERE lj.lj_jobnum = j.jo_jobnum ORDER BY lj.lj_linenum) as so_number,
                (SELECT TOP 1 lj.lj_quant FROM dtljob lj WHERE lj.lj_jobnum = j.jo_jobnum ORDER BY lj.lj_linenum) as job_quantity,
                (SELECT SUM(ISNULL(j4.j4_quant, 0)) FROM dtjob4 j4 WHERE j4.j4_jobnum = j.jo_jobnum) as completed_quantity
            FROM dtjob j
            WHERE j.jo_closed IS NULL AND j.jo_type = 'a'
              AND EXISTS (SELECT 1 FROM dtljob lj WHERE lj.lj_jobnum = j.jo_jobnum AND lj.lj_ordnum IS NOT NULL);
        """
        return db.execute_query(sql)

    def get_open_job_details(self, job_numbers):
        """
        Retrieves TRANSACTION details (dtfifo) for a list of open jobs.
        Does NOT need to join dtljob anymore for header info.
        """
        if not job_numbers: return []
        db = get_erp_db_connection()
        if not db: return []

        str_job_numbers = [str(jn) for jn in job_numbers]
        placeholders_where = ', '.join(['?'] * len(str_job_numbers))

        sql = f"""
            SELECT
                f.fi_postref,
                f.fi_action,
                f.fi_quant,
                f.fi_prid,
                p_fifo.pr_codenum AS part_number,
                p_fifo.pr_descrip AS part_description
                -- Removed joins to PrimaryJobLine, dmpr1
            FROM dtfifo f
            LEFT JOIN dmprod p_fifo ON f.fi_prid = p_fifo.pr_id
            WHERE f.fi_postref IN ({placeholders_where}) -- Filter based on the 'JJ-' prefixed strings
        """
        params = [f'JJ-{jn}' for jn in str_job_numbers] # Only need JJ-prefixed params now
        return db.execute_query(sql, params)

    def get_relieve_job_data(self, job_numbers):
        """ Retrieves relieve job data (dtfifo2) for a list of open jobs. """
        if not job_numbers: return []
        db = get_erp_db_connection()
        if not db: return []
        str_job_numbers = [str(jn) for jn in job_numbers]
        placeholders_where = ', '.join(['?'] * len(str_job_numbers))
        sql = f"""
            SELECT
                f2.f2_postref, f2.f2_action, f2.f2_prid,
                (f2.f2_oldquan - f2.f2_newquan) AS net_quantity,
                p.pr_codenum AS part_number, p.pr_descrip AS part_description
            FROM dtfifo2 f2
            LEFT JOIN dmprod p ON f2.f2_prid = p.pr_id
            WHERE f2.f2_postref IN ({placeholders_where})
            AND f2.f2_action = 'Relieve Job'
        """
        params = [f'JJ-{jn}' for jn in str_job_numbers]
        return db.execute_query(sql, params)

    def get_open_jobs_by_line(self, facility, line):
        """ Retrieves distinct open jobs filtered by production line and facility. """
        # Query remains the same...
        db = get_erp_db_connection()
        if not db: return []
        sql = """
             SELECT DISTINCT
                j.jo_jobnum AS JobNumber,
                CASE j.jo_waid WHEN 1 THEN 'IRWINDALE' WHEN 2 THEN 'DUARTE' WHEN 3 THEN 'AREA_3' ELSE 'UNKNOWN' END AS Facility,
                ISNULL(p.pr_codenum, 'UNKNOWN') AS PartNumber, ISNULL(p.pr_descrip, 'UNKNOWN') AS PartDescription,
                ISNULL(p1.p1_name, 'N/A') AS Customer,
                CASE WHEN ca.ca_name = 'Stick Pack' THEN 'SP' ELSE 'BPS' END AS s_BU,
                ISNULL(line.d3_value, 'N/A') AS ProductionLine,
                CASE
                    WHEN jl.lj_ordnum IS NOT NULL AND jl.lj_ordnum != 0 THEN CONVERT(VARCHAR, jl.lj_ordnum)
                    WHEN wip_so.d2_value IS NOT NULL AND wip_so.d2_value != '' AND wip_so.d2_value != '0' THEN wip_so.d2_value
                    ELSE ''
                END AS SalesOrder
            FROM dtjob j
            LEFT JOIN dtljob jl ON j.jo_jobnum = jl.lj_jobnum LEFT JOIN dmprod p ON jl.lj_prid = p.pr_id
            LEFT JOIN dmpr1 p1 ON p.pr_user5 = p1.p1_id LEFT JOIN dmcats ca ON p.pr_caid = ca.ca_id
            LEFT JOIN dtd2 line_link ON j.jo_jobnum = line_link.d2_recid AND line_link.d2_d1id = 5
            LEFT JOIN dmd3 line ON line_link.d2_value = line.d3_id AND line.d3_d1id = 5
            LEFT JOIN dtd2 wip_so ON j.jo_jobnum = wip_so.d2_recid AND wip_so.d2_d1id = 31
            WHERE j.jo_closed IS NULL AND j.jo_type = 'a' AND UPPER(TRIM(line.d3_value)) = UPPER(?)
              AND UPPER(CASE j.jo_waid WHEN 1 THEN 'IRWINDALE' WHEN 2 THEN 'DUARTE' WHEN 3 THEN 'AREA_3' ELSE 'UNKNOWN' END) = UPPER(?)
            ORDER BY j.jo_jobnum ASC;
        """
        return db.execute_query(sql, (line, facility))