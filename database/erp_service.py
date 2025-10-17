from .connection import get_db

class ErpService:
    def __init__(self):
        pass

    def get_open_jobs_by_line(self, facility, line):
        db = get_db('erp') # This one uses the 'erp' connection
        sql = """
            SELECT DISTINCT
                j.jo_jobnum AS JobNumber,
                ISNULL(p.pr_codenum, 'UNKNOWN') AS PartNumber,
                ISNULL(p.pr_descrip, 'UNKNOWN') AS PartDescription
            FROM dtjob j
            LEFT JOIN dtljob jl ON j.jo_jobnum = jl.lj_jobnum
            LEFT JOIN dmprod p ON jl.lj_prid = p.pr_id
            LEFT JOIN dtd2 line_link ON j.jo_jobnum = line_link.d2_recid AND line_link.d2_d1id = 5
            LEFT JOIN dmd3 line ON line_link.d2_value = line.d3_id AND line.d3_d1id = 5
            WHERE
                j.jo_closed IS NULL
                AND j.jo_type = 'a'
                AND UPPER(TRIM(line.d3_value)) = UPPER(?)
                AND UPPER(CASE j.jo_waid
                    WHEN 1 THEN 'IRWINDALE'
                    WHEN 2 THEN 'DUARTE'
                    ELSE 'UNKNOWN'
                  END) = UPPER(?)
            ORDER BY j.jo_jobnum ASC;
        """
        return db.execute_query(sql, (line, facility))

_erp_service_instance = None
def get_erp_service():
    global _erp_service_instance
    if _erp_service_instance is None:
        _erp_service_instance = ErpService()
    return _erp_service_instance
