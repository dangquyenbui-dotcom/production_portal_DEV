# database/erp_queries/bom_queries.py
"""
ERP Queries related to Bills of Materials (BOM).
"""
from database.erp_connection_base import get_erp_db_connection

class BOMQueries:
    """Contains ERP query methods specific to Bills of Materials."""

    def get_bom_data(self, parent_part_number=None):
        """
        Retrieves detailed BOM data for active parent parts and latest revisions.
        Optionally filters by a specific parent part number.
        """
        db = get_erp_db_connection()
        if not db: return []
        sql = """
            WITH LatestBOMRevisions AS (
                SELECT bom.bo_bomfor as parent_product_id, MAX(bom.bo_reid) as latest_revision_id
                FROM dmbom bom
                INNER JOIN dmprod parent ON bom.bo_bomfor = parent.pr_id
                WHERE parent.pr_active = 1
                GROUP BY bom.bo_bomfor
            )
            SELECT
                bom.bo_seq as Seq, comp.pr_codenum as "Part Number", comp.pr_descrip as Description,
                bom_unit.un_name as Unit, bom.bo_quant as Quantity, bom.bo_scrap as "Scrap %",
                bom.bo_overage as "Overage %", bom.bo_overissue as "Overissue %", bom.bo_incqty as "Incremental Qty",
                CASE WHEN bom.bo_uselot = 1 THEN 'Yes' ELSE 'No' END as "Lot Tracking",
                CASE WHEN bom.bo_useexp = 1 THEN 'Yes' ELSE 'No' END as "Expiration Tracking",
                cat.ca_name as "Product Category", bom.bo_bomcalc as "Calculation Method",
                CASE WHEN bom.bo_costonly = 1 THEN 'Yes' ELSE 'No' END as "Costing Only",
                CASE WHEN bom.bo_byproduct = 1 THEN 'Yes' ELSE 'No' END as "Byproduct",
                CASE WHEN bom.bo_subtot = 1 THEN 'Yes' ELSE 'No' END as "Subtotal",
                CASE WHEN bom.bo_reqseq = 1 THEN 'Yes' ELSE 'No' END as "Sequential",
                bom.bo_shelfdays as "Shelf Life Days", bom.bo_shelfpct as "Shelf Life %",
                bom.bo_minage as "Min Age Days", bom.bo_maxage as "Max Age Days",
                bom.bo_reid as "Revision ID", bom.bo_desig as Designator, bom.bo_notes as Notes,
                comp.pr_id as "Component ID", parent.pr_id as "Parent ID",
                parent.pr_codenum as "Parent Part Number", parent.pr_descrip as "Parent Description",
                bom_unit.un_factor as "Unit Factor", comp.pr_reorder as "Reorder Point",
                comp.pr_minquant as "Min Order Qty", comp.pr_orddays as "Lead Time Days",
                comp.pr_stocked as "Stocked Item", comp.pr_make as "Make Item", comp.pr_purable as "Purchase Item"
            FROM dmbom bom
            INNER JOIN dmprod comp ON bom.bo_prid = comp.pr_id
            INNER JOIN dmprod parent ON bom.bo_bomfor = parent.pr_id
            INNER JOIN dmcats cat ON comp.pr_caid = cat.ca_id
            LEFT JOIN dmunit bom_unit ON bom.bo_unid = bom_unit.un_id
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