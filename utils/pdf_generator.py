# utils/pdf_generator.py
"""
Utility to generate PDF reports, starting with the CoC Report.
"""
import io
from datetime import datetime
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_RIGHT, TA_CENTER, TA_LEFT

def generate_coc_pdf(job_details):
    """
    Generates a Certificate of Compliance PDF from the job_details dictionary.
    """
    buffer = io.BytesIO()
    # Use landscape orientation to fit all the columns
    doc = SimpleDocTemplate(buffer, pagesize=landscape(letter),
                            rightMargin=0.5*inch, leftMargin=0.5*inch,
                            topMargin=0.5*inch, bottomMargin=0.5*inch)
    story = []
    styles = getSampleStyleSheet()
    
    # --- Title ---
    title_style = ParagraphStyle(name='TitleStyle', fontSize=16, alignment=TA_CENTER, fontName='Helvetica-Bold')
    story.append(Paragraph("SALEABLE PRODUCT CERTIFICATE OF COMPLIANCE", title_style))
    story.append(Spacer(1, 0.25*inch))

    # --- Header Info Table ---
    header_data = [
        [Paragraph("<b>Job Number:</b>", styles['Normal']), 
         Paragraph(job_details.get('job_number', 'N/A'), styles['Normal']),
         Paragraph("<b>Part Number:</b>", styles['Normal']), 
         Paragraph(job_details.get('part_number', 'N/A'), styles['Normal'])],
         
        [Paragraph("<b>Sales Order:</b>", styles['Normal']), 
         Paragraph(job_details.get('sales_order', 'N/A'), styles['Normal']),
         Paragraph("<b>Part Description:</b>", styles['Normal']), 
         Paragraph(job_details.get('part_description', 'N/A'), styles['Normal'])],
         
        [Paragraph("<b>Customer:</b>", styles['Normal']), 
         Paragraph(job_details.get('customer_name', 'N/A'), styles['Normal']),
         Paragraph("<b>Completed Qty:</b>", styles['Normal']), 
         Paragraph(f"{job_details.get('completed_qty', 0.0):,.2f}", styles['Normal'])],
         
        [Paragraph("<b>Required Qty:</b>", styles['Normal']), 
         Paragraph(f"{job_details.get('required_qty', 0.0):,.2f}", styles['Normal']),
         "", ""], 
    ]
    
    header_table = Table(header_data, colWidths=[1.2*inch, 3.8*inch, 1.2*inch, 3.8*inch])
    header_table.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING', (0,0), (-1,-1), 0),
        ('RIGHTPADDING', (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
        ('SPAN', (1, -1), (3, -1)), 
    ]))
    story.append(header_table)
    story.append(Spacer(1, 0.25*inch))

    # --- Main Component Table ---
    
    # <<< MODIFICATION: Use one centered style for all headers
    header_style_center = ParagraphStyle(name='HeaderCenter', fontSize=9, fontName='Helvetica-Bold', alignment=TA_CENTER)

    # Wrap headers in Paragraphs to allow wrapping
    table_headers = [
        Paragraph("Part", header_style_center),
        Paragraph("Part Description", header_style_center),
        Paragraph("Lot #", header_style_center),
        Paragraph("Exp Date", header_style_center),
        Paragraph("Starting Lot Qty", header_style_center),
        Paragraph("Ending Inventory", header_style_center),
        Paragraph("Packaged Qty", header_style_center),
        Paragraph("Yield Cost/Scrap", header_style_center),
        Paragraph("Yield Loss", header_style_center)
    ]
    
    col_widths = [
        1.0*inch, 2.5*inch, 1.2*inch, 0.8*inch, 
        1.0*inch, 1.0*inch, 1.0*inch, 1.0*inch, 0.5*inch
    ]

    table_data = [table_headers]
    
    table_styles = [
        ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
        ('GRID', (0,0), (-1,-1), 1, colors.black),
        ('BOX', (0,0), (-1,-1), 1, colors.black),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), # <<< MODIFICATION: VAlign all cells to MIDDLE
        ('ALIGN', (0,1), (-1,-1), 'CENTER'),  # <<< MODIFICATION: Align all data cells to CENTER
    ]

    # Populate data and styles
    current_row = 1 # Start after header
    if not job_details.get('grouped_list'):
        table_data.append([
            Paragraph("No component transactions found for this job.", styles['Normal']), 
            "", "", "", "", "", "", "", ""
        ])
        table_styles.append(('SPAN', (0, 1), (-1, 1)))
        # Alignment is already handled by the global 'ALIGN' style
    else:
        # <<< MODIFICATION: This block is fixed to populate data correctly
        for part_num, group in job_details.get('grouped_list', {}).items():
            num_lots = len(group['lots'])
            if num_lots == 0:
                continue

            start_row = current_row
            end_row = current_row + num_lots - 1

            # Add rowspan styles if more than one lot
            if num_lots > 1:
                table_styles.append(('SPAN', (0, start_row), (0, end_row))) # Part
                table_styles.append(('SPAN', (1, start_row), (1, end_row))) # Part Description
            
            # This style ensures spanned cells are also centered
            table_styles.append(('VALIGN', (0, start_row), (1, end_row), 'MIDDLE'))

            for i, lot_summary in enumerate(group['lots']):
                # Wrap all cell content in Paragraphs for correct centering
                part_cell = Paragraph(part_num, styles['Normal'])
                desc_cell = Paragraph(group.get('part_description', 'N/A'), styles['Normal'])
                lot_cell = Paragraph(lot_summary.get('lot_number', 'N/A'), styles['Normal'])
                exp_cell = Paragraph(lot_summary.get('exp_date', 'N/A'), styles['Normal'])
                
                # Build the full row with all 9 columns
                row_data = [
                    part_cell,
                    desc_cell,
                    lot_cell,
                    exp_cell,
                    f"{lot_summary.get('Starting Lot Qty', 0.0):,.2f}",
                    f"{lot_summary.get('Ending Inventory', 0.0):,.2f}",
                    f"{lot_summary.get('Packaged Qty', 0.0):,.2f}",
                    f"{lot_summary.get('Yield Cost/Scrap', 0.0):,.2f}",
                    f"{lot_summary.get('Yield Loss', 0.0):.2f}%"
                ]
                
                # If not the first row, blank out the spanned cells
                if i > 0:
                    row_data[0] = "" # Blank for Part
                    row_data[1] = "" # Blank for Part Description
                
                table_data.append(row_data)
                
                # Apply centered alignment to the text Paragraphs
                # Numeric cells are already centered by the global ALIGN style
                part_cell.style.alignment = TA_CENTER
                desc_cell.style.alignment = TA_CENTER
                lot_cell.style.alignment = TA_CENTER
                exp_cell.style.alignment = TA_CENTER
                
                current_row += 1
        # <<< END MODIFICATION

    # Create the table
    component_table = Table(table_data, colWidths=col_widths)
    component_table.setStyle(TableStyle(table_styles))
    story.append(component_table)

    # --- Footer ---
    story.append(Spacer(1, 0.5*inch))
    footer_style = ParagraphStyle(name='FooterStyle', fontSize=10, alignment=TA_RIGHT)
    story.append(Paragraph(f"Report Generated: {datetime.now().strftime('%m/%d/%Y %I:%M %p')}", footer_style))

    # Build the PDF
    doc.build(story)
    
    buffer.seek(0)
    filename = f"CoC_{job_details.get('job_number', '000000000')}.pdf"
    
    return buffer, filename