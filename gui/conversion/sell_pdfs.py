import os
import io
from PyPDF2 import PdfReader, PdfWriter
from reportlab.pdfgen import canvas

def watermark_pdfs(input_folder, output_folder):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for filename in os.listdir(input_folder):
        if not filename.lower().endswith(".pdf"):
            continue

        input_path = os.path.join(input_folder, filename)
        print("✅ Procesando archivo:", filename)
        output_name = os.path.splitext(filename)[0] + "_.pdf"
        output_path = os.path.join(output_folder, output_name)

        reader = PdfReader(input_path)
        writer = PdfWriter()

        for page in reader.pages:
            w = float(page.mediabox.width)
            h = float(page.mediabox.height)

            buffer = io.BytesIO()
            c = canvas.Canvas(buffer, pagesize=(w, h))
            # Gris más claro y más transparente
            c.setFillColorRGB(0.7, 0.7, 0.7, alpha=0.15)  # Gris más suave
            font_size = max(12, int(min(w, h) / 15))
            c.setFont("Helvetica-Bold", font_size)
            c.saveState()
            c.translate(w / 2, h / 2)
            c.rotate(45)

            x_step = max(200, int(w / 4))
            y_step = max(150, int(h / 4))
            for x in range(-int(w), int(w), x_step):
                for y in range(-int(h), int(h), y_step):
                    c.drawString(x, y, "PRIVADO")

            c.restoreState()
            c.save()
            buffer.seek(0)

            watermark = PdfReader(buffer)
            page.merge_page(watermark.pages[0])
            writer.add_page(page)

        with open(output_path, "wb") as f:
            writer.write(f)