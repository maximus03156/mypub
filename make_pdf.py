"""
make_pdf.py — Convert USERGUIDE.md to USERGUIDE.pdf using fpdf2
Run: python make_pdf.py
"""

import re
from fpdf import FPDF

COLORS = {
    "h1_bg":    (30,  58,  95),
    "h1_text":  (255, 255, 255),
    "h2_text":  (30,  58,  95),
    "h3_text":  (37,  99,  235),
    "body":     (26,  26,  26),
    "code_bg":  (241, 245, 249),
    "code_text":(15,  23,  42),
    "pre_bg":   (15,  23,  42),
    "pre_text": (226, 232, 240),
    "table_hdr":(30,  58,  95),
    "table_alt":(248, 250, 252),
    "rule":     (203, 213, 225),
    "quote_bar":(37,  99,  235),
    "quote_bg": (239, 246, 255),
    "footer":   (148, 163, 184),
    "link":     (37,  99,  235),
}

class PDF(FPDF):
    def __init__(self):
        super().__init__(format="A4")
        self.set_margins(18, 18, 18)
        self.set_auto_page_break(auto=True, margin=20)
        self.add_page()
        self.set_font("Helvetica", size=11)
        self._page_num = 0

    def header(self):
        pass

    def footer(self):
        self.set_y(-14)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(*COLORS["footer"])
        self.cell(0, 6, "Damodaran Value Scanner -- User Guide", align="L")
        self.cell(0, 6, f"Page {self.page_no()}", align="R")
        self.set_text_color(*COLORS["body"])

    # ── helpers ────────────────────────────────────────────────

    def _set_body(self):
        self.set_font("Helvetica", size=10.5)
        self.set_text_color(*COLORS["body"])

    def rule(self):
        self.ln(2)
        self.set_draw_color(*COLORS["rule"])
        self.set_line_width(0.3)
        y = self.get_y()
        self.line(self.l_margin, y, self.w - self.r_margin, y)
        self.ln(4)

    def write_paragraph(self, text):
        """Write a line of body text, handling inline code spans."""
        self._set_body()
        parts = re.split(r'(`[^`]+`)', text)
        for part in parts:
            if part.startswith('`') and part.endswith('`'):
                code = _clean(part[1:-1])
                self.set_font("Courier", size=9.5)
                self.set_fill_color(*COLORS["code_bg"])
                self.set_text_color(*COLORS["code_text"])
                self.write(5.5, code)
                self._set_body()
            else:
                part = re.sub(r'\*\*(.+?)\*\*', r'\1', part)
                part = re.sub(r'\*(.+?)\*', r'\1', part)
                part = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', part)
                self.write(5.5, _clean(part))
        self.ln()

    def h1(self, text):
        self.ln(4)
        self.set_fill_color(*COLORS["h1_bg"])
        self.set_text_color(*COLORS["h1_text"])
        self.set_font("Helvetica", "B", 16)
        self.multi_cell(0, 11, _clean(text), fill=True, align="L")
        self.ln(2)
        self._set_body()

    def h2(self, text):
        self.ln(5)
        self.set_text_color(*COLORS["h2_text"])
        self.set_font("Helvetica", "B", 13)
        self.multi_cell(0, 8, _clean(text))
        self.set_draw_color(*COLORS["rule"])
        self.set_line_width(0.4)
        y = self.get_y()
        self.line(self.l_margin, y, self.w - self.r_margin, y)
        self.ln(3)
        self._set_body()

    def h3(self, text):
        self.ln(4)
        self.set_text_color(*COLORS["h3_text"])
        self.set_font("Helvetica", "B", 11.5)
        self.multi_cell(0, 7, _clean(text))
        self.ln(1)
        self._set_body()

    def h4(self, text):
        self.ln(3)
        self.set_text_color(*COLORS["body"])
        self.set_font("Helvetica", "B", 10.5)
        self.multi_cell(0, 6, _clean(text))
        self.ln(1)
        self._set_body()

    def code_block(self, lines):
        self.ln(2)
        self.set_fill_color(*COLORS["pre_bg"])
        self.set_text_color(*COLORS["pre_text"])
        self.set_font("Courier", size=8.5)
        for line in lines:
            line = _clean(line.rstrip()) or " "
            self.cell(0, 4.8, line, fill=True, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)
        self._set_body()

    def blockquote(self, text):
        self.ln(2)
        y = self.get_y()
        self.set_fill_color(*COLORS["quote_bar"])
        self.rect(self.l_margin, y, 2, 12, "F")
        self.set_x(self.l_margin + 6)
        self.set_fill_color(*COLORS["quote_bg"])
        self.set_font("Helvetica", "I", 9.5)
        self.set_text_color(30, 58, 95)
        clean = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
        clean = re.sub(r'\*(.+?)\*', r'\1', clean)
        self.multi_cell(0, 5, _clean(clean), fill=True)
        self.ln(2)
        self._set_body()

    def bullet(self, text, indent=0):
        self._set_body()
        self.set_x(self.l_margin + indent * 5)
        clean = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
        clean = re.sub(r'\*(.+?)\*', r'\1', clean)
        clean = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', clean)
        clean = re.sub(r'`([^`]+)`', r'\1', clean)
        self.cell(4, 5.5, "-")
        self.multi_cell(0, 5.5, _clean(clean))

    def table(self, headers, rows):
        self.ln(2)
        col_w = (self.w - self.l_margin - self.r_margin) / len(headers)
        self.set_fill_color(*COLORS["table_hdr"])
        self.set_text_color(255, 255, 255)
        self.set_font("Helvetica", "B", 9)
        for h in headers:
            self.cell(col_w, 7, _clean(str(h))[:40], border=1, fill=True)
        self.ln()
        self.set_font("Helvetica", size=8.5)
        self.set_text_color(*COLORS["body"])
        for i, row in enumerate(rows):
            fill = i % 2 == 1
            self.set_fill_color(*COLORS["table_alt"])
            for cell in row:
                self.cell(col_w, 6, _clean(str(cell))[:50], border=1, fill=fill)
            self.ln()
        self.ln(2)
        self._set_body()


def _clean(text: str) -> str:
    """Replace non-latin-1 characters with safe ASCII equivalents."""
    return (text
        .replace("\u2014", "--")   # em dash
        .replace("\u2013", "-")    # en dash
        .replace("\u2019", "'")    # right single quote
        .replace("\u2018", "'")    # left single quote
        .replace("\u201c", '"')    # left double quote
        .replace("\u201d", '"')    # right double quote
        .replace("\u2022", "-")    # bullet
        .replace("\u00a0", " ")    # non-breaking space
        .replace("\u00d7", "x")    # multiplication sign
        .encode("latin-1", errors="replace").decode("latin-1")
    )

def parse_and_render(md_path: str, pdf_path: str):
    pdf = PDF()

    with open(md_path, encoding="utf-8") as f:
        lines = f.readlines()

    in_code = False
    code_lines = []
    in_table = False
    table_headers = []
    table_rows = []

    i = 0
    while i < len(lines):
        raw = lines[i].rstrip("\n")
        stripped = raw.strip()

        # ── fenced code block ──────────────────────────────────
        if stripped.startswith("```"):
            if not in_code:
                in_code = True
                code_lines = []
            else:
                pdf.code_block(code_lines)
                in_code = False
            i += 1
            continue

        if in_code:
            code_lines.append(raw)
            i += 1
            continue

        # ── table ──────────────────────────────────────────────
        if stripped.startswith("|") and stripped.endswith("|"):
            cells = [c.strip() for c in stripped.strip("|").split("|")]
            if not in_table:
                in_table = True
                table_headers = cells
                table_rows = []
                i += 1
                # skip separator row
                if i < len(lines) and re.match(r'^\|[-| :]+\|$', lines[i].strip()):
                    i += 1
                continue
            else:
                if not all(re.match(r'^[-:]+$', c) for c in cells):
                    table_rows.append(cells)
                i += 1
                continue
        else:
            if in_table:
                pdf.table(table_headers, table_rows)
                in_table = False

        # ── headings ───────────────────────────────────────────
        if stripped.startswith("#### "):
            pdf.h4(stripped[5:])
        elif stripped.startswith("### "):
            pdf.h3(stripped[4:])
        elif stripped.startswith("## "):
            pdf.h2(stripped[3:])
        elif stripped.startswith("# "):
            pdf.h1(stripped[2:])

        # ── horizontal rule ────────────────────────────────────
        elif stripped in ("---", "***", "___"):
            pdf.rule()

        # ── blockquote ─────────────────────────────────────────
        elif stripped.startswith("> "):
            pdf.blockquote(stripped[2:])

        # ── bullet list ────────────────────────────────────────
        elif re.match(r'^(\s*[-*+])\s+', raw):
            indent = (len(raw) - len(raw.lstrip())) // 2
            text = re.sub(r'^\s*[-*+]\s+', '', stripped)
            pdf.bullet(text, indent)

        # ── numbered list ──────────────────────────────────────
        elif re.match(r'^\d+\.\s+', stripped):
            text = re.sub(r'^\d+\.\s+', '', stripped)
            pdf.bullet(text)

        # ── blank line ─────────────────────────────────────────
        elif stripped == "":
            pdf.ln(2)

        # ── body paragraph ─────────────────────────────────────
        else:
            pdf.write_paragraph(stripped)

        i += 1

    # flush any pending table
    if in_table:
        pdf.table(table_headers, table_rows)

    pdf.output(pdf_path)
    print(f"PDF saved -> {pdf_path}")


if __name__ == "__main__":
    parse_and_render("USERGUIDE.md", "USERGUIDE.pdf")
