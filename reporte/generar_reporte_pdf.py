"""
Genera el PDF del reporte a partir del archivo Markdown.

Uso (desde la raíz del proyecto):
    pip install fpdf2
    python reporte/generar_reporte_pdf.py

Lee reporte/Reporte_Proyecto2a_MongoAsync_AdolfoEscobar_362839.md y
escribe el .pdf con el mismo nombre. Renderiza un subconjunto de
Markdown: títulos (#/##/###), párrafos, negritas (**), código (`),
listas (-), bloques de código (```) y tablas (|).
"""

import os
import re
import sys

FUENTE = "Helvetica"
BASE = os.path.dirname(os.path.abspath(__file__))
MD = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
    BASE, "Reporte_Proyecto2a_MongoAsync_AdolfoEscobar_362839.md")
PDF = (sys.argv[2] if len(sys.argv) > 2
       else os.path.splitext(MD)[0] + ".pdf")

REEMPLAZOS = {
    "—": "-", "–": "-", "→": "->", "≤": "<=", "≥": ">=",
    "×": "x", "“": '"', "”": '"', "‘": "'", "’": "'",
}


def sanear(texto):
    for a, b in REEMPLAZOS.items():
        texto = texto.replace(a, b)
    return texto.encode("latin-1", "replace").decode("latin-1")


def segmentos(parafo):
    """Parte un párrafo en (texto, estilo) con **negrita** y `código`."""
    partes = re.split(r"(\*\*.+?\*\*|`.+?`)", parafo)
    for parte in partes:
        if not parte:
            continue
        if parte.startswith("**") and parte.endswith("**"):
            yield parte[2:-2], "B"
        elif parte.startswith("`") and parte.endswith("`"):
            yield parte[1:-1], "codigo"
        else:
            yield parte, ""


class Reporte:
    def __init__(self):
        from fpdf import FPDF

        self.pdf = FPDF(format="letter")
        self.pdf.set_auto_page_break(True, margin=25)
        self.pdf.set_margins(25, 20, 25)
        self.pdf.add_page()
        self.pdf.set_y(20)

    def titulo(self, texto, nivel):
        tam = {1: 20, 2: 15, 3: 12}[nivel]
        self.pdf.ln(4 if nivel > 1 else 2)
        self.pdf.set_font(FUENTE, "B", tam)
        self.pdf.multi_cell(0, 8 if nivel == 1 else 7, sanear(texto))
        if nivel == 1:
            self.pdf.set_draw_color(0, 0, 0)
            self.pdf.line(25, self.pdf.get_y() + 1, 190, self.pdf.get_y() + 1)
            self.pdf.ln(3)
        else:
            self.pdf.ln(1)

    def parrafo(self, texto):
        self.pdf.set_font(FUENTE, "", 11)
        x0 = self.pdf.l_margin
        ancho = self.pdf.w - self.pdf.l_margin - self.pdf.r_margin
        for trozo, estilo in segmentos(texto):
            trozo = sanear(trozo)
            if estilo == "codigo":
                self.pdf.set_font("Courier", "", 10)
                self.pdf.write(6, trozo)
                self.pdf.set_font(FUENTE, "", 11)
            else:
                self.pdf.set_font(FUENTE, estilo, 11)
                self.pdf.write(6, trozo)
        self.pdf.ln(7)
        self.pdf.set_x(x0)
        _ = ancho

    def viñeta(self, texto):
        x0 = self.pdf.l_margin
        self.pdf.set_x(x0 + 5)
        self.pdf.set_font(FUENTE, "", 11)
        self.pdf.write(6, "- ")
        for trozo, estilo in segmentos(texto):
            trozo = sanear(trozo)
            if estilo == "codigo":
                self.pdf.set_font("Courier", "", 10)
                self.pdf.write(6, trozo)
                self.pdf.set_font(FUENTE, "", 11)
            else:
                self.pdf.set_font(FUENTE, estilo, 11)
                self.pdf.write(6, trozo)
        self.pdf.ln(7)
        self.pdf.set_x(x0)

    def codigo(self, lineas):
        self.pdf.set_fill_color(245, 245, 245)
        self.pdf.set_font("Courier", "", 9)
        x0 = self.pdf.l_margin
        ancho = self.pdf.w - self.pdf.l_margin - self.pdf.r_margin
        for linea in lineas:
            self.pdf.set_x(x0)
            self.pdf.cell(ancho, 5, " " + sanear(linea).rstrip(), fill=True,
                          new_x="LMARGIN", new_y="NEXT")
        self.pdf.ln(3)

    def tabla(self, filas):
        if len(filas) < 2:
            return
        x0 = self.pdf.l_margin
        ancho = self.pdf.w - self.pdf.l_margin - self.pdf.r_margin
        ncols = max(len(f) for f in filas)
        col = ancho / ncols
        # Fila 0 = encabezado; fila 1 = separador --- (se ignora).
        datos = [filas[0]] + [f for f in filas[2:] if f]
        for idx, fila in enumerate(datos):
            self.pdf.set_font(FUENTE, "B" if idx == 0 else "", 9)
            if idx == 0:
                self.pdf.set_fill_color(230, 230, 230)
            alturas = []
            textos = []
            for c in range(ncols):
                celda = sanear(fila[c] if c < len(fila) else "")
                nlineas = max(1, int(self.pdf.get_string_width(celda)
                                     / (col - 2)) + 1)
                alturas.append(nlineas * 5)
                textos.append(celda)
            alto = max(alturas)
            y_ini = self.pdf.get_y()
            if y_ini + alto > self.pdf.h - 25:
                self.pdf.add_page()
                y_ini = self.pdf.get_y()
            for c, celda in enumerate(textos):
                self.pdf.set_xy(x0 + c * col, y_ini)
                self.pdf.rect(x0 + c * col, y_ini, col, alto)
                self.pdf.multi_cell(col, 5, celda,
                                    fill=(idx == 0))
            self.pdf.set_y(y_ini + alto)
        self.pdf.ln(3)

    def guardar(self):
        self.pdf.output(PDF)
        print(f"PDF escrito en {PDF}")


def es_tabla(linea):
    return linea.strip().startswith("|") and linea.strip().endswith("|")


def celdas(linea):
    return [c.strip() for c in linea.strip().strip("|").split("|")]


def es_separador(fila):
    return all(set(c) <= set("-: ") for c in fila)


def main():
    with open(MD, encoding="utf-8") as f:
        lineas = f.read().splitlines()

    rep = Reporte()
    parrafo = []
    bloque_codigo = None
    bloque_tabla = []

    def vaciar_parrafo():
        if parrafo:
            texto = " ".join(parrafo).strip()
            if texto and texto != "---":
                rep.parrafo(texto)
            parrafo.clear()

    def vaciar_tabla():
        if bloque_tabla:
            rep.tabla(bloque_tabla)
            bloque_tabla.clear()

    for linea in lineas:
        if linea.strip().startswith("```"):
            if bloque_codigo is None:
                vaciar_parrafo()
                vaciar_tabla()
                bloque_codigo = []
            else:
                rep.codigo(bloque_codigo)
                bloque_codigo = None
            continue
        if bloque_codigo is not None:
            bloque_codigo.append(linea)
            continue
        if es_tabla(linea):
            vaciar_parrafo()
            fila = celdas(linea)
            if not es_separador(fila):
                bloque_tabla.append(fila)
            elif bloque_tabla:
                bloque_tabla.append(["---"])  # marca de separador
            continue
        vaciar_tabla()
        s = linea.strip()
        if not s:
            vaciar_parrafo()
            continue
        if s.startswith("### "):
            vaciar_parrafo()
            rep.titulo(s[4:], 3)
        elif s.startswith("## "):
            vaciar_parrafo()
            rep.titulo(s[3:], 2)
        elif s.startswith("# "):
            vaciar_parrafo()
            rep.titulo(s[2:], 1)
        elif s == "---":
            vaciar_parrafo()
        elif s.startswith("- "):
            vaciar_parrafo()
            rep.viñeta(s[2:])
        elif re.match(r"^\d+\.\s", s):
            vaciar_parrafo()
            rep.viñeta(re.sub(r"^\d+\.\s", "", s))
        else:
            parrafo.append(s)
    vaciar_parrafo()
    vaciar_tabla()
    rep.guardar()


if __name__ == "__main__":
    main()
