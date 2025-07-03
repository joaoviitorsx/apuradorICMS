import os
import sys
from PySide6 import QtGui

def resource_path(rel_path):
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, rel_path)
    return os.path.join(os.path.abspath("."), rel_path)

def usar_icone(janela):
    caminho_ico = resource_path("images/icone.ico")
    caminho_png = resource_path("images/icone.png")

    if sys.platform == "win32" and os.path.exists(caminho_ico):
        janela.setWindowIcon(QtGui.QIcon(caminho_ico))
    elif os.path.exists(caminho_png):
        janela.setWindowIcon(QtGui.QIcon(caminho_png))
    else:
        print("[ERRO] Ícone não encontrado nos caminhos esperados.")
