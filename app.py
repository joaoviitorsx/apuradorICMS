import sys
import signal
from PySide6 import QtWidgets
from PySide6.QtCore import QCoreApplication
from ui.telaEmpresa import EmpresaWindow
from utils.icone import usar_icone

def main():
    app = QtWidgets.QApplication(sys.argv)
    app.aboutToQuit.connect(lambda: print("Sistema encerrando..."))

    janela = EmpresaWindow()
    usar_icone(janela)
    janela.show()

    sys.exit(app.exec())

def sinal_encerramento(sig, frame):
    QCoreApplication.quit()

signal.signal(signal.SIGINT, sinal_encerramento)

if __name__ == '__main__':
    main()
