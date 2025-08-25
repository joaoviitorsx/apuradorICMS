from PySide6 import QtWidgets, QtGui, QtCore
from ui.cadastroEmpresa import EmpresaCadastro
from ui.telaPrincipal import MainWindow
from utils.mensagem import mensagem_error, mensagem_aviso
from utils.icone import usar_icone
from utils.icone import resource_path
from controllers.empresa_controller import (
    InicializacaoWorker,
    CarregarEmpresasWorker,
    buscar_empresa_id,
)

class EmpresaWindow(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('Apurado de ICMS - Empresas')
        self.setGeometry(200, 200, 900, 700)
        self.setStyleSheet('background-color: #030d18;')

        self.banco_empresas = 'empresas_db'

        self._setup_layout()
        self._iniciar_verificacao_banco()

        #self._carregar_empresas()

        screen = QtGui.QGuiApplication.screenAt(QtGui.QCursor.pos())
        screen_geometry = screen.availableGeometry() if screen else QtWidgets.QApplication.primaryScreen().availableGeometry()
        center_point = screen_geometry.center()
        self.move(center_point - self.rect().center())

    def _setup_layout(self):
        self.layout = QtWidgets.QVBoxLayout(self)
        self.layout.setContentsMargins(20, 20, 20, 20)
        self.layout.setSpacing(10)

        self.logo_label = QtWidgets.QLabel()
        pixmap = QtGui.QPixmap(resource_path("images/logo.png")).scaled(300, 300, QtCore.Qt.KeepAspectRatio, QtCore.Qt.SmoothTransformation)
        self.logo_label.setPixmap(pixmap)
        self.logo_label.setAlignment(QtCore.Qt.AlignCenter)
        self.layout.addWidget(self.logo_label, alignment=QtCore.Qt.AlignCenter)
        self.layout.addSpacing(5)

        self.label_titulo = QtWidgets.QLabel('Escolha ou cadastre uma empresa')
        self.label_titulo.setStyleSheet("font-size: 20px; font-weight: bold; color: #ffffff;")
        self.label_titulo.setAlignment(QtCore.Qt.AlignCenter)

        self.combo_empresas = QtWidgets.QComboBox()
        self.combo_empresas.setStyleSheet("font-size: 20px; padding: 8px; border: 1px solid #ccc; border-radius: 5px; color: white;")
        self.combo_empresas.setFixedWidth(400)
        self.combo_empresas.addItem("Carregando empresas...")

        self.entrar_btn = QtWidgets.QPushButton('Entrar')
        self.entrar_btn.setStyleSheet(self._botao_estilo())
        self.entrar_btn.setFixedWidth(400)
        self.entrar_btn.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        self.entrar_btn.setEnabled(False)
        self.entrar_btn.clicked.connect(self.entrar)

        self.cadastrar_btn = QtWidgets.QPushButton('Cadastrar Empresa')
        self.cadastrar_btn.setStyleSheet(self._botao_estilo())
        self.cadastrar_btn.setFixedWidth(400)
        self.cadastrar_btn.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        self.cadastrar_btn.clicked.connect(self.cadastrar_empresa)

        self.layout.addStretch()
        self.layout.addWidget(self.label_titulo)
        self.layout.addSpacing(20)
        self.layout.addWidget(self.combo_empresas, alignment=QtCore.Qt.AlignCenter)
        self.layout.addSpacing(20)
        self.layout.addWidget(self.entrar_btn, alignment=QtCore.Qt.AlignCenter)
        self.layout.addSpacing(10)
        self.layout.addWidget(self.cadastrar_btn, alignment=QtCore.Qt.AlignCenter)
        self.layout.addStretch()

    def _iniciar_verificacao_banco(self):
        self.worker_db = InicializacaoWorker()
        self.worker_db.terminado.connect(self._carregar_empresas)
        self.worker_db.erro.connect(self._erro_banco)
        self.worker_db.start()

    def _carregar_empresas(self):
        self.worker_empresas = CarregarEmpresasWorker()
        self.worker_empresas.empresas_carregadas.connect(self._popular_combo)
        self.worker_empresas.erro.connect(self.exibir_erro_empresas)
        self.worker_empresas.start()

    def _popular_combo(self, empresas):
        self.combo_empresas.clear()
        self.combo_empresas.addItem("Selecione uma empresa")
        self.combo_empresas.addItems(empresas)
        self.combo_empresas.model().item(0).setEnabled(False)
        self.entrar_btn.setEnabled(True)

    def _erro_banco(self, erro):
        mensagem_error(f"Erro ao preparar estrutura do banco: {erro}")
        self.combo_empresas.clear()
        self.combo_empresas.addItem("Erro ao carregar")
        self.entrar_btn.setEnabled(False)

    def _botao_estilo(self):
        return """
            QPushButton {
                font-size: 20px;
                font-weight: bold;
                padding: 10px;
                background-color: #001F3F;
                color: white;
            }
            QPushButton:hover {
                background-color: #005588;
                color: white;
            }
            QPushButton:disabled {
                background-color: #555555;
                color: #cccccc;
            }
        """

    def entrar(self):
        nome_empresa = self.combo_empresas.currentText()
        if nome_empresa == "Selecione uma empresa" or not nome_empresa:
            mensagem_aviso("Selecione uma empresa.")
            return

        try:
            empresa_id = buscar_empresa_id(nome_empresa)
            if not empresa_id:
                mensagem_error("Empresa não encontrada na base de dados.")
                return

            self.janela_principal = MainWindow(nome_empresa, empresa_id)
            usar_icone(self.janela_principal)
            self.janela_principal.show()
            self.close()
        except Exception as e:
            mensagem_error(f"Erro ao abrir a empresa: {e}")

    def cadastrar_empresa(self, nome_banco):
        self.empresa_cadastro = EmpresaCadastro(nome_banco)
        usar_icone(self.empresa_cadastro)
        self.empresa_cadastro.show()
        self.close()

    def exibir_erro_empresas(self, erro):
        QtWidgets.QMessageBox.critical(self, "Erro", f"Erro ao carregar empresas: {erro}")
        self.combo_empresas.clear()
        self.combo_empresas.addItem("Erro ao carregar")
        self.entrar_btn.setEnabled(False)
