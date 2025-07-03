import os
import asyncio
from PySide6 import QtWidgets, QtGui, QtCore
from PySide6.QtWidgets import QMessageBox, QFileDialog, QApplication
from utils.icone import usar_icone
from services.tributacaoService import enviar_tributacao
from services.spedService.carregamento import iniciarProcessamentoSped
from services.exportacaoService import ExportWorker
from services.spedService import sinal_popup
from ui.popupAliquota import PopupAliquota
from db.conexao import conectarBanco, fecharBanco
from utils.mensagem import mensagem_sucesso, mensagem_error, mensagem_aviso
from utils.icone import resource_path

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, empresa, empresa_id):
        super().__init__()
        self.setWindowTitle(f'Apurado de ICMS - Assertivus Contábil - {empresa}')
        self.setGeometry(200, 200, 900, 700)
        self.setStyleSheet('background-color: #030d18;')

        self.empresa = empresa             
        self.empresa_id = empresa_id        

        self.central_widget = QtWidgets.QWidget()
        self.setCentralWidget(self.central_widget)
        self.layout = QtWidgets.QVBoxLayout(self.central_widget)

        self._criar_botao_voltar()
        self._setup_empresa_header()
        self._criar_botoes()

        self.label_arquivo = QtWidgets.QLabel('Nenhum arquivo selecionado')
        self.layout.addWidget(self.label_arquivo)

        self.stack_layout = QtWidgets.QStackedLayout()
        self.layout.addLayout(self.stack_layout)

        self.imagem_placeholder = QtWidgets.QLabel()
        self.imagem_placeholder.setPixmap(QtGui.QPixmap(resource_path("images/logo.png")).scaled(300, 300, QtCore.Qt.KeepAspectRatio))
        self.imagem_placeholder.setAlignment(QtCore.Qt.AlignCenter)
        self.stack_layout.addWidget(self.imagem_placeholder)

        self.progress_bar = QtWidgets.QProgressBar()
        self.layout.addWidget(self.progress_bar)

        def abrir_popup_aliquota(empresa_id, janela_pai=None):
            popup = PopupAliquota(empresa_id, janela_pai)
            resultado = popup.exec()
            sinal_popup.resultado_popup = resultado
            usar_icone(popup)
            if sinal_popup.event_loop and sinal_popup.event_loop.isRunning():
                sinal_popup.event_loop.quit()

        sinal_popup.abrir_popup_signal.connect(abrir_popup_aliquota)

        self._criar_seletor_mes_ano()

        screen = QtGui.QGuiApplication.screenAt(QtGui.QCursor.pos())
        screen_geometry = screen.availableGeometry() if screen else QApplication.primaryScreen().availableGeometry()

        center_point = screen_geometry.center()
        self.move(center_point - self.rect().center())

    def _criar_botao_voltar(self):
        layout_topo = QtWidgets.QHBoxLayout()
        btn_voltar = QtWidgets.QPushButton("Voltar")
        btn_voltar.setFont(QtGui.QFont("Arial", 12))
        btn_voltar.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        btn_voltar.setStyleSheet("""
            QPushButton {
                background-color: #001F3F;
                color: white;
                font-size: 18px;
                font-weight: bold;
                border-radius: 5px;
                padding: 5px 10px;
                text-align: left;
            }
            QPushButton:hover {
                color: #00bfff;
            }
        """)
        btn_voltar.clicked.connect(self._voltarTelaInicial)
        layout_topo.addWidget(btn_voltar, alignment=QtCore.Qt.AlignLeft)

        layout_topo.addStretch()

        btn_icone = QtWidgets.QPushButton()
        btn_icone.setIcon(QtGui.QIcon(resource_path("images/config.png")))
        btn_icone.setIconSize(QtCore.QSize(32, 32))
        btn_icone.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        btn_icone.setStyleSheet("background-color: transparent; border: none;")
        btn_icone.clicked.connect(self._abrir_tela_produto)
        layout_topo.addWidget(btn_icone, alignment=QtCore.Qt.AlignRight)

        self.layout.addLayout(layout_topo)

    def _setup_empresa_header(self):
        self.frame_db = QtWidgets.QGroupBox('Empresa')
        self.frame_layout = QtWidgets.QVBoxLayout(self.frame_db)

        self.label_empresa = QtWidgets.QLabel(f'{self.empresa} (ID: {self.empresa_id})')
        self.label_empresa.setStyleSheet('font-size: 20px; font-weight: bold; color: #ffffff; font-family: Arial;')
        self.frame_layout.addWidget(self.label_empresa)

        self.layout.addWidget(self.frame_db)

    def _criar_botoes(self):
        botao_frame = QtWidgets.QHBoxLayout()
        self.layout.addLayout(botao_frame)

        botoes_info = [
            ("Enviar Tributação", self._enviar_tributacao),
            ("Inserir Sped", self._processar_sped),
        ]

        for texto, comando in botoes_info:
            btn = QtWidgets.QPushButton(texto)
            btn.setFont(QtGui.QFont("Arial", 14))
            btn.setStyleSheet("""
                QPushButton {
                    background-color: #001F3F;
                    color: white;
                    border: none;
                    padding: 5px 10px;
                    border-radius: 5px;
                }
                QPushButton:hover {
                    background-color: #2E236C;
                }
            """)
            btn.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
            btn.clicked.connect(comando)
            botao_frame.addWidget(btn)

    def _enviar_tributacao(self):
        enviar_tributacao(self.empresa_id, self.progress_bar)

    def _processar_sped(self):
        self.progress_bar.setValue(0)
        iniciarProcessamentoSped(self.empresa_id, self.progress_bar, self.label_arquivo, self)

    def _criar_seletor_mes_ano(self):
        mes_frame = QtWidgets.QHBoxLayout()
        self.layout.addLayout(mes_frame)

        self.mes_var = QtWidgets.QComboBox()
        self.mes_var.addItems(['Escolha o mês'] + [f"{i:02}" for i in range(1, 13)])
        self.mes_var.setStyleSheet("font-size: 16px; padding: 5px; color: #000000; background-color: #ffffff;")
        mes_frame.addWidget(self.mes_var)

        self.ano_var = QtWidgets.QComboBox()
        ano_atual = QtCore.QDate.currentDate().year()
        self.ano_var.addItems(['Escolha o ano'] + [str(ano_atual - i) for i in range(0, 4)][::-1])
        self.ano_var.setStyleSheet("font-size: 16px; padding: 5px; color: #000000; background-color: #ffffff;")
        mes_frame.addWidget(self.ano_var)

        btn_baixar_tabela = QtWidgets.QPushButton("Baixar Tabela")
        btn_baixar_tabela.setStyleSheet("""
            QPushButton {
                background-color: #28a745;
                color: white;
                border: none;
                padding: 5px;
                border-radius: 5px;
                font-size: 16px;
            }
            QPushButton:hover {
                background-color: #218838;
            }
        """)
        btn_baixar_tabela.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        btn_baixar_tabela.clicked.connect(self._baixar_tabela)
        mes_frame.addWidget(btn_baixar_tabela)

    def _baixar_tabela(self):
        mes = self.mes_var.currentText()
        ano = self.ano_var.currentText()

        if mes == "Escolha o mês" or ano == "Escolha o ano":
            mensagem_aviso("Selecione um mês e um ano válidos.")
            return

        conexao = conectarBanco()

        cursor = conexao.cursor()
        periodo = f"{mes.zfill(2)}/{ano}"
        cursor.execute("""
            SELECT COUNT(*) FROM c170_clone
            WHERE empresa_id = %s AND periodo = %s
        """, (self.empresa_id, periodo))
        total = cursor.fetchone()[0]

        if total == 0:
            mensagem_aviso(f"Não há registros para o período {mes}/{ano}.")
            fecharBanco(conexao)
            return

        if not conexao:
            mensagem_error("Não foi possível conectar ao banco de dados.")
            return

        try:
            cursor = conexao.cursor()
            cursor.execute("SELECT razao_social FROM empresas WHERE id = %s", (self.empresa_id,))
            resultado = cursor.fetchone()
            nome_empresa = resultado[0] if resultado else "empresa"
        finally:
            fecharBanco(conexao)

        sugestao_nome = f"{ano}-{mes}-{nome_empresa}.xlsx"

        caminho_arquivo, _ = QFileDialog.getSaveFileName(self, "Salvar Resultado", sugestao_nome, "Planilhas Excel (*.xlsx)")
        if not caminho_arquivo:
            mensagem_aviso("Exportação cancelada pelo usuário.")
            return

        self.progress_bar.setValue(0)
        self.export_worker = ExportWorker(self.empresa_id, mes, ano, caminho_arquivo)
        self.export_worker.progress.connect(self.progress_bar.setValue)
        self.export_worker.finished.connect(self._exportacao_concluida)
        self.export_worker.erro.connect(lambda msg: mensagem_error(msg))
        self.export_worker.start()

    def _exportacao_concluida(self, caminho_arquivo):
        mensagem_sucesso(f"Exportação concluída com sucesso!\n{caminho_arquivo}")

        abrir = QMessageBox.question(
            self,
            "Abrir Arquivo",
            "Deseja abrir a planilha exportada?",
            QMessageBox.Yes | QMessageBox.No
        )
        if abrir == QMessageBox.Yes and os.path.exists(caminho_arquivo):
            os.startfile(caminho_arquivo)

        self.progress_bar.setValue(0)

    def _voltarTelaInicial(self):
        from ui.telaEmpresa import EmpresaWindow
        self.tela_empresa = EmpresaWindow()
        self.tela_empresa.show()
        usar_icone(self.tela_empresa)
        self.close()
    
    def _abrir_tela_produto(self):
        from ui.telaProdutos import TelaProduto
        self.tela_produto = TelaProduto(self.empresa_id)
        tela_produtos = TelaProduto(self.empresa_id)
        tela_produtos.resize(self.size())
        tela_produtos.move(self.pos())
        usar_icone(self.tela_produto)
        self.tela_produto.show()
