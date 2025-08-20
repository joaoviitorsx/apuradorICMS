import os
import traceback
import pandas as pd
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QLabel, QPushButton, QTableWidget,
    QTableWidgetItem, QFileDialog, QHBoxLayout, QMessageBox, QApplication
)
from PySide6.QtCore import Qt
from PySide6 import QtGui
from db.conexao import conectarBanco, fecharBanco
from utils.aliquota import formatarAliquota

class PopupAliquota(QDialog):
    def __init__(self, empresa_id, parent=None):
        super().__init__(parent)
        self.empresa_id = empresa_id
        self.setWindowTitle("Preencher Alíquotas Nulas")
        self.setMinimumSize(800, 600)
        self._setup_ui()
        self._centralizar_janela()

    def _centralizar_janela(self):
        screen = QtGui.QGuiApplication.screenAt(QtGui.QCursor.pos())
        screen_geometry = screen.availableGeometry() if screen else QApplication.primaryScreen().availableGeometry()
        self.move(screen_geometry.center() - self.rect().center())

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        self.label = QLabel("Preencha as alíquotas nulas antes de prosseguir:")
        layout.addWidget(self.label)

        self.tabela = QTableWidget()
        layout.addWidget(self.tabela)

        botoes = QHBoxLayout()
        botoes.addWidget(self._criar_botao("Criar Planilha Modelo", self.exportar_planilha_modelo))
        botoes.addWidget(self._criar_botao("Importar Planilha", self.importar_planilha))
        layout.addLayout(botoes)

        self.botao_salvar = self._criar_botao("Salvar Tudo", self.salvar_dados, cor="#28a745", cor_hover="#166628")
        layout.addWidget(self.botao_salvar)

        self.carregar_dados()

    def _criar_botao(self, texto, callback, cor="#007bff", cor_hover="#0054b3"):
        botao = QPushButton(texto)
        botao.clicked.connect(callback)
        botao.setCursor(Qt.PointingHandCursor)
        botao.setStyleSheet(f"""
            QPushButton {{
                background-color: {cor};
                color: white;
                font-weight: bold;
                border-radius: 6px;
                padding: 6px 12px;
            }}
            QPushButton:hover {{
                background-color: {cor_hover};
            }}
        """)
        return botao

    def carregar_dados(self):
        conexao = conectarBanco()
        cursor = conexao.cursor()
        cursor.execute("""
            SELECT MIN(id), MIN(codigo), produto, ncm, NULL
            FROM cadastro_tributacao
            WHERE empresa_id = %s AND (aliquota IS NULL OR TRIM(aliquota) = '')
            GROUP BY produto, ncm
        """, (self.empresa_id,))
        dados = cursor.fetchall()
        cursor.close()
        fecharBanco(conexao)

        self.tabela.setRowCount(len(dados))
        self.tabela.setColumnCount(5)
        self.tabela.setHorizontalHeaderLabels(["ID", "Código", "Produto", "NCM", "Alíquota"])

        for i, (id_, cod, prod, ncm, aliq) in enumerate(dados):
            self.tabela.setItem(i, 0, QTableWidgetItem(str(id_)))
            self.tabela.setItem(i, 1, QTableWidgetItem(str(cod)))
            self.tabela.setItem(i, 2, QTableWidgetItem(prod))
            self.tabela.setItem(i, 3, QTableWidgetItem(ncm or ""))
            item_aliq = QTableWidgetItem(aliq if aliq else "")
            item_aliq.setFlags(item_aliq.flags() | Qt.ItemIsEditable)
            self.tabela.setItem(i, 4, item_aliq)

        self.tabela.resizeColumnsToContents()
        self.tabela.setColumnWidth(2, max(200, self.tabela.columnWidth(2)))
        self.tabela.horizontalHeader().setStretchLastSection(True)

    def categoria_por_aliquota(self, aliquota):
        aliquota_str = str(aliquota).upper().replace('%', '').replace(',', '.').strip()
        if aliquota_str in ["ISENTO", "ST", "SUBSTITUICAO", "0", "0.00"]:
            return 'ST'
        try:
            aliquota_num = float(aliquota_str)
            if aliquota_num in [17.00, 12.00, 4.00]:
                return '20RegraGeral'
            elif aliquota_num in [5.95, 4.20, 1.54]:
                return '7CestaBasica'
            elif aliquota_num in [10.20, 7.20, 2.63]:
                return '12CestaBasica'
            elif aliquota_num in [37.80, 30.39, 8.13]:
                return '28BebidaAlcoolica'
            else:
                return 'regraGeral'
        except ValueError:
            return 'regraGeral'

    def salvar_dados(self):
        linhas_pendentes = []
        for i in range(self.tabela.rowCount()):
            prod = self.tabela.item(i, 2).text().strip()
            aliq = self.tabela.item(i, 4).text().strip()
            if not aliq:
                linhas_pendentes.append(f"Linha {i + 1}: {prod}")

        if linhas_pendentes:
            msg = "As seguintes alíquotas precisam ser preenchidas:\n\n"
            msg += "\n".join(linhas_pendentes[:10])
            if len(linhas_pendentes) > 10:
                msg += f"\n\n... e mais {len(linhas_pendentes) - 10} produtos."
            QMessageBox.warning(self, "Alíquotas Pendentes", msg)
            return

        conexao = conectarBanco()
        cursor = conexao.cursor()
        try:
            for i in range(self.tabela.rowCount()):
                id_ = self.tabela.item(i, 0).text().strip()
                ncm = self.tabela.item(i, 3).text().strip()
                aliq = self.tabela.item(i, 4).text().strip()
                if not (id_ and aliq):
                    continue
                aliq_fmt = formatarAliquota(aliq)
                categoria = self.categoria_por_aliquota(aliq_fmt)
                cursor.execute("""
                    UPDATE cadastro_tributacao
                    SET ncm = %s, aliquota = %s, categoriaFiscal = %s
                    WHERE id = %s AND empresa_id = %s
                """, (ncm, aliq_fmt, categoria, id_, self.empresa_id))
            conexao.commit()
            self.label.setText("Dados atualizados com sucesso.")
            self.accept()
        except Exception as e:
            conexao.rollback()
            self.label.setText(f"Erro ao salvar: {e}")
            print(f"[ERRO SALVAR]: {e}")
        finally:
            cursor.close()
            fecharBanco(conexao)

    def exportar_planilha_modelo(self):
        caminho, _ = QFileDialog.getSaveFileName(self, "Salvar Planilha Modelo", "Tributacao.xlsx", "Arquivos Excel (*.xlsx)")
        if not caminho:
            return

        dados = []
        for i in range(self.tabela.rowCount()):
            ncm_valor = self.tabela.item(i, 3).text().strip().zfill(8)
            dados.append({
                "Código": self.tabela.item(i, 1).text(),
                "Produto": self.tabela.item(i, 2).text(),
                "NCM": ncm_valor,
                "Alíquota": self.tabela.item(i, 4).text()
            })

        df = pd.DataFrame(dados)
        with pd.ExcelWriter(caminho, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
            for i, _ in enumerate(df['NCM'], start=2):
                writer.sheets['Sheet1'].cell(row=i, column=3).number_format = '@'

        if QMessageBox.question(self, "Abrir Planilha", "Planilha criada com sucesso. Deseja abri-la?", QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes:
            os.startfile(caminho)

    def importar_planilha(self):
        caminho, _ = QFileDialog.getOpenFileName(self, "Selecionar Planilha", "", "Arquivos Excel (*.xlsx *.xls)")
        if not caminho:
            return

        try:
            df = pd.read_excel(caminho, dtype=str).fillna("")

            def norm(col):
                from unidecode import unidecode
                return unidecode(str(col)).lower().replace(" ", "").strip()

            colunas = {norm(c): c for c in df.columns}

            col_codigo = next((colunas[c] for c in colunas if "codigo" in c or "cod" in c), None)
            col_produto = next((colunas[c] for c in colunas if "produto" in c), None)
            col_ncm = next((colunas[c] for c in colunas if "ncm" in c), None)
            col_aliquota = next((colunas[c] for c in colunas if "aliquota" in c), None)

            if not all([col_codigo, col_produto, col_ncm, col_aliquota]):
                QMessageBox.warning(self, "Erro", "Colunas obrigatórias (código, produto, ncm, alíquota) não encontradas.")
                return

            atualizados = 0
            for i in range(self.tabela.rowCount()):
                cod_gui = self.tabela.item(i, 1).text().strip()
                prod_gui = self.tabela.item(i, 2).text().strip()
                for _, row in df.iterrows():
                    if str(row[col_codigo]).strip() == cod_gui:
                        if not self.tabela.item(i, 3).text().strip():
                            self.tabela.setItem(i, 3, QTableWidgetItem(str(row[col_ncm]).strip().zfill(8)))
                        if not self.tabela.item(i, 4).text().strip():
                            self.tabela.setItem(i, 4, QTableWidgetItem(str(row[col_aliquota]).strip()))
                        atualizados += 1
                        break

            QMessageBox.information(self, "Importação", f"{atualizados} registros atualizados com sucesso.")

        except Exception as e:
            QMessageBox.critical(self, "Erro ao importar", traceback.format_exc())
