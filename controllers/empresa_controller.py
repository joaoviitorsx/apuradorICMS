from PySide6.QtCore import QThread, Signal
from db.conexao import conectarBanco, fecharBanco, inicializarBanco

class InicializacaoWorker(QThread):
    terminado = Signal()
    erro = Signal(str)

    def run(self):
        try:
            conexao = inicializarBanco()
            if conexao:
                fecharBanco(conexao)
            self.terminado.emit()
        except Exception as e:
            self.erro.emit(str(e))

class CarregarEmpresasWorker(QThread):
    empresas_carregadas = Signal(list)
    erro = Signal(str)

    def run(self):
        try:
            conexao = conectarBanco()
            cursor = conexao.cursor()
            cursor.execute("SELECT razao_social FROM empresas ORDER BY razao_social ASC")
            empresas = [row[0] for row in cursor.fetchall()]
            cursor.close()
            fecharBanco(conexao)
            self.empresas_carregadas.emit(empresas)
        except Exception as e:
            self.erro.emit(str(e))

def buscar_empresa_id(nome_empresa):
    try:
        conexao = conectarBanco()
        cursor = conexao.cursor()
        cursor.execute("SELECT id FROM empresas WHERE razao_social = %s", (nome_empresa,))
        resultado = cursor.fetchone()
        cursor.close()
        fecharBanco(conexao)
        return resultado[0] if resultado else None
    except Exception:
        return None
