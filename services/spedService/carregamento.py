import asyncio
import threading
import os
import math
from PySide6.QtWidgets import QFileDialog
from PySide6.QtCore import QObject, Signal

from db.conexao import conectarBanco, fecharBanco
from utils.processData import process_data
from utils.mensagem import mensagem_sucesso, mensagem_error, mensagem_aviso
from .salvamento import salvarDados
from .pos_processamento import etapas_pos_processamento
from services.fornecedorService import mensageiro as mensageiro_fornecedor
from services.spedService.limpeza import limpar_tabelas_temporarias

sem_limite = asyncio.Semaphore(3)

class Mensageiro(QObject):
    sinal_sucesso = Signal(str)
    sinal_erro = Signal(str)

def processarSpedThread(empresa_id, progress_bar, label_arquivo, caminhos, janela=None, mensageiro=None):
    print(f"[DEBUG] Iniciando thread de processamento SPED com {len(caminhos)} arquivo(s)")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    result, mensagem_final = loop.run_until_complete(
        processarSped(empresa_id, progress_bar, label_arquivo, caminhos, janela)
    )

    print(f"[DEBUG] Thread de processamento SPED finalizada")

    if mensagem_final and mensageiro:
        if result:
            print("[DEBUG] Emitindo sinal de sucesso")
            mensageiro.sinal_sucesso.emit(mensagem_final)
        else:
            print("[DEBUG] Emitindo sinal de erro")
            mensageiro.sinal_erro.emit(mensagem_final)

    loop.close()

def iniciarProcessamentoSped(empresa_id, progress_bar, label_arquivo, janela=None):
    print(f"[DEBUG] Solicitando seleção de arquivos SPED...")
    caminhos, _ = QFileDialog.getOpenFileNames(None, "Inserir Speds", "", "Arquivos Sped (*.txt)")
    if not caminhos:
        mensagem_aviso("Nenhum arquivo selecionado.", parent=janela)
        print(f"[DEBUG] Nenhum arquivo selecionado.")
        return

    mensageiro = Mensageiro()
    mensageiro.sinal_sucesso.connect(lambda texto: mensagem_sucesso(texto, parent=janela))
    mensageiro.sinal_erro.connect(lambda texto: mensagem_error(texto, parent=janela))
    mensageiro_fornecedor.sinal_log.connect(lambda texto: mensagem_sucesso(texto, parent=janela))
    mensageiro_fornecedor.sinal_erro.connect(lambda texto: mensagem_error(texto, parent=janela))

    print(f"[DEBUG] {len(caminhos)} arquivo(s) selecionado(s):")
    for i, caminho in enumerate(caminhos):
        print(f"[DEBUG] {i+1}. {os.path.basename(caminho)} ({os.path.getsize(caminho)/1024:.1f} KB)")

    thread = threading.Thread(
        target=processarSpedThread,
        args=(empresa_id, progress_bar, label_arquivo, caminhos, janela, mensageiro)
    )
    thread.start()
    print(f"[DEBUG] Thread de processamento SPED iniciada")

async def processarSped(empresa_id, progress_bar, label_arquivo, caminhos, janela=None):
    print(f"[DEBUG] Iniciando processamento de {len(caminhos)} arquivo(s) SPED...")

    conexaoCheck = conectarBanco()
    if not conexaoCheck:
        return False, "Erro ao conectar ao banco"

    checagem = conexaoCheck.cursor()
    checagem.execute("SHOW TABLES LIKE 'cadastro_tributacao'")
    if not checagem.fetchone():
        checagem.close()
        fecharBanco(conexaoCheck)
        return False, "Tributação não encontrada. Envie primeiro a tributação."
    checagem.close()
    fecharBanco(conexaoCheck)

    total = len(caminhos)
    progresso_por_arquivo = math.ceil(100 / total) if total > 0 else 100
    dados_gerais = []

    try:
        for i, caminho in enumerate(caminhos):
            nome_arquivo = os.path.basename(caminho)
            label_arquivo.setText(f"Processando arquivo {i+1}/{total}: {nome_arquivo}")

            with open(caminho, 'r', encoding='utf-8', errors='ignore') as arquivo:
                conteudo = arquivo.read().strip()

            print(f"[DEBUG] Lendo: {nome_arquivo}")
            print(f"[DEBUG] Tamanho conteúdo bruto: {len(conteudo)}")

            conteudo_processado = process_data(conteudo)
            print(f"[DEBUG] Resultado process_data: Tipo={type(conteudo_processado)}, Tamanho={len(conteudo_processado) if isinstance(conteudo_processado, list) else 'N/A'}")

            if isinstance(conteudo_processado, str):
                linhas = conteudo_processado.strip().splitlines()
            elif isinstance(conteudo_processado, list):
                linhas = conteudo_processado
            else:
                linhas = []

            print(f"[DEBUG] Adicionando {len(linhas)} linhas do arquivo {nome_arquivo}")
            dados_gerais.extend(linhas)

            progresso_atual = min((i + 1) * progresso_por_arquivo, 100)
            progress_bar.setValue(progresso_atual)
            await asyncio.sleep(0.1)

        conexao = conectarBanco()
        cursor = conexao.cursor()

        #limpar_tabelas_temporarias(empresa_id)

        mensagem = await salvarDados(dados_gerais, cursor, conexao, empresa_id)
        conexao.commit()
        cursor.close()
        fecharBanco(conexao)

        if isinstance(mensagem, str) and not mensagem.lower().startswith(("falha", "erro")):
            await etapas_pos_processamento(empresa_id, progress_bar, janela_pai=janela)
            return True, mensagem
        else:
            return False, mensagem or "Erro durante salvamento em lote."

    except ValueError as ve:
        print(f"[AVISO] Processamento interrompido: {ve}")
        if conexao:
            try:
                cursor.close()
                fecharBanco(conexao)
            except:
                pass
        progress_bar.setValue(0)
        label_arquivo.setText("Processamento interrompido.")
        return False, str(ve)
    except Exception as e:
        import traceback
        print("[ERRO] Falha no processar_sped:", traceback.format_exc())
        if conexao:
            try:
                cursor.close()
                fecharBanco(conexao)
            except:
                pass
        progress_bar.setValue(0)
        label_arquivo.setText("Erro no processamento.")
        return False, f"Erro inesperado durante o processamento: {e}"

    finally:
        if conexao:
            try:
                fecharBanco(conexao)
            except:
                pass
        await asyncio.sleep(0.5)
        label_arquivo.setText("Processamento finalizado.")