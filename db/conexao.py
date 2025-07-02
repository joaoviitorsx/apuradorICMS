import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv('HOST')
USUARIO = os.getenv('USUARIO')
SENHA = os.getenv('SENHA')
BANCO = os.getenv('BANCO')

def conectar_mysql():
    try:
        conexao = mysql.connector.connect(
            host=HOST,
            user=USUARIO,
            password=SENHA
        )
        if conexao.is_connected():
            return conexao
    except Error as e:
        print(f"[ERRO] ao conectar ao MySQL: {e}")
    return None

def conectar_banco():
    try:
        conexao = mysql.connector.connect(
            host=HOST,
            user=USUARIO,
            password=SENHA,
            database=BANCO
        )
        if conexao.is_connected():
            return conexao
    except Error as e:
        print(f"[ERRO] ao conectar ao banco de dados '{BANCO}': {e}")
    return None

def fechar_banco(conexao):
    if conexao and conexao.is_connected():
        conexao.close()

def inicializar_banco():
    from db.criarTabelas import criar_tabelas_principais

    conexao_mysql = conectar_mysql()
    if not conexao_mysql:
        print("[FALHA] Não foi possível conectar ao MySQL.")
        return None

    try:
        cursor = conexao_mysql.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {BANCO}")
        print(f"[INFO] Banco '{BANCO}' verificado/criado com sucesso.")
    except Error as e:
        print(f"[ERRO] ao criar banco '{BANCO}': {e}")
    finally:
        fechar_banco(conexao_mysql)

    conexao_final = conectar_banco()
    if conexao_final:
        from db.criarTabelas import criar_tabela_empresas
        criar_tabela_empresas(conexao_final)
        criar_tabelas_principais() 
        return conexao_final
    return None


