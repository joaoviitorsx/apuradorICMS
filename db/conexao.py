import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

def env():
    envDiretorio = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
    load_dotenv(dotenv_path=envDiretorio, override=True)
    host = os.getenv('HOST')
    usuario = os.getenv('USUARIO')
    banco = os.getenv('BANCO')
    return {
        'host': host,
        'usuario': usuario,
        'senha': os.getenv('SENHA'),
        'banco': banco,
        'port': os.getenv('PORT', '3306')
    }

def conectarBanco():
    try:
        config = env()
        #print(f"[INFO] Tentando conectar ao banco: host={config['host']}, usuário={config['usuario']}, banco={config['banco']}")
        conexao = mysql.connector.connect(
            host=config['host'],
            user=config['usuario'],
            password=config['senha'],
            database=config['banco'],
            port=int(config.get('PORT', 3306)),
            charset='utf8mb4',
            use_unicode=True,
            autocommit=False,
            connection_timeout=30,
            sql_mode='STRICT_TRANS_TABLES'
        )
        if conexao.is_connected():
            print(f"[SUCESSO] Conexão estabelecida com o banco {config['banco']}")
            return conexao
    except Error as e:
        print(f"[ERRO] ao conectar ao banco: {e}")
    return None

def fecharBanco(conexao):
    if conexao and conexao.is_connected():
        conexao.close()

# def conectarMySQL():
#     try:
#         config = env()
#         conexao = mysql.connector.connect(
#             host=config['host'],
#             user=config['usuario'],
#             password=config['senha']
#         )
#         if conexao.is_connected():
#             return conexao
#     except Error as e:
#         print(f"[ERRO] ao conectar ao MySQL: {e}")
#     return None

# def conectarBanco():
#     try:
#         config = env()
#         conexao = mysql.connector.connect(
#             host=config['host'],
#             user=config['usuario'],
#             password=config['senha'],
#             database=config['banco']
#         )
#         if conexao.is_connected():
#             return conexao
#     except Error as e:
#         print(f"[ERRO] ao conectar ao banco de dados '{config['banco']}': {e}")
#     return None

# def inicializarBanco():
#     from db.criarTabelas import criar_tabelas_principais, criar_tabela_empresas

#     config = env()
#     conexaoMySQL = conectarMySQL()
#     if not conexaoMySQL:
#         print("[FALHA] Não foi possível conectar ao MySQL.")
#         return None

#     try:
#         cursor = conexaoMySQL.cursor()
#         cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config['banco']}")
#         print(f"[INFO] Banco '{config['banco']}' verificado/criado com sucesso.")
#     except Error as e:
#         print(f"[ERRO] ao criar banco '{config['banco']}': {e}")
#     finally:
#         fecharBanco(conexaoMySQL)

#     conexaoFinal = conectarBanco()
#     if conexaoFinal:
#         criar_tabela_empresas(conexaoFinal)
#         criar_tabelas_principais()
#         return conexaoFinal
#     return None

