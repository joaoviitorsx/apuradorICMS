from db.conexao import conectarBanco, fecharBanco

def limpar_tabelas_temporarias(empresa_id):
    print("iniciando limpeza condicional")
    tabelas = ['`0000`', '`0150`', '`0200`', '`c100`', '`c170`', '`c170nova`']

    conexao = conectarBanco()
    cursor = conexao.cursor()
    try:
        houve_limpeza = False
        for tabela in tabelas:
            cursor.execute(f"SELECT COUNT(*) FROM {tabela} WHERE empresa_id = %s", (empresa_id,))
            total = cursor.fetchone()[0]
            if total > 0:
                cursor.execute(f"DELETE FROM {tabela} WHERE empresa_id = %s", (empresa_id,))
                cursor.execute(f"SELECT COUNT(*) FROM {tabela}")
                count_total = cursor.fetchone()[0]
                if count_total == 0:
                    cursor.execute(f"ALTER TABLE {tabela} AUTO_INCREMENT = 1")
                houve_limpeza = True

        if houve_limpeza:
            conexao.commit()
            print(f"[LIMPEZA] Dados temporários da empresa {empresa_id} removidos com sucesso e AUTO_INCREMENT resetado.")
        else:
            print(f"[LIMPEZA] Nenhum dado temporário encontrado para a empresa {empresa_id}.")

    except Exception as e:
        print(f"[ERRO] Falha ao limpar dados temporários: {e}")
        conexao.rollback()
    finally:
        cursor.close()
        fecharBanco(conexao)
