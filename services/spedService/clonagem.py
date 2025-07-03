from db.conexao import conectarBanco, fecharBanco

async def clonar_tabela_c170nova(empresa_id):
    print(f"[INÍCIO] Clonagem completa da c170nova para c170_clone (empresa_id={empresa_id})")
    
    conexao = conectarBanco()
    if not conexao:
        print("[ERRO] Falha na conexão com o banco.")
        return

    cursor = conexao.cursor()

    try:
        cursor.execute("DELETE FROM c170_clone WHERE empresa_id = %s", (empresa_id,))
        conexao.commit()

        cursor.execute("""
            INSERT IGNORE INTO c170_clone (
                id, empresa_id, cod_item, periodo, reg, num_item, descr_compl, ncm, qtd, unid,
                vl_item, vl_desc, cst, cfop, id_c100, filial,
                ind_oper, cod_part, num_doc, chv_nfe, aliquota, resultado
            )
            SELECT 
                c.id, c.empresa_id, c.cod_item, c.periodo, c.reg, c.num_item, c.descr_compl, c.cod_ncm, c.qtd, c.unid,
                c.vl_item, c.vl_desc, c.cst, c.cfop, c.id_c100, c.filial,
                c.ind_oper, c.cod_part, c.num_doc, c.chv_nfe, '' AS aliquota, '' AS resultado
            FROM c170nova c
            WHERE c.empresa_id = %s
        """, (empresa_id,))

        conexao.commit()
        print(f"[OK] {cursor.rowcount} registros clonados para c170_clone.")

    except Exception as e:
        conexao.rollback()
        print(f"[ERRO] Falha durante a clonagem da c170nova: {e}")

    finally:
        cursor.close()
        fecharBanco(conexao)
        print("[FIM] Clonagem finalizada.")
