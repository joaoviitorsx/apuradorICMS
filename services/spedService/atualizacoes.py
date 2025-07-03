from db.conexao import conectarBanco, fecharBanco
from utils.conversor import Conversor

async def atualizarAliquota(empresa_id, lote_tamanho=5000):
    print("[INÍCIO] Atualizando alíquotas em c170_clone por lotes...")

    conexao = conectarBanco()
    cursor = conexao.cursor(dictionary=True)

    try:
        cursor.execute("SELECT dt_ini FROM `0000` WHERE empresa_id = %s ORDER BY id DESC LIMIT 1", (empresa_id,))
        row = cursor.fetchone()
        if not row or not row['dt_ini']:
            print("[AVISO] Nenhum dt_ini encontrado. Cancelando.")
            return

        ano = int(row['dt_ini'][4:]) if len(row['dt_ini']) >= 6 else 0
        coluna = "aliquota" if ano >= 2024 else "aliquota_antiga"

        cursor.execute(f"""
            SELECT n.id AS id_c170, c.{coluna} AS nova_aliquota
            FROM c170_clone n
            JOIN cadastro_tributacao c
              ON c.empresa_id = n.empresa_id
             AND c.produto = n.descr_compl
             AND c.ncm = n.ncm
            WHERE n.empresa_id = %s
              AND (n.aliquota IS NULL OR n.aliquota = '')
              AND c.{coluna} IS NOT NULL AND c.{coluna} != ''
        """, (empresa_id,))
        registros = cursor.fetchall()
        total = len(registros)
        print(f"[INFO] {total} registros a atualizar...")

        for i in range(0, total, lote_tamanho):
            lote = registros[i:i + lote_tamanho]
            dados = [(r['nova_aliquota'][:10], r['id_c170']) for r in lote]

            cursor.executemany("""
                UPDATE c170_clone
                SET aliquota = %s
                WHERE id = %s
            """, dados)
            conexao.commit()
            print(f"[OK] Lote {i//lote_tamanho + 1} atualizado com {len(lote)} itens.")

        print(f"[FINALIZADO] Alíquotas atualizadas em {total} registros para empresa {empresa_id}.")

    except Exception as err:
        print(f"[ERRO] ao atualizar alíquotas: {err}")
        conexao.rollback()

    finally:
        cursor.close()
        fecharBanco(conexao)

async def aliquotaSimples(empresa_id, periodo):
    print("[INÍCIO] Atualizando alíquotas Simples Nacional")
    conexao = conectarBanco()
    cursor = conexao.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT c.id, c.aliquota, c.descr_compl, c.cod_part
            FROM c170_clone c
            JOIN cadastro_fornecedores f 
                ON f.cod_part = c.cod_part AND f.empresa_id = %s
            WHERE c.periodo = %s AND c.empresa_id = %s
              AND f.simples = 'Sim'
        """, (empresa_id, periodo, empresa_id))

        registros = cursor.fetchall()
        atualizacoes = []

        for row in registros:
            aliquota_str = str(row.get('aliquota') or '').strip().upper()
            
            if aliquota_str in ['ST', 'ISENTO', 'PAUTA', '']:
                continue

            try:
                aliquota = Conversor(row['aliquota'])
                
                nova_aliquota = round(aliquota + 3, 2)

                aliquota_str = f"{nova_aliquota:.2f}".replace('.', ',') + '%'

                atualizacoes.append((aliquota_str, row['id']))
                
            except Exception as e:
                print(f"[AVISO] Erro ao processar registro {row['id']}: {e}")

        if atualizacoes:
            cursor.executemany("""
                UPDATE c170_clone
                SET aliquota = %s
                WHERE id = %s
            """, atualizacoes)

            conexao.commit()

    except Exception as e:
        print(f"[ERRO] ao atualizar alíquota Simples: {e}")
        conexao.rollback()

    finally:
        cursor.close()
        fecharBanco(conexao)
        print("[FIM] Finalização da atualização de alíquota Simples.")

async def atualizarResultado(empresa_id):
    print("[INÍCIO] Atualizando resultado")
    conexao = conectarBanco()
    cursor = conexao.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT id, vl_item, vl_desc, aliquota 
            FROM c170_clone
            WHERE empresa_id = %s
        """, (empresa_id,))

        registros = cursor.fetchall()
        total = len(registros)

        atualizacoes = []

        for row in registros:
            vl_item = Conversor(row['vl_item'])
            vl_desc = Conversor(row['vl_desc'])
            aliquota = Conversor(row['aliquota'])

            resultado = round((vl_item - vl_desc) * (aliquota / 100), 2)

            atualizacoes.append((resultado, row['id']))

        if atualizacoes:
            cursor.executemany("""
                UPDATE c170_clone
                SET resultado = %s
                WHERE id = %s
            """, atualizacoes)

            conexao.commit()
            print(f"[OK] Resultado atualizado para {total} registros.")

    except Exception as err:
        print(f"[ERRO] ao atualizar resultado: {err}")
        conexao.rollback()

    finally:
        cursor.close()
        fecharBanco(conexao)
        print("[FIM] Finalização da atualização de resultado.")


