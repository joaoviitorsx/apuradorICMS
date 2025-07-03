import traceback
from utils.siglas import obterUF
from utils.sanitizacao import truncar, corrigirUnidade, corrigir_ind_mov, corrigir_cst_icms, TAMANHOS_MAXIMOS, calcular_periodo, validar_estrutura_c170

UNIDADE_PADRAO = "UN"

async def salvarDados(conteudo, cursor, conexao, empresa_id, janela=None):
    linhas = conteudo 
    
    dt_ini_0000 = None
    for linha in linhas:
        if linha.startswith("|0000|"):
            partes = linha.split('|')[1:-1]
            partes += [None] * (15 - len(partes))
            dt_ini_0000 = partes[3]
            periodo = calcular_periodo(dt_ini_0000)

            cursor.execute("SELECT COUNT(*) FROM `0000` WHERE periodo = %s AND empresa_id = %s", (periodo, empresa_id))
                           
            if cursor.fetchone()[0] > 0:
                cursor.execute("SELECT COUNT(*) FROM c170 WHERE periodo = %s AND empresa_id = %s", (periodo, empresa_id))
                count_c170 = cursor.fetchone()[0]
                raise ValueError(f"SPED do período {periodo} já foi processado anteriormente. {count_c170} itens já existem no banco.")
            break
    
    if dt_ini_0000 is None:
        raise ValueError("Não foi possível encontrar o registro 0000 nos dados fornecidos.")
    
    print(f"[DEBUG] Iniciando processamento de {len(linhas)} linhas")
    
    contadores = {"0000": 0, "0150": 0, "0200": 0, "C100": 0, "C170": 0, "salvos": 0, "erros": 0, "existentes": 0}
    lote_0000, lote_0150, lote_0200, lote_c100, lote_c170 = [], [], [], [], []
    registros_processados = set()

    dt_ini_0000 = None
    filial = None
    num_doc = None
    ultimo_num_doc = None
    mapa_documentos = {}

    def verificarRegistroExistente(cursor, tabela, condicoes, retornar_id=False):
        if not condicoes:
            raise ValueError("As condições para verificação não podem estar vazias.")

        where_clause = " AND ".join([f"`{campo}` = %s" for campo in condicoes])
        valores = list(condicoes.values())

        tabela_escapada = f"`{tabela}`"

        if retornar_id:
            sql = f"SELECT `id` FROM {tabela_escapada} WHERE {where_clause}"
        else:
            sql = f"SELECT COUNT(*) FROM {tabela_escapada} WHERE {where_clause}"

        cursor.execute(sql, valores)
        resultado = cursor.fetchone()

        if retornar_id:
            return resultado[0] if resultado else None
        else:
            return resultado[0] > 0

    def inserir_lote(sql, lote, descricao):
        if not lote: return
        try:
            cursor.executemany(sql, lote)
            contadores["salvos"] += len(lote)
        except Exception as e:
            if "Duplicate entry" in str(e):
                salvos = 0
                for item in lote:
                    try:
                        cursor.execute(sql, item)
                        salvos += 1
                    except Exception as e_item:
                        if "Duplicate entry" not in str(e_item):
                            print(f"[ERRO] Falha ao inserir item em {descricao}: {e_item}")
                            contadores["erros"] += 1
                print(f"[PARCIAL] {descricao}: {salvos}/{len(lote)} registros inseridos após tratar duplicidades.")
                contadores["salvos"] += salvos
            else:
                print(f"[ERRO] Falha ao inserir {descricao}: {e}")
                contadores["erros"] += len(lote)

    try:
        for linha in linhas:
            if not linha.strip(): continue
            partes = linha.split('|')[1:-1]

            if linha.startswith("|0000|"):
                partes += [None] * (15 - len(partes))
                dt_ini_0000 = partes[3]
                cnpj = partes[6]
                filial = cnpj[8:12] if cnpj else '0000'
                periodo = calcular_periodo(dt_ini_0000)
                partes += [filial, calcular_periodo(dt_ini_0000), empresa_id]
                lote_0000.append(partes)
                contadores["0000"] += 1

            elif linha.startswith("|0150|"):
                partes += [None] * (13 - len(partes))
                municipio = partes[7]
                cod_uf = municipio[:2] if municipio else None
                uf = obterUF(cod_uf)
                pj_pf = "PF" if partes[4] is None else "PJ"
                periodo = calcular_periodo(dt_ini_0000)
                cod_part = partes[1]
                partes += [cod_uf, uf, pj_pf, periodo, empresa_id]
                if verificarRegistroExistente(cursor, '0150', {
                    'cod_part': cod_part,
                    'periodo': periodo,
                    'empresa_id': empresa_id
                }):
                    contadores["existentes"] += 1
                else:
                    lote_0150.append(partes)
                    contadores["0150"] += 1

            elif linha.startswith("|0200|"):
                partes += [None] * (13 - len(partes))
                partes[1] = truncar(partes[1], TAMANHOS_MAXIMOS['cod_item'])
                partes[2] = truncar(partes[2], TAMANHOS_MAXIMOS['descr_item'])
                partes[5] = truncar(partes[5], TAMANHOS_MAXIMOS['unid'])
                periodo = calcular_periodo(dt_ini_0000)
                cod_item = partes[1]
                partes.append(periodo)
                partes.append(empresa_id)

                if verificarRegistroExistente(cursor, '0200', {
                    'cod_item': cod_item,
                    'periodo': periodo,
                    'empresa_id': empresa_id
                }):
                    contadores["existentes"] += 1
                else:
                    lote_0200.append(partes)
                    contadores["0200"] += 1

            elif linha.startswith("|C100|"):
                partes += [None] * (29 - len(partes))
                ind_oper, cod_part, num_doc, chv_nfe = partes[1], partes[4], partes[7], partes[9]
                periodo = calcular_periodo(dt_ini_0000)

                condicoes = {'periodo': periodo, 'empresa_id': empresa_id}
                if chv_nfe:
                    condicoes['chv_nfe'] = chv_nfe
                elif num_doc:
                    condicoes['num_doc'] = num_doc
                    if cod_part:
                        condicoes['cod_part'] = cod_part
                
                id_c100_existente = verificarRegistroExistente(cursor, 'c100', condicoes, retornar_id=True)
                
                if id_c100_existente:
                    contadores["existentes"] += 1
                    if num_doc:
                        mapa_documentos[num_doc] = {
                            "ind_oper": ind_oper,
                            "cod_part": cod_part,
                            "chv_nfe": chv_nfe,
                            "id_c100": id_c100_existente
                        }
                        ultimo_num_doc = num_doc
                else:
                    registro = [periodo] + partes + [filial, empresa_id]

                    cursor.execute("""
                        INSERT INTO c100 (
                            periodo, reg, ind_oper, ind_emit, cod_part, cod_mod, cod_sit, ser, num_doc, chv_nfe,
                            dt_doc, dt_e_s, vl_doc, ind_pgto, vl_desc, vl_abat_nt, vl_merc, ind_frt, vl_frt, vl_seg,
                            vl_out_da, vl_bc_icms, vl_icms, vl_bc_icms_st, vl_icms_st, vl_ipi, vl_pis, vl_cofins,
                            vl_pis_st, vl_cofins_st, filial, empresa_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s)
                    """, registro)

                    id_c100 = cursor.lastrowid

                    if num_doc:
                        mapa_documentos[num_doc] = {
                            "ind_oper": ind_oper,
                            "cod_part": cod_part,
                            "chv_nfe": chv_nfe,
                            "id_c100": id_c100
                        }
                        ultimo_num_doc = num_doc
                    contadores["C100"] += 1

            elif linha.startswith("|C170|"):
                partes += [None] * (39 - len(partes))
                if len(partes) < 10:
                    continue

                if not ultimo_num_doc:
                    print(f"[DEBUG CRÍTICO] ultimo_num_doc indefinido antes do registro C170: linha={linha}")
                    continue

                dados_doc = mapa_documentos.get(ultimo_num_doc)
                if not dados_doc:
                    print(f"[WARN] Documento {ultimo_num_doc} não encontrado no mapa. Ignorando linha C170.")
                    continue

                ind_oper = dados_doc["ind_oper"]
                cod_part = dados_doc["cod_part"]
                chv_nfe = dados_doc["chv_nfe"]
                id_c100 = dados_doc.get("id_c100")

                if not id_c100:
                    print(f"[WARN] id_c100 não encontrado para nota {ultimo_num_doc}. Ignorando C170.")
                    continue

                num_item = partes[2]
                cod_item = truncar(partes[3], TAMANHOS_MAXIMOS['cod_item'])
            
                if verificarRegistroExistente(cursor, 'c170', {
                    'id_c100': id_c100,
                    'num_item': num_item,
                    'cod_item': cod_item
                }):
                    contadores["existentes"] += 1
                    continue
                
                descr_compl = truncar(partes[4], TAMANHOS_MAXIMOS['descr_compl']) 
                qtd = partes[5]
                unid = truncar(corrigirUnidade(partes[6]), TAMANHOS_MAXIMOS['unid'])
                vl_item = partes[7]
                vl_desc = partes[8]
                ind_mov = corrigir_ind_mov(partes[9])
                cst_icms = corrigir_cst_icms(partes[10])
                cfop = partes[11]
                cod_nat = truncar(partes[37], TAMANHOS_MAXIMOS['cod_nat'])

                dados = [
                    calcular_periodo(dt_ini_0000),  # periodo
                    "C170",                         # reg fixo
                    num_item,
                    cod_item,
                    descr_compl,
                    qtd,
                    unid,
                    vl_item,
                    vl_desc,
                    ind_mov,
                    cst_icms,
                    cfop,
                    cod_nat,
                    partes[12],  # vl_bc_icms
                    partes[13],  # aliq_icms
                    partes[14],  # vl_icms
                    partes[15],  # vl_bc_icms_st
                    partes[16],  # aliq_st
                    partes[17],  # vl_icms_st
                    partes[18],  # ind_apur
                    partes[19],  # cst_ipi
                    partes[20],  # cod_enq
                    partes[21],  # vl_bc_ipi
                    partes[22],  # aliq_ipi
                    partes[23],  # vl_ipi
                    partes[24],  # cst_pis
                    partes[25],  # vl_bc_pis
                    partes[26],  # aliq_pis
                    partes[27],  # quant_bc_pis
                    partes[28],  # aliq_pis_reais
                    partes[29],  # vl_pis
                    partes[30],  # cst_cofins
                    partes[31],  # vl_bc_cofins
                    partes[32],  # aliq_cofins
                    partes[33],  # quant_bc_cofins
                    partes[34],  # aliq_cofins_reais
                    partes[35],  # vl_cofins
                    truncar(partes[36], TAMANHOS_MAXIMOS['cod_cta']),
                    partes[37],  # vl_abat_nt
                    id_c100,
                    filial,
                    ind_oper,
                    cod_part,
                    ultimo_num_doc,
                    chv_nfe,
                    empresa_id
                ]

                if len(dados) != 46:
                    print(f"[ERRO] Registro com tamanho inválido: {len(dados)} (esperado: 46)")
                    continue

                if not validar_estrutura_c170(dados):
                    continue

                registro_id = f"{filial}_{ultimo_num_doc}_{cod_item}"
                if registro_id in registros_processados:
                    continue

                lote_c170.append(dados)
                registros_processados.add(registro_id)
                contadores["C170"] += 1

        inserir_lote("""
            INSERT INTO `0000` (reg, cod_ver, cod_fin, dt_ini, dt_fin, nome, cnpj, cpf, uf, ie, cod_num, im, suframa,
            ind_perfil, ind_ativ, filial, periodo, empresa_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lote_0000, "|0000|")

        inserir_lote("""
            INSERT INTO `0150` (reg, cod_part, nome, cod_pais, cnpj, cpf, ie, cod_mun, suframa, ende, num, compl, bairro,
            cod_uf, uf, pj_pf, periodo, empresa_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lote_0150, "|0150|")

        inserir_lote("""
            INSERT INTO `0200` (reg, cod_item, descr_item, cod_barra, cod_ant_item, unid_inv, tipo_item, cod_ncm,
            ex_ipi, cod_gen, cod_list, aliq_icms, cest, periodo, empresa_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lote_0200, "|0200|")

        inserir_lote("""
            INSERT INTO c100 (
                periodo, reg, ind_oper, ind_emit, cod_part, cod_mod, cod_sit, ser, num_doc, chv_nfe,
                dt_doc, dt_e_s, vl_doc, ind_pgto, vl_desc, vl_abat_nt, vl_merc, ind_frt, vl_frt, vl_seg,
                vl_out_da, vl_bc_icms, vl_icms, vl_bc_icms_st, vl_icms_st, vl_ipi, vl_pis, vl_cofins,
                vl_pis_st, vl_cofins_st, filial, empresa_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                     %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, lote_c100, "|C100|")

        for i in range(0, len(lote_c170), 5000):
            lote = lote_c170[i:i+5000]
            try:
                cursor.executemany("""
                    INSERT INTO c170 (
                        periodo, reg, num_item, cod_item, descr_compl, qtd, unid, vl_item, vl_desc,
                        ind_mov, cst_icms, cfop, cod_nat, vl_bc_icms, aliq_icms, vl_icms, vl_bc_icms_st,
                        aliq_st, vl_icms_st, ind_apur, cst_ipi, cod_enq, vl_bc_ipi, aliq_ipi, vl_ipi,
                        cst_pis, vl_bc_pis, aliq_pis, quant_bc_pis, aliq_pis_reais, vl_pis, cst_cofins,
                        vl_bc_cofins, aliq_cofins, quant_bc_cofins, aliq_cofins_reais, vl_cofins, cod_cta,
                        vl_abat_nt, id_c100, filial, ind_oper, cod_part, num_doc, chv_nfe, empresa_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s
                    )
                """, lote)
                contadores["salvos"] += len(lote)

                cursor.execute("SELECT COUNT(*) FROM c170 WHERE periodo = %s AND empresa_id = %s",
                               (calcular_periodo(dt_ini_0000), empresa_id))
                total_registros = cursor.fetchone()[0]
                print(f"[DEBUG] Total de registros em c170 após inserção: {total_registros}")
            except Exception as e:
                contadores["erros"] += len(lote)
                print(f"[ERRO] Falha no lote C170 {i}-{i+len(lote)}: {e}")

        conexao.commit()
        print(f"[FINAL] Processamento concluído: {contadores['salvos']} salvos, {contadores['erros']} erros.")
        return f"Processamento finalizado e dados salvos no banco."

    except Exception as e:
        print("[FATAL] Erro durante o salvamento:", e)
        print(traceback.format_exc())
        return f"Erro geral ao salvar: {e}"