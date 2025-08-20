import pandas as pd
import unicodedata
from PySide6.QtWidgets import QFileDialog
from db.conexao import conectarBanco, fecharBanco
from utils.aliquota import formatarAliquota
from utils.mensagem import mensagem_aviso, mensagem_error, mensagem_sucesso

COLUNAS_SINONIMAS = {
    'CODIGO': ['codigo', 'código', 'cod', 'cod_produto', 'id'],
    'PRODUTO': ['produto', 'descricao', 'descrição', 'nome', 'produto_nome'],
    'NCM': ['ncm', 'cod_ncm', 'ncm_code'],
    'ALIQUOTA': ['aliquota', 'alíquota', 'aliq', 'aliq_icms'],
}

COLUNAS_NECESSARIAS = ['CODIGO', 'PRODUTO', 'NCM', 'ALIQUOTA']


def normalizar_texto(texto):
    return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode().lower().replace('_', '').replace(' ', '')


def mapear_colunas(df):
    colunas_encontradas = {}
    colunas_atuais = [col.lower().strip() for col in df.columns]

    for coluna_padrao, sinonimos in COLUNAS_SINONIMAS.items():
        for nome in sinonimos:
            for col in df.columns:
                if col.strip().lower() == nome.lower():
                    colunas_encontradas[coluna_padrao] = col
                    break
            if coluna_padrao in colunas_encontradas:
                break

    if all(col in colunas_encontradas for col in COLUNAS_NECESSARIAS):
        return colunas_encontradas

    mensagem_error(f"Erro: Colunas esperadas não encontradas. Colunas atuais: {df.columns.tolist()}")
    return None

def carregar_planilha():
    filename, _ = QFileDialog.getOpenFileName(None, "Enviar Tributação", "", "Arquivos Excel (*.xlsx)")
    if not filename:
        mensagem_aviso("Nenhum arquivo selecionado.")
        return None
    return filename

def preparar_dataframe(df, mapeamento, empresa_id):
    df = df.rename(columns=mapeamento)

    df[mapeamento['ALIQUOTA']] = (
        df[mapeamento['ALIQUOTA']].fillna('').astype(str).str.strip().apply(formatarAliquota)
    )

    def categoria_por_aliquota(aliquota):
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

    df['categoriaFiscal'] = df[mapeamento['ALIQUOTA']].apply(categoria_por_aliquota)

    df = df.drop_duplicates(subset=[mapeamento['CODIGO'], mapeamento['PRODUTO'], mapeamento['NCM']])

    df_resultado = df[
        [mapeamento['CODIGO'], mapeamento['PRODUTO'], mapeamento['NCM'], mapeamento['ALIQUOTA'], 'categoriaFiscal']
    ].copy()
    df_resultado['empresa_id'] = empresa_id
    return df_resultado

def buscar_registros_existentes(cursor, empresa_id):
    cursor.execute("""
        SELECT codigo, produto, ncm, aliquota, categoriaFiscal FROM cadastro_tributacao
        WHERE empresa_id = %s
    """, (empresa_id,))
    return {
        (str(c).strip(), str(p).strip(), str(n).strip()): (str(a).strip(), str(cat).strip())
        for c, p, n, a, cat in cursor.fetchall()
    }

def processar_registros(df, mapeamento, registros_existentes, empresa_id):
    novos, atualizacoes = [], []

    for _, linha in df.iterrows():
        codigo = str(linha[mapeamento['CODIGO']]).strip()
        produto = str(linha[mapeamento['PRODUTO']]).strip()
        ncm = str(linha[mapeamento['NCM']]).strip()
        aliquota = str(linha[mapeamento['ALIQUOTA']]).strip()
        categoria_fiscal = str(linha['categoriaFiscal']).strip()

        chave = (codigo, produto, ncm)
        registro_atual = registros_existentes.get(chave)

        if not registro_atual:
            novos.append((empresa_id, codigo, produto, ncm, aliquota, categoria_fiscal))
        elif registro_atual != (aliquota, categoria_fiscal):
            atualizacoes.append((aliquota, categoria_fiscal, empresa_id, codigo, produto, ncm))

    novos = list(set(novos))
    atualizacoes = list(set(atualizacoes))

    return novos, atualizacoes

def salvar_registros(cursor, novos, atualizacoes):
    registros_pulou = 0

    if novos:
        for registro in novos:
            try:
                cursor.execute("""
                    INSERT INTO cadastro_tributacao (empresa_id, codigo, produto, ncm, aliquota, categoriaFiscal)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, registro)
            except Exception as e:
                if "1062" in str(e):
                    registros_pulou += 1
                    continue
                else:
                    raise e

        print(f"[DEBUG] {len(novos) - registros_pulou} novos registros inseridos.")
        if registros_pulou > 0:
            print(f"[DEBUG] {registros_pulou} registros duplicados ignorados.")

    if atualizacoes:
        cursor.executemany("""
            UPDATE cadastro_tributacao
            SET aliquota = %s, categoriaFiscal = %s
            WHERE empresa_id = %s AND codigo = %s AND produto = %s AND ncm = %s
        """, atualizacoes)
        print(f"[DEBUG] {len(atualizacoes)} registros atualizados.")


def enviar_tributacao(empresa_id, progress_bar):
    progress_bar.setValue(0)
    filename = carregar_planilha()
    if not filename:
        return

    conexao = conectarBanco()
    if not conexao:
        mensagem_error("Erro ao conectar ao banco de dados.")
        return

    progress_bar.setValue(10)

    try:
        df = pd.read_excel(filename, dtype=str)
        mapeamento = mapear_colunas(df)
        if not mapeamento:
            return

        df_preparado = preparar_dataframe(df, mapeamento, empresa_id)
        cursor = conexao.cursor()
        registros_existentes = buscar_registros_existentes(cursor, empresa_id)
        novos, atualizacoes = processar_registros(df_preparado, mapeamento, registros_existentes, empresa_id)

        salvar_registros(cursor, novos, atualizacoes)
        conexao.commit()

        total = len(novos) + len(atualizacoes)
        mensagem_sucesso(f"Tributação enviada com sucesso! Total: {total} registros.")
        print(f"[DEBUG] Total final processado: {total}")
        progress_bar.setValue(100)

    except Exception as e:
        conexao.rollback()
        mensagem_error(f"Ocorreu um erro: {str(e)}")
    finally:
        progress_bar.setValue(0)
        cursor.close()
        fecharBanco(conexao)
