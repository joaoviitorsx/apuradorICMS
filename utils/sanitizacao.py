import re

TAMANHOS_MAXIMOS = {
    'unid': 2,
    'cod_item': 60,
    'descr_item': 255,
    'descr_compl': 255,
    'cod_nat': 11,
    'cod_cta': 255,
    'cod_part': 60,
    'nome': 100,
}

def limpar_aliquota(valor):
    if not valor:
        return None
    valor = str(valor).strip().replace('%', '').replace(',', '.')
    try:
        num = float(valor)
        if num == 0:
            return '0%'
        return f"{num:.2f}%"
    except ValueError:
        valor_upper = valor.upper()
        if valor_upper in ["ST", "ISENTO", "PAUTA"]:
            return valor_upper
        return None
    
def truncar(valor, limite):
    if valor is None:
        return None
    valor_str = str(valor)
    return valor_str[:limite]

def corrigirUnidade(valor):
    if not valor:
        return 'UN'
        
    valor_str = str(valor)
    
    if re.match(r'^\d+[,\.]\d+$', valor_str) or re.match(r'^\d+$', valor_str):
        return 'UN'
    
    match = re.match(r'^([A-Za-z]+)(\d+)', valor_str)
    if match:
        unidade_base = match.group(1)
        return unidade_base
    
    tamanho_banco = 3
    if len(valor_str) > tamanho_banco:
        return valor_str[:tamanho_banco]
    
    return valor_str

def corrigir_cst_icms(valor):
    if not valor:
        return "00"

    valor_str = str(valor).strip().replace(',', '.')

    if valor_str.replace('.', '').isdigit():
        try:
            return str(int(float(valor_str))).zfill(2)[:2]
        except ValueError:
            return "00"

    return valor_str[:2]

def corrigir_cfop(valor):
    if valor is None:
        return valor
    valor = re.sub(r'\D', '', valor)
    if len(valor) > 4:
        valor = valor[:4]
    if len(valor) < 4:
        valor = valor.zfill(4)
    return valor

def corrigir_ind_mov(valor):
    if not valor:
        return '0'
    valor_str = str(valor)
    if len(valor_str) > 1:
        return valor_str[:1]
    return valor_str

def validar_estrutura_c170(dados):
    try:
        if not dados or len(dados) < 45:
            return False
        periodo = dados[0]
        filial = dados[41]
        num_doc = dados[43]
        if not (periodo and filial and num_doc):
            return False
        return True
    except Exception:
        return False


def sanitizar_campo(campo, valor):
    regras = {
        'cod_item': lambda v: truncar(v, 60),
        'descr_item': lambda v: truncar(v, 255),
        'descr_compl': lambda v: truncar(v, 255),
        'unid_inv': corrigirUnidade,
        'unid': corrigirUnidade,
        'cod_part': lambda v: truncar(v, 60),
        'nome': lambda v: truncar(v, 100),
        'ind_mov': corrigir_ind_mov,
        'cod_mod': lambda v: str(v).zfill(2)[:2] if v is not None else '00',
        'cst_icms': corrigir_cst_icms,
        'cfop': corrigir_cfop,
        'cod_nat': lambda v: truncar(v, 11),
        'cod_cta': lambda v: truncar(v, 255),
        'reg': lambda v: truncar(v, 4),
        'vl_item': lambda v: str(v).replace(',', '.') if isinstance(v, str) else v,
        'vl_desc': lambda v: str(v).replace(',', '.') if isinstance(v, str) else v,
        'vl_merc': lambda v: str(v).replace(',', '.') if isinstance(v, str) else v,
        'aliq_icms': lambda v: str(v).replace(',', '.') if isinstance(v, str) else v,
        'aliq_ipi': lambda v: str(v).replace(',', '.') if isinstance(v, str) else v,
        'aliq_pis': lambda v: str(v).replace(',', '.') if isinstance(v, str) else v,
        'aliq_cofins': lambda v: str(v).replace(',', '.') if isinstance(v, str) else v,
    }

    try:
        if campo in regras:
            novo_valor = regras[campo](valor)
            return novo_valor
        return valor
    except Exception:
        return valor

def sanitizar_registro(registro_dict):
    return {campo: sanitizar_campo(campo, valor) for campo, valor in registro_dict.items()}

def calcular_periodo(dt_ini_0000):
    return f'{dt_ini_0000[2:4]}/{dt_ini_0000[4:]}' if dt_ini_0000 else '00/0000'
