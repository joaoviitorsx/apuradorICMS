import re

def Conversor(valor_str):
    try:
        if valor_str is None:
            return 0.0

        valor = str(valor_str).strip().upper()

        if valor in ['ISENTO', 'ST', 'N/A', 'PAUTA', '', None]:
            return 0.0

        valor = re.sub(r'[^0-9.,]', '', valor)

        if ',' in valor:
            valor = valor.replace('.', '').replace(',', '.')

        return round(float(valor), 4)

    except (ValueError, TypeError):
        return 0.0
