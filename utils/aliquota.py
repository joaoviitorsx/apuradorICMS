import math

def formatarAliquota(aliquota):
    aliquota = aliquota.strip()
    if aliquota and aliquota[0].isdigit():
        try:
            aliquota_float = round(float(aliquota) * 100, 2)
            return f"{aliquota_float:.2f}%"
        except ValueError:
            return aliquota
    return aliquota
