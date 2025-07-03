import aiohttp
import asyncio
import re
from functools import wraps
from time import time

def create_cache(ttl=3600):
    cache_dict = {}
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            key = str((args, frozenset(kwargs.items())))
            now = time()
            if key in cache_dict:
                cached_time, result = cache_dict[key]
                if now - cached_time <= ttl:
                    return result
            result = await func(*args, **kwargs)
            cache_dict[key] = (time(), result)
            return result
        return wrapper
    return decorator


def remover_caracteres_nao_numericos(valor: str) -> str:
    return re.sub(r'\D', '', valor)


lista_cnaes = [
    '4623108', '4623199', '4632001', '4637107', '4639701', '4639702',
    '4646002', '4647801', '4649408', '4635499', '4637102', '4637199',
    '4644301', '4632003', '4691500', '4693100', '3240099', '4649499',
    '8020000', '4711301', '4711302', '4712100', '4721103', '4721104',
    '4729699', '4761003', '4789005', '4771701', '4771702', '4771703',
    '4772500', '4763601'
]

cache_resultados = {}
semaforo = asyncio.Semaphore(5)


@create_cache()
async def buscar_informacoes(cnpj: str, semaforo, tentativas: int = 6) -> tuple:
    url = f'https://minhareceita.org/{cnpj}'
    timeout = aiohttp.ClientTimeout(total=30)

    async with semaforo:
        for tentativa in range(1, tentativas + 1):
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url) as resposta:
                        if resposta.status == 200:
                            dados = await resposta.json()
                            cnae_codigo = str(dados.get('cnae_fiscal', ''))
                            existe_na_lista = "Sim" if cnae_codigo in lista_cnaes else "Não"
                            uf = dados.get('uf', '')
                            simples = "Sim" if dados.get('opcao_pelo_simples') else "Não"
                            return cnae_codigo, existe_na_lista, uf, simples
                        else:
                            print(f"[{tentativa}] Erro {resposta.status} para CNPJ {cnpj}")
            except asyncio.TimeoutError:
                print(f"[{tentativa}] Timeout no CNPJ {cnpj}")
            except Exception as e:
                print(f"[{tentativa}] Erro inesperado no CNPJ {cnpj}: {e}")
            await asyncio.sleep(2 ** tentativa)

    print(f"[FALHA] Tentativas esgotadas para o CNPJ {cnpj}")
    return None, None, None, None


async def _processar_cnpj(cnpj: str, resultados: dict, semaforo):
    cnpj_limpo = remover_caracteres_nao_numericos(cnpj)

    if not validar_cnpj(cnpj_limpo):
        print(f"[IGNORADO] CNPJ inválido: {cnpj}")
        return

    cnae_codigo, existe_na_lista, uf, simples = await buscar_informacoes(cnpj_limpo, semaforo)

    if all([cnae_codigo, existe_na_lista, uf]):
        resultados[cnpj] = (cnae_codigo, existe_na_lista, uf, simples)
        cache_resultados[cnpj] = resultados[cnpj]
    else:
        print(f"[ERRO] Dados incompletos para o CNPJ {cnpj}. Ignorado.")


async def processar_cnpjs(cnpjs: list[str]) -> dict:
    resultados = {}
    tasks = []
    semaforo = asyncio.Semaphore(5)

    for cnpj in cnpjs:
        if cnpj in cache_resultados:
            resultado = cache_resultados[cnpj]
            if all(resultado):
                resultados[cnpj] = resultado
            else:
                print(f"[CACHE ERRO] Cache inválido para {cnpj}. Ignorando.")
        else:
            tasks.append(_processar_cnpj(cnpj, resultados, semaforo))

    if tasks:
        await asyncio.gather(*tasks)

    return resultados


def validar_cnpj(cnpj: str) -> bool:
    cnpj = ''.join(filter(str.isdigit, cnpj))
    if len(cnpj) != 14 or cnpj == cnpj[0] * 14:
        return False

    def calc_digito(cnpj, peso):
        soma = sum(int(cnpj[i]) * peso[i] for i in range(len(peso)))
        digito = 11 - (soma % 11)
        return '0' if digito > 9 else str(digito)

    peso1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    peso2 = [6] + peso1
    return (calc_digito(cnpj, peso1) == cnpj[12] and
            calc_digito(cnpj, peso2) == cnpj[13])

def formatar_cnpj(cnpj: str) -> str:
    cnpj = ''.join(filter(str.isdigit, cnpj))
    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"

async def consultar_cnpj_api_async(cnpj: str) -> dict:
    cnpj = re.sub(r'\D', '', cnpj)
    if len(cnpj) != 14:
        raise ValueError("CNPJ inválido.")

    url = f'https://minhareceita.org/{cnpj}'
    timeout = aiohttp.ClientTimeout(total=15)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as resposta:
            if resposta.status == 200:
                return await resposta.json()
            raise ValueError(f"Erro {resposta.status} ao consultar CNPJ.")

def consultar_cnpj_api(cnpj: str) -> dict:
    return asyncio.run(consultar_cnpj_api_async(cnpj))