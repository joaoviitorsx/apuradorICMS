import asyncio
from PySide6.QtCore import QObject, Signal, QEventLoop
from db.conexao import conectarBanco, fecharBanco

class SinalPopup(QObject):
    abrir_popup_signal = Signal(int, object)

    def __init__(self):
        super().__init__()
        self.resultado_popup = None
        self.event_loop = None
        self._lock = asyncio.Lock()
        self._popup_ativo = False

sinal_popup = SinalPopup()

async def verificaoPopupAliquota(empresa_id, janela_pai=None):
    print(f"[INFO] Verificando alíquotas nulas para empresa_id={empresa_id}...")

    async with sinal_popup._lock:
        if sinal_popup._popup_ativo:
            print("[INFO] Popup já está sendo processado por outra operação")
            return
        
        sinal_popup._popup_ativo = True
    
    try:
        conexao = conectarBanco()
        cursor = conexao.cursor()
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM (
                    SELECT MIN(codigo) AS codigo, produto, ncm
                    FROM cadastro_tributacao
                    WHERE empresa_id = %s AND (aliquota IS NULL OR TRIM(aliquota) = '')
                    GROUP BY produto, ncm
                ) AS sub
            """, (empresa_id,))
            count = cursor.fetchone()[0]
        finally:
            cursor.close()
            fecharBanco(conexao)

        print("[INFO] Conexão com banco encerrada.")

        if count > 0:
            print(f"[INFO] Existem {count} alíquotas nulas. Solicitando preenchimento via popup...")

            sinal_popup.event_loop = QEventLoop()
            sinal_popup.resultado_popup = None
            sinal_popup.abrir_popup_signal.emit(empresa_id, janela_pai)
            sinal_popup.event_loop.exec()

        else:
            print("[INFO] Nenhuma alíquota nula encontrada.")

    except Exception as e:
        print(f"[ERRO] Falha na verificação de popup: {e}")
        raise
    finally:
        sinal_popup._popup_ativo = False
        print("[INFO] Lock do popup liberado")

async def preencherTributacao(empresa_id, parent=None):
    print(f"[VERIFICAÇÃO] Preenchendo cadastro_tributacao com produtos da empresa_id={empresa_id}")
    conexao = conectarBanco()
    cursor = conexao.cursor()

    try:
        cursor.execute("""
            INSERT IGNORE INTO cadastro_tributacao (
                empresa_id, codigo, produto, ncm, aliquota, aliquota_antiga
            )
            SELECT 
                sub.empresa_id,
                sub.cod_item AS codigo,
                sub.produto,
                sub.ncm,
                NULL,
                NULL
            FROM (
                SELECT 
                    c.empresa_id,
                    c.cod_item,
                    COALESCE(p.descr_item, c.descr_compl) AS produto,
                    p.cod_ncm AS ncm
                FROM c170 c
                JOIN c100 cc 
                    ON cc.id = c.id_c100
                JOIN cadastro_fornecedores f 
                    ON cc.cod_part = f.cod_part
                    AND f.empresa_id = c.empresa_id
                LEFT JOIN `0200` p 
                    ON c.cod_item = p.cod_item 
                    AND p.empresa_id = c.empresa_id
                WHERE c.empresa_id = %s
                AND c.cfop IN (
                        '1101', '1401', '1102', '1403', '1910', '1116',
                        '2101', '2102', '2401', '2403', '2910', '2116'
                )
                AND (
                        (f.uf = 'CE' AND f.decreto = 'Não')
                        OR
                        (f.uf <> 'CE')
                )
            ) AS sub
            WHERE NOT EXISTS (
                SELECT 1 FROM cadastro_tributacao ct
                WHERE ct.empresa_id = sub.empresa_id
                AND ct.codigo = sub.cod_item
                AND ct.produto = sub.produto
                AND ct.ncm = sub.ncm
            )
        """, (empresa_id,))

        novos = cursor.rowcount
        conexao.commit()
        print(f"[OK] {novos} códigos únicos inseridos na tabela cadastro_tributacao.")

    except Exception as e:
        print(f"[ERRO] Falha ao preencher cadastro_tributacao: {e}")
        conexao.rollback()

    finally:
        cursor.close()
        fecharBanco(conexao)
        print("[FIM] Preenchimento de cadastro_tributacao concluído.")









