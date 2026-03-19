import datetime
import time
import pyodbc
import requests
import pandas as pd
import schedule
import logging

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
    encoding='utf-8',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.DEBUG
)
logger = logging.getLogger("TIAMA_LINHAS")
console_handler = logging.StreamHandler()
console_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(console_handler)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÕES
# ─────────────────────────────────────────────────────────────────────────────
API_HOST  = "10.79.22.207"          # IP do servidor da API
API_PORT  = 8001                     # Porta
LINHAS    = [591, 592, 593, 594, 595, 596]
INTERVALO_ENTRE_LINHAS = 30          # segundos entre consultas de cada linha

# Dados do banco de dados SQL Server
DB_SERVER   = '10.79.22.206'
DB_DATABASE = 'TIAMA'
DB_USERNAME = 'ROOT'
DB_PASSWORD = 'root'

# Aqui você pode definir pra qual tabela vai os dados de cada linha
TABELA_POR_LINHA = {
    591: 'IQSCANVR1',
    592: 'IQSCANVR1',
    593: 'IQSCANVR1',
    594: 'IQSCANVR2',
    595: 'IQSCANVR2',
    596: 'IQSCANVR2',
}

# ─────────────────────────────────────────────────────────────────────────────
# CONEXÃO COM O BANCO DE DADOS
# ─────────────────────────────────────────────────────────────────────────────
def SQL():
    try:
        conn = pyodbc.connect(
            f'Driver={{ODBC Driver 17 for SQL Server}};'
            f'Server={DB_SERVER};Database={DB_DATABASE};'
            f'UID={DB_USERNAME};PWD={DB_PASSWORD}'
        )
        logger.info("Conexão bem-sucedida com o banco de dados SQL Server!")
        cursor = conn.cursor()
        return cursor, conn
    except pyodbc.Error as e:
        logger.error("Erro durante a conexão com o banco de dados:")
        logger.error(e)
        raise

# ─────────────────────────────────────────────────────────────────────────────
# CONSULTA À API -> No modo atual consulta os dados 1x a cada hora.
# Retry automático: tenta até 3 vezes com 10s de espera entre cada tentativa.
# ─────────────────────────────────────────────────────────────────────────────
def APIJSON(line_number, from_dt, until_dt):
    """
    Consulta a API para uma linha específica dentro do intervalo de tempo
    informado.  from_dt e until_dt devem ser objetos datetime.datetime.
    """
    from_str  = from_dt.strftime("%Y-%m-%dT%H:%M:%S")
    until_str = until_dt.strftime("%Y-%m-%dT%H:%M:%S")
    url = (
        f"http://{API_HOST}:{API_PORT}/rafisxml?getCounters"
        f"&line=L{line_number}"
        f"&from={from_str}"
        f"&until={until_str}&"
    )
    logger.info(f"Consultando API → {url}")
    for tentativa in range(1, 4):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            logger.info(f"Conexão com a API bem-sucedida (L{line_number})")
            return response.json()
        except requests.RequestException as error:
            logger.warning(f"Tentativa {tentativa}/3 falhou (L{line_number}): {error}")
            if tentativa < 3:
                time.sleep(10)
    raise Exception(f"API indisponível após 3 tentativas (L{line_number})")

# ─────────────────────────────────────────────────────────────────────────────
# HELPER — divisão segura para porcentagens
# Retorna array([0.0]) se qualquer dos arrays vier vazio ou denominador = 0
# ─────────────────────────────────────────────────────────────────────────────
import numpy as np

def safe_pct(numerator, denominator):
    if (len(numerator) == 0 or len(denominator) == 0 or denominator[0] == 0):
        return np.array([0.0])
    return ((numerator / denominator) * 100).round(1)

# ─────────────────────────────────────────────────────────────────────────────
# PROCESSAMENTO DOS DADOS
# ─────────────────────────────────────────────────────────────────────────────
def processar_dados(response_json, line_number):

    data = []
    for info in response_json["infos"]:
        row_data = [
            info["value"],
            info["equation"],
            info["label"],
            info["line"],
            info["threshold"],
            info["name"],
            info["numero"],
            info["info"],
            info["syskey"],
            info["origname"]
        ]
        data.append(row_data)

    columns = [
        "Valor", "Equacao", "Label1", "Line", "Limite", "Nome",
        "Numero", "Informacao", "Chave_do_sistema", "Nome_original"
    ]
    df = pd.DataFrame(data, columns=columns)

    # ── IS ───────────────────────────────────────────────────────────────────
    filtered_df_ISIN    = df[df['Equacao'] == 'IS.IN'] # Gotas cortadas
    filtered_df_ISDEF2  = df[df['Equacao'] == 'IS.DEF2'] # Rejeição de máquinas
    filtered_df_ISOUT   = df[df['Equacao'] == 'IS.OUT'] # Gotas carregadas
    # filtered_df_ISBPM   = df[df['Equacao'] == 'IS.BPM'] # Era pra ser GPM, mas não é muito preciso. Ao invés disso faço calculando Gotas Cortadas / 60
    filtered_df_ISS1    = df[df['Equacao'] == 'IS.S1'] # Eficiência da Seção 1
    filtered_df_ISS2    = df[df['Equacao'] == 'IS.S2'] # Eficiência da Seção 2
    filtered_df_ISS3    = df[df['Equacao'] == 'IS.S3'] # Eficiência da Seção 3
    filtered_df_ISS4    = df[df['Equacao'] == 'IS.S4'] # Eficiência da Seção 4
    filtered_df_ISS5    = df[df['Equacao'] == 'IS.S5'] # Eficiência da Seção 5
    filtered_df_ISS6    = df[df['Equacao'] == 'IS.S6'] # Eficiência da Seção 6
    filtered_df_ISS7    = df[df['Equacao'] == 'IS.S7'] # Eficiência da Seção 7
    filtered_df_ISS8    = df[df['Equacao'] == 'IS.S8'] # Eficiência da Seção 8
    filtered_df_ISS9    = df[df['Equacao'] == 'IS.S9'] # Eficiência da Seção 9
    filtered_df_ISS10   = df[df['Equacao'] == 'IS.S10'] # Eficiência da Seção 10
    filtered_df_ISS11   = df[df['Equacao'] == 'IS.S11'] # Eficiência da Seção 11
    filtered_df_ISS12   = df[df['Equacao'] == 'IS.S12'] # Eficiência da Seção 12

    ValorISIN           = filtered_df_ISIN["Valor"].values
    ValorISDEF2         = filtered_df_ISDEF2["Valor"].values
    ValorISOUT          = filtered_df_ISOUT["Valor"].values
    ValorISBPM          = (ValorISIN / 60).round(1) if len(ValorISIN) > 0 else np.array([0.0])  # Convertendo para GPM (gotas por minuto)
    ValorEficISS1       = filtered_df_ISS1["Valor"].values
    ValorEficISS2       = filtered_df_ISS2["Valor"].values
    ValorEficISS3       = filtered_df_ISS3["Valor"].values
    ValorEficISS4       = filtered_df_ISS4["Valor"].values
    ValorEficISS5       = filtered_df_ISS5["Valor"].values
    ValorEficISS6       = filtered_df_ISS6["Valor"].values
    ValorEficISS7       = filtered_df_ISS7["Valor"].values
    ValorEficISS8       = filtered_df_ISS8["Valor"].values
    ValorEficISS9       = filtered_df_ISS9["Valor"].values
    ValorEficISS10      = filtered_df_ISS10["Valor"].values
    ValorEficISS11      = filtered_df_ISS11["Valor"].values
    ValorEficISS12      = filtered_df_ISS12["Valor"].values
    ValorISREJPORCEM    = safe_pct(ValorISDEF2, ValorISIN)
    ValorISLoadedPorcem = safe_pct(ValorISOUT,  ValorISIN)

    DATA_ISMACHINE = [[
        ValorISIN, ValorISOUT, ValorISDEF2, ValorISBPM,
        ValorEficISS1, ValorEficISS2, ValorEficISS3, ValorEficISS4,
        ValorEficISS5, ValorEficISS6, ValorEficISS7, ValorEficISS8,
        ValorEficISS9, ValorEficISS10, ValorEficISS11, ValorEficISS12,
        ValorISREJPORCEM, ValorISLoadedPorcem,
        line_number, "IS"
    ]]
    df_DATA_ISMACHINE = pd.DataFrame(DATA_ISMACHINE, columns=[
        'Gotas_cortadas', 'Gotas_Carregadas', 'Rej_IS', 'GPMIS',
        'Efic_ISS1', 'Efic_ISS2', 'Efic_ISS3', 'Efic_ISS4',
        'Efic_ISS5', 'Efic_ISS6', 'Efic_ISS7', 'Efic_ISS8',
        'Efic_ISS9', 'Efic_ISS10', 'Efic_ISS11', 'Efic_ISS12',
        'rej_MaquinaPorcem', 'gotas_loadedPorcem', 'Line', 'Maq'
    ])
    df_meltedIS = pd.melt(df_DATA_ISMACHINE, id_vars=['Line', 'Maq'],
                          value_vars=[
                              'Gotas_cortadas', 'Gotas_Carregadas', 'Rej_IS', 'GPMIS',
                              'Efic_ISS1', 'Efic_ISS2', 'Efic_ISS3', 'Efic_ISS4',
                              'Efic_ISS5', 'Efic_ISS6', 'Efic_ISS7', 'Efic_ISS8',
                              'Efic_ISS9', 'Efic_ISS10', 'Efic_ISS11', 'Efic_ISS12',
                              'rej_MaquinaPorcem', 'gotas_loadedPorcem'
                          ])

    # ── XPAR ─────────────────────────────────────────────────────────────────
    filtered_df_XPARIN   = df[df['Equacao'] == 'XPAR.IN'] # Inspecionados XPAR
    filtered_df_XPARERR  = df[df['Equacao'] == 'XPAR.ERR'] # Rejeição no XPAR
    filtered_df_XPARDEF1 = df[df['Equacao'] == 'XPAR.DEF1'] # Missing do XPAR

    ValorXparIN        = filtered_df_XPARIN["Valor"].values
    ValorXparERR       = filtered_df_XPARERR["Valor"].values
    ValorXparDEF1      = filtered_df_XPARDEF1["Valor"].values
    ValorXparERRPORCEM = safe_pct(ValorXparERR, ValorXparIN)

    DATA_XPAR = [[ValorXparIN, ValorXparERR, ValorXparDEF1,
                  ValorXparERRPORCEM, line_number, 'XPAR']]
    df_Xpar = pd.DataFrame(DATA_XPAR, columns=[
        'INSPECAO', 'REJEICAO', 'MISSING', 'REJEICAOPORCEM', 'Line', 'Maq'
    ])
    df_meltedXPAR = pd.melt(df_Xpar, id_vars=['Line', 'Maq'],
                             value_vars=['INSPECAO', 'REJEICAO', 'MISSING', 'REJEICAOPORCEM'])

    # ── LEHR ─────────────────────────────────────────────────────────────────
    filtered_df_LEHRIN   = df[df['Equacao'] == 'LEHR.IN']
    filtered_df_LEHROUT  = df[df['Equacao'] == 'LEHR.OUT']
    dropduplicateLEHROUT = filtered_df_LEHROUT.drop_duplicates()

    ValorLEHRIN        = filtered_df_LEHRIN["Valor"].values
    ValorLEHROUT       = dropduplicateLEHROUT["Valor"].values
    ValorLEHROUTPORCEM = safe_pct(ValorLEHROUT, ValorISIN)
    ValorLEHRINPORCEM  = safe_pct(ValorLEHRIN,  ValorISIN)

    DATA_LEHR = [[ValorLEHRIN, ValorLEHROUT,
                  ValorLEHRINPORCEM, ValorLEHROUTPORCEM,
                  line_number, 'LEHR']]
    df_LEHR = pd.DataFrame(DATA_LEHR, columns=[
        'Enforna_entrada', 'Entrada_mcal',
        'Enforna_entradaPORCEM', 'Entrada_mcalPORCEM', 'Line', 'Maq'
    ])
    df_meltedLEHR = pd.melt(df_LEHR, id_vars=['Line', 'Maq'],
                             value_vars=['Enforna_entrada', 'Entrada_mcal',
                                         'Enforna_entradaPORCEM', 'Entrada_mcalPORCEM'])

    # ── MCAL A ───────────────────────────────────────────────────────────────
    filtered_df_MCALIN_A             = df[df['Equacao'] == 'SW.IN.A'] # Inspecionados MCAL A
    filtered_df_MCALREJ_A            = df[df['Equacao'] == 'SW.ERR.A'] # Rejeição MCAL A
    filtered_df_MCALASPECTO_A        = df[df['Equacao'] == 'SW.DEF1.A'] # Aspecto
    filtered_df_MCALSTRESS_A         = df[df['Equacao'] == 'SW.DEF2.A'] # Stress
    filtered_df_MCALDIMENSIONAL_A    = df[df['Equacao'] == 'SW.DEF3.A'] # Dimensional
    filtered_df_MCALBAIXOCONTRASTE_A = df[df['Equacao'] == 'SW.DEF16.A'] # Baixo Contraste
    filtered_df_MCALALTURA_A         = df[df['Equacao'] == 'SW.DEF4.A'] # Altura
    filtered_df_MCALDIAMETRO_A       = df[df['Equacao'] == 'SW.DEF5.A'] # Diâmetro
    filtered_df_MCALVERTICALIDADE_A  = df[df['Equacao'] == 'SW.DEF6.A'] # Verticalidade
    filtered_df_MCALINCLUSAO_A       = df[df['Equacao'] == 'SW.DEF7.A'] # Inclusão
    filtered_df_MCALBOLHA_A          = df[df['Equacao'] == 'SW.DEF8.A'] # Bolha
    filtered_df_MCALBIRDSWING_A      = df[df['Equacao'] == 'SW.DEF9.A'] # Birdswing
    filtered_df_MCALFINO_A           = df[df['Equacao'] == 'SW.DEF10.A'] # Fino
    filtered_df_MCALESTIRAMENTO_A    = df[df['Equacao'] == 'SW.DEF11.A'] # Estiramento
    filtered_df_MCALOBJBORDO_A       = df[df['Equacao'] == 'SW.DEF12.A'] # ObjDeBordo
    filtered_df_MCALDENSIDADE_A      = df[df['Equacao'] == 'SW.DEF13.A'] # Densidade
    filtered_df_MCALOUTROSDEFEITOS_A = df[df['Equacao'] == 'SW.DEF14.A'] # Outros Defeitos
    filtered_df_MCALPERFILGLOBAL_A   = df[df['Equacao'] == 'SW.DEF15.A'] # Perfil Global do Corpo

    ValorMCALIN_A             = filtered_df_MCALIN_A["Valor"].values
    ValorMCALREJ_A            = filtered_df_MCALREJ_A["Valor"].values
    ValorMCALASPECTO_A        = filtered_df_MCALASPECTO_A["Valor"].values
    ValorMCALSTRESS_A         = filtered_df_MCALSTRESS_A["Valor"].values
    ValorMCALDIMENSIONAL_A    = filtered_df_MCALDIMENSIONAL_A["Valor"].values
    ValorMCALBAIXOCONTRASTE_A = filtered_df_MCALBAIXOCONTRASTE_A["Valor"].values
    ValorMCALALTURA_A         = filtered_df_MCALALTURA_A["Valor"].values
    ValorMCALDIAMETRO_A       = filtered_df_MCALDIAMETRO_A["Valor"].values
    ValorMCALVERTICALIDADE_A  = filtered_df_MCALVERTICALIDADE_A["Valor"].values
    ValorMCALINCLUSAO_A       = filtered_df_MCALINCLUSAO_A["Valor"].values
    ValorMCALBOLHA_A          = filtered_df_MCALBOLHA_A["Valor"].values
    ValorMCALBIRDSWING_A      = filtered_df_MCALBIRDSWING_A["Valor"].values
    ValorMCALFINO_A           = filtered_df_MCALFINO_A["Valor"].values
    ValorMCALESTIRAMENTO_A    = filtered_df_MCALESTIRAMENTO_A["Valor"].values
    ValorMCALOBJBORDO_A       = filtered_df_MCALOBJBORDO_A["Valor"].values
    ValorMCALDENSIDADE_A      = filtered_df_MCALDENSIDADE_A["Valor"].values
    ValorMCALOUTROSDEFEITOS_A = filtered_df_MCALOUTROSDEFEITOS_A["Valor"].values
    ValorMCALPERFILGLOBAL_A   = filtered_df_MCALPERFILGLOBAL_A["Valor"].values

    DATA_MCALA = [[
        ValorMCALIN_A, ValorMCALREJ_A, ValorMCALASPECTO_A, ValorMCALSTRESS_A,
        ValorMCALDIMENSIONAL_A, ValorMCALBAIXOCONTRASTE_A, ValorMCALALTURA_A,
        ValorMCALDIAMETRO_A, ValorMCALVERTICALIDADE_A, ValorMCALINCLUSAO_A,
        ValorMCALBOLHA_A, ValorMCALBIRDSWING_A, ValorMCALFINO_A,
        ValorMCALESTIRAMENTO_A, ValorMCALOBJBORDO_A, ValorMCALDENSIDADE_A,
        ValorMCALOUTROSDEFEITOS_A, ValorMCALPERFILGLOBAL_A,
        line_number, 'MCALA'
    ]]
    df_MCALA = pd.DataFrame(DATA_MCALA, columns=[
        'INSPECIONADOS', 'REJEICAO', 'Aspecto', 'Stress', 'Dimensional',
        'BaixoContraste', 'Altura', 'Diametro', 'Verticalidade', 'Inclusao',
        'Bolha', 'Birdswing', 'Fino', 'Estiramento', 'ObjDeBordo',
        'densidade', 'OutrosDef', 'PerfilGlobalCorpo', 'Line', 'Maq'
    ])
    df_meltedMCALA = pd.melt(df_MCALA, id_vars=['Line', 'Maq'],
                              value_vars=[
                                  'INSPECIONADOS', 'REJEICAO', 'Aspecto', 'Stress',
                                  'Dimensional', 'BaixoContraste', 'Altura', 'Diametro',
                                  'Verticalidade', 'Inclusao', 'Bolha', 'Birdswing',
                                  'Fino', 'Estiramento', 'ObjDeBordo', 'densidade',
                                  'OutrosDef', 'PerfilGlobalCorpo'
                              ])

    # ── MCAL B ───────────────────────────────────────────────────────────────
    filtered_df_MCALIN_B             = df[df['Equacao'] == 'SW.IN.B'] # Inspecionados MCAL B
    filtered_df_MCALREJ_B            = df[df['Equacao'] == 'SW.ERR.B'] # Rejeição MCAL B
    filtered_df_MCALBSPECTO_B        = df[df['Equacao'] == 'SW.DEF1.B'] # Aspecto
    filtered_df_MCALSTRESS_B         = df[df['Equacao'] == 'SW.DEF2.B'] # Stress
    filtered_df_MCALDIMENSIONAL_B    = df[df['Equacao'] == 'SW.DEF3.B'] # Dimensional
    filtered_df_MCALBAIXOCONTRASTE_B = df[df['Equacao'] == 'SW.DEF16.B'] # Baixo Contraste
    filtered_df_MCALBLTURA_B         = df[df['Equacao'] == 'SW.DEF4.B'] # Altura
    filtered_df_MCALDIAMETRO_B       = df[df['Equacao'] == 'SW.DEF5.B'] # Diâmetro
    filtered_df_MCALVERTICALIDADE_B  = df[df['Equacao'] == 'SW.DEF6.B'] # Verticalidade
    filtered_df_MCALINCLUSAO_B       = df[df['Equacao'] == 'SW.DEF7.B'] # Inclusão
    filtered_df_MCALBOLHA_B          = df[df['Equacao'] == 'SW.DEF8.B'] # Bolha
    filtered_df_MCALBIRDSWING_B      = df[df['Equacao'] == 'SW.DEF9.B'] # Birdswing
    filtered_df_MCALFINO_B           = df[df['Equacao'] == 'SW.DEF10.B'] # Fino
    filtered_df_MCALESTIRAMENTO_B    = df[df['Equacao'] == 'SW.DEF11.B'] # Estiramento
    filtered_df_MCALOBJBORDO_B       = df[df['Equacao'] == 'SW.DEF12.B'] # ObjDeBordo
    filtered_df_MCALDENSIDADE_B      = df[df['Equacao'] == 'SW.DEF13.B'] # Densidade
    filtered_df_MCALOUTROSDEFEITOS_B = df[df['Equacao'] == 'SW.DEF14.B'] # Outros Defeitos
    filtered_df_MCALPERFILGLOBAL_B   = df[df['Equacao'] == 'SW.DEF15.B'] # Perfil Global do Corpo

    ValorMCALIN_B             = filtered_df_MCALIN_B["Valor"].values
    ValorMCALREJ_B            = filtered_df_MCALREJ_B["Valor"].values
    ValorMCALASPECTO_B        = filtered_df_MCALBSPECTO_B["Valor"].values
    ValorMCALSTRESS_B         = filtered_df_MCALSTRESS_B["Valor"].values
    ValorMCALDIMENSIONAL_B    = filtered_df_MCALDIMENSIONAL_B["Valor"].values
    ValorMCALBAIXOCONTRASTE_B = filtered_df_MCALBAIXOCONTRASTE_B["Valor"].values
    ValorMCALALTURA_B         = filtered_df_MCALBLTURA_B["Valor"].values
    ValorMCALDIAMETRO_B       = filtered_df_MCALDIAMETRO_B["Valor"].values
    ValorMCALVERTICALIDADE_B  = filtered_df_MCALVERTICALIDADE_B["Valor"].values
    ValorMCALINCLUSAO_B       = filtered_df_MCALINCLUSAO_B["Valor"].values
    ValorMCALBOLHA_B          = filtered_df_MCALBOLHA_B["Valor"].values
    ValorMCALBIRDSWING_B      = filtered_df_MCALBIRDSWING_B["Valor"].values
    ValorMCALFINO_B           = filtered_df_MCALFINO_B["Valor"].values
    ValorMCALESTIRAMENTO_B    = filtered_df_MCALESTIRAMENTO_B["Valor"].values
    ValorMCALOBJBORDO_B       = filtered_df_MCALOBJBORDO_B["Valor"].values
    ValorMCALDENSIDADE_B      = filtered_df_MCALDENSIDADE_B["Valor"].values
    ValorMCALOUTROSDEFEITOS_B = filtered_df_MCALOUTROSDEFEITOS_B["Valor"].values
    ValorMCALPERFILGLOBAL_B   = filtered_df_MCALPERFILGLOBAL_B["Valor"].values

    DATA_MCALB = [[
        ValorMCALIN_B, ValorMCALREJ_B, ValorMCALASPECTO_B, ValorMCALSTRESS_B,
        ValorMCALDIMENSIONAL_B, ValorMCALBAIXOCONTRASTE_B, ValorMCALALTURA_B,
        ValorMCALDIAMETRO_B, ValorMCALVERTICALIDADE_B, ValorMCALINCLUSAO_B,
        ValorMCALBOLHA_B, ValorMCALBIRDSWING_B, ValorMCALFINO_B,
        ValorMCALESTIRAMENTO_B, ValorMCALOBJBORDO_B, ValorMCALDENSIDADE_B,
        ValorMCALOUTROSDEFEITOS_B, ValorMCALPERFILGLOBAL_B,
        line_number, 'MCALB'
    ]]
    df_MCALB = pd.DataFrame(DATA_MCALB, columns=[
        'INSPECIONADOS', 'REJEICAO', 'Aspecto', 'Stress', 'Dimensional',
        'BaixoContraste', 'Altura', 'Diametro', 'Verticalidade', 'Inclusao',
        'Bolha', 'Birdswing', 'Fino', 'Estiramento', 'ObjDeBordo',
        'densidade', 'OutrosDef', 'PerfilGlobalCorpo', 'Line', 'Maq'
    ])
    df_meltedMCALB = pd.melt(df_MCALB, id_vars=['Line', 'Maq'],
                              value_vars=[
                                  'INSPECIONADOS', 'REJEICAO', 'Aspecto', 'Stress',
                                  'Dimensional', 'BaixoContraste', 'Altura', 'Diametro',
                                  'Verticalidade', 'Inclusao', 'Bolha', 'Birdswing',
                                  'Fino', 'Estiramento', 'ObjDeBordo', 'densidade',
                                  'OutrosDef', 'PerfilGlobalCorpo'
                              ])

    # ── MULTI A ──────────────────────────────────────────────────────────────
    filtered_df_MULTIIN_A              = df[df['Equacao'] == 'MULTI.IN.A'] # Inspecionados MULTI A
    filtered_df_MULTIREJ_A             = df[df['Equacao'] == 'MULTI.ERR.A'] # Rejeição MULTI A
    filtered_df_MULTIFUNDO_A           = df[df['Equacao'] == 'MULTI.DEF1.A'] # Fundo
    filtered_df_MULTIFUNDOSTRESS_A     = df[df['Equacao'] == 'MULTI.DEF2.A'] # Fundo Stress
    filtered_df_MULTIBOCA_A            = df[df['Equacao'] == 'MULTI.DEF3.A'] # Boca
    filtered_df_MULTIBOCAWEM_A         = df[df['Equacao'] == 'MULTI.DEF4.A'] # Fundo Compacto
    filtered_df_MULTIFUNDOCOMPACTO_A   = df[df['Equacao'] == 'MULTI.DEF5.A'] # Fundo Bolha
    filtered_df_MULTIFUNDOBOLHA_A      = df[df['Equacao'] == 'MULTI.DEF6.A'] # Fino
    filtered_df_MULTIFINO_A            = df[df['Equacao'] == 'MULTI.DEF7.A'] # Fundo Drawn
    filtered_df_MULTIFUNDODRAWN_A      = df[df['Equacao'] == 'MULTI.DEF8.A'] # Boca Line Over
    filtered_df_MULTIBOCALINEOVER_A    = df[df['Equacao'] == 'MULTI.DEF9.A'] # Boca Scaly
    filtered_df_MULTIBOCASCALY_A       = df[df['Equacao'] == 'MULTI.DEF10.A'] # Boca Overpressed
    filtered_df_MULTIBOCAOVERPRESSED_A = df[df['Equacao'] == 'MULTI.DEF11.A'] # Boca Sugary Top
    filtered_df_MULTIBOCASUGARYTOP_A   = df[df['Equacao'] == 'MULTI.DEF12.A'] # Boca Unfilled
    filtered_df_MULTIBOCAUNFILLED_A    = df[df['Equacao'] == 'MULTI.DEF13.A'] # Lasc Quebra Inter
    filtered_df_MULTILASCINTER_A       = df[df['Equacao'] == 'MULTI.DEF14.A'] # Lasc Quebra Exter
    filtered_df_MULTILASCEXTER_A       = df[df['Equacao'] == 'MULTI.DEF15.A'] # Lasc Quebra Super
    filtered_df_MULTILASCSUPER_A       = df[df['Equacao'] == 'MULTI.DEF16.A'] # Boca Wem
    # filtered_df_MULTIBLACKDEFECT_A     = df[df['Equacao'] == 'MULTI.DEF17.A'] # Black Defect -> Caso seja necessário, pode buscar. Precisa descomentar a linha 398 e 407 também.

    ValorMULTIIN_A              = filtered_df_MULTIIN_A["Valor"].values
    ValorMULTIREJ_A             = filtered_df_MULTIREJ_A["Valor"].values
    ValorMULTIFUNDO_A           = filtered_df_MULTIFUNDO_A["Valor"].values
    ValorMULTIFUNDOSTRESS_A     = filtered_df_MULTIFUNDOSTRESS_A["Valor"].values
    ValorMULTIBOCA_A            = filtered_df_MULTIBOCA_A["Valor"].values
    ValorMULTIBOCAWEM_A         = filtered_df_MULTIBOCAWEM_A["Valor"].values
    ValorMULTIFUNDOCOMPACTO_A   = filtered_df_MULTIFUNDOCOMPACTO_A["Valor"].values
    ValorMULTIFUNDOBOLHA_A      = filtered_df_MULTIFUNDOBOLHA_A["Valor"].values
    ValorMULTIFINO_A            = filtered_df_MULTIFINO_A["Valor"].values
    ValorMULTIFUNDODRAWN_A      = filtered_df_MULTIFUNDODRAWN_A["Valor"].values
    ValorMULTIBOCALINEOVER_A    = filtered_df_MULTIBOCALINEOVER_A["Valor"].values
    ValorMULTIBOCASCALY_A       = filtered_df_MULTIBOCASCALY_A["Valor"].values
    ValorMULTIBOCAOVERPRESSED_A = filtered_df_MULTIBOCAOVERPRESSED_A["Valor"].values
    ValorMULTIBOCASUGARYTOP_A   = filtered_df_MULTIBOCASUGARYTOP_A["Valor"].values
    ValorMULTIBOCAUNFILLED_A    = filtered_df_MULTIBOCAUNFILLED_A["Valor"].values
    ValorMULTILASCINTER_A       = filtered_df_MULTILASCINTER_A["Valor"].values
    ValorMULTILASCEXTER_A       = filtered_df_MULTILASCEXTER_A["Valor"].values
    ValorMULTILASCSUPER_A       = filtered_df_MULTILASCSUPER_A["Valor"].values
    # ValorMULTIBLACKDEFECT_A     = filtered_df_MULTIBLACKDEFECT_A["Valor"].values

    DATA_MULTIA = [[
        ValorMULTIIN_A, ValorMULTIREJ_A, ValorMULTIFUNDO_A, ValorMULTIFUNDOSTRESS_A,
        ValorMULTIBOCA_A, ValorMULTIBOCAWEM_A, ValorMULTIFUNDOCOMPACTO_A,
        ValorMULTIFUNDOBOLHA_A, ValorMULTIFINO_A, ValorMULTIFUNDODRAWN_A,
        ValorMULTIBOCALINEOVER_A, ValorMULTIBOCASCALY_A, ValorMULTIBOCAOVERPRESSED_A,
        ValorMULTIBOCASUGARYTOP_A, ValorMULTIBOCAUNFILLED_A,
        ValorMULTILASCINTER_A, ValorMULTILASCEXTER_A, ValorMULTILASCSUPER_A,
        # ValorMULTIBLACKDEFECT_A, -> Descomentar caso queira incluir
        line_number, 'MULTIA'
    ]]
    df_MULTIA = pd.DataFrame(DATA_MULTIA, columns=[
        'INSPECIONADOS', 'REJEICAO', 'FUNDO', 'FUNDOSTRESS', 'BOCA', 'BOCAWEM',
        'FUNDOCOMPACTO', 'FUNDOBOLHA', 'FINO', 'FUNDODRAWN', 'BOCALINEOVER',
        'BOCASCALY', 'BOCAOVERPRESSED', 'BOCASUGARYTOP', 'BOCAUNFILLED',
        'LASC_QUEBRA_INTER', 'LASC_QUEBRA_EXTER', 'LASC_QUEBRA_SUPER',
        # 'LASC_QUEBRA_BLACKDEFECT', -> Descomentar junto com ValorMULTIBLACKDEFECT_A
        'Line', 'Maq'
    ])
    df_meltedMULTIA = pd.melt(df_MULTIA, id_vars=['Line', 'Maq'],
                               value_vars=[
                                   'INSPECIONADOS', 'REJEICAO', 'FUNDO', 'FUNDOSTRESS',
                                   'BOCA', 'BOCAWEM', 'FUNDOCOMPACTO', 'FUNDOBOLHA', 'FINO',
                                   'FUNDODRAWN', 'BOCALINEOVER', 'BOCASCALY',
                                   'BOCAOVERPRESSED', 'BOCASUGARYTOP', 'BOCAUNFILLED',
                                   'LASC_QUEBRA_INTER', 'LASC_QUEBRA_EXTER',
                                   'LASC_QUEBRA_SUPER',
                                   # 'LASC_QUEBRA_BLACKDEFECT', -> Descomentar junto com ValorMULTIBLACKDEFECT_A
                               ])

    # ── MULTI B ──────────────────────────────────────────────────────────────
    filtered_df_MULTIIN_B              = df[df['Equacao'] == 'MULTI.IN.B'] # Inspecionados MULTI B
    filtered_df_MULTIREJ_B             = df[df['Equacao'] == 'MULTI.ERR.B'] # Rejeição MULTI B
    filtered_df_MULTIFUNDO_B           = df[df['Equacao'] == 'MULTI.DEF1.B'] # Fundo
    filtered_df_MULTIFUNDOSTRESS_B     = df[df['Equacao'] == 'MULTI.DEF2.B'] # Fundo Stress
    filtered_df_MULTIBOCA_B            = df[df['Equacao'] == 'MULTI.DEF3.B'] # Boca
    filtered_df_MULTIBOCAWEM_B         = df[df['Equacao'] == 'MULTI.DEF4.B'] # Fundo Compacto
    filtered_df_MULTIFUNDOCOMPACTO_B   = df[df['Equacao'] == 'MULTI.DEF5.B'] # Fundo Bolha
    filtered_df_MULTIFUNDOBOLHA_B      = df[df['Equacao'] == 'MULTI.DEF6.B'] # Fino
    filtered_df_MULTIFINO_B            = df[df['Equacao'] == 'MULTI.DEF7.B'] # Fundo Drawn
    filtered_df_MULTIFUNDODRAWN_B      = df[df['Equacao'] == 'MULTI.DEF8.B'] # Boca Line Over
    filtered_df_MULTIBOCALINEOVER_B    = df[df['Equacao'] == 'MULTI.DEF9.B'] # Boca Scaly
    filtered_df_MULTIBOCASCALY_B       = df[df['Equacao'] == 'MULTI.DEF10.B'] # Boca Overpressed
    filtered_df_MULTIBOCAOVERPRESSED_B = df[df['Equacao'] == 'MULTI.DEF11.B'] # Boca Sugary Top
    filtered_df_MULTIBOCASUGARYTOP_B   = df[df['Equacao'] == 'MULTI.DEF12.B'] # Boca Unfilled
    filtered_df_MULTIBOCAUNFILLED_B    = df[df['Equacao'] == 'MULTI.DEF13.B'] # Lasc Quebra Inter
    filtered_df_MULTILASCINTER_B       = df[df['Equacao'] == 'MULTI.DEF14.B'] # Lasc Quebra Exter
    filtered_df_MULTILASCEXTER_B       = df[df['Equacao'] == 'MULTI.DEF15.B'] # Lasc Quebra Super
    filtered_df_MULTILASCSUPER_B       = df[df['Equacao'] == 'MULTI.DEF16.B'] # Boca Wem
    # filtered_df_MULTIBLACKDEFECT_B     = df[df['Equacao'] == 'MULTI.DEF17.B'] # Black Defect -> Caso seja necessário, pode buscar. Precisa descomentar a linha 466 e 475 também.

    ValorMULTIIN_B              = filtered_df_MULTIIN_B["Valor"].values
    ValorMULTIREJ_B             = filtered_df_MULTIREJ_B["Valor"].values
    ValorMULTIFUNDO_B           = filtered_df_MULTIFUNDO_B["Valor"].values
    ValorMULTIFUNDOSTRESS_B     = filtered_df_MULTIFUNDOSTRESS_B["Valor"].values
    ValorMULTIBOCA_B            = filtered_df_MULTIBOCA_B["Valor"].values
    ValorMULTIBOCAWEM_B         = filtered_df_MULTIBOCAWEM_B["Valor"].values
    ValorMULTIFUNDOCOMPACTO_B   = filtered_df_MULTIFUNDOCOMPACTO_B["Valor"].values
    ValorMULTIFUNDOBOLHA_B      = filtered_df_MULTIFUNDOBOLHA_B["Valor"].values
    ValorMULTIFINO_B            = filtered_df_MULTIFINO_B["Valor"].values
    ValorMULTIFUNDODRAWN_B      = filtered_df_MULTIFUNDODRAWN_B["Valor"].values
    ValorMULTIBOCALINEOVER_B    = filtered_df_MULTIBOCALINEOVER_B["Valor"].values
    ValorMULTIBOCASCALY_B       = filtered_df_MULTIBOCASCALY_B["Valor"].values
    ValorMULTIBOCAOVERPRESSED_B = filtered_df_MULTIBOCAOVERPRESSED_B["Valor"].values
    ValorMULTIBOCASUGARYTOP_B   = filtered_df_MULTIBOCASUGARYTOP_B["Valor"].values
    ValorMULTIBOCAUNFILLED_B    = filtered_df_MULTIBOCAUNFILLED_B["Valor"].values
    ValorMULTILASCINTER_B       = filtered_df_MULTILASCINTER_B["Valor"].values
    ValorMULTILASCEXTER_B       = filtered_df_MULTILASCEXTER_B["Valor"].values
    ValorMULTILASCSUPER_B       = filtered_df_MULTILASCSUPER_B["Valor"].values
    # ValorMULTIBLACKDEFECT_B     = filtered_df_MULTIBLACKDEFECT_B["Valor"].values

    DATA_MULTIB = [[
        ValorMULTIIN_B, ValorMULTIREJ_B, ValorMULTIFUNDO_B, ValorMULTIFUNDOSTRESS_B,
        ValorMULTIBOCA_B, ValorMULTIBOCAWEM_B, ValorMULTIFUNDOCOMPACTO_B,
        ValorMULTIFUNDOBOLHA_B, ValorMULTIFINO_B, ValorMULTIFUNDODRAWN_B,
        ValorMULTIBOCALINEOVER_B, ValorMULTIBOCASCALY_B, ValorMULTIBOCAOVERPRESSED_B,
        ValorMULTIBOCASUGARYTOP_B, ValorMULTIBOCAUNFILLED_B,
        ValorMULTILASCINTER_B, ValorMULTILASCEXTER_B, ValorMULTILASCSUPER_B,
        # ValorMULTIBLACKDEFECT_B,
        line_number, 'MULTIB'
    ]]
    df_MULTIB = pd.DataFrame(DATA_MULTIB, columns=[
        'INSPECIONADOS', 'REJEICAO', 'FUNDO', 'FUNDOSTRESS', 'BOCA', 'BOCAWEM',
        'FUNDOCOMPACTO', 'FUNDOBOLHA', 'FINO', 'FUNDODRAWN', 'BOCALINEOVER',
        'BOCASCALY', 'BOCAOVERPRESSED', 'BOCASUGARYTOP', 'BOCAUNFILLED',
        'LASC_QUEBRA_INTER', 'LASC_QUEBRA_EXTER', 'LASC_QUEBRA_SUPER',
        # 'LASC_QUEBRA_BLACKDEFECT', -> Descomentar junto com ValorMULTIBLACKDEFECT_B
        'Line', 'Maq'
    ])
    df_meltedMULTIB = pd.melt(df_MULTIB, id_vars=['Line', 'Maq'],
                               value_vars=[
                                   'INSPECIONADOS', 'REJEICAO', 'FUNDO', 'FUNDOSTRESS',
                                   'BOCA', 'BOCAWEM', 'FUNDOCOMPACTO', 'FUNDOBOLHA', 'FINO',
                                   'FUNDODRAWN', 'BOCALINEOVER', 'BOCASCALY',
                                   'BOCAOVERPRESSED', 'BOCASUGARYTOP', 'BOCAUNFILLED',
                                   'LASC_QUEBRA_INTER', 'LASC_QUEBRA_EXTER',
                                   'LASC_QUEBRA_SUPER',
                                   # 'LASC_QUEBRA_BLACKDEFECT', -> Descomentar junto com ValorMULTIBLACKDEFECT_B
                               ])

    # ── MX4 ──────────────────────────────────────────────────────────────────
    _MX4_COLS = [
        'INSPECIONADOS', 'REJEITO', 'VERTICALIDADE', 'CALIBRE', 'PLUG',
        'PLANIDADE', 'OVALIZACAO', 'ESPESSURA1', 'ESPESSURA2', 'ESPESSURA3',
        'ESPESSURA4', 'TOPFINISHED', 'TRINCAHORIZONTAL', 'TRINCAVERTICAL',
        'TRINCAOMBRO', 'TRINCAFUNDO', 'LNM', 'Line', 'Maq'
    ]

    def _build_mx4(suffix, maq_label):
        eq = {
            'IN': f'CHK.IN.{suffix}',   'REJ': f'CHK.REJ.{suffix}',
            'V':  f'CHK.DEF1.{suffix}', 'CAL': f'CHK.DEF2.{suffix}',
            'PLG':f'CHK.DEF3.{suffix}', 'PLA': f'CHK.DEF4.{suffix}',
            'OVA':f'CHK.DEF5.{suffix}', 'E1':  f'CHK.DEF6.{suffix}',
            'E2': f'CHK.DEF7.{suffix}', 'E3':  f'CHK.DEF8.{suffix}',
            'E4': f'CHK.DEF9.{suffix}', 'TFC': f'CHK.DEF10.{suffix}',
            'TH': f'CHK.DEF11.{suffix}','TV':  f'CHK.DEF12.{suffix}',
            'TO': f'CHK.DEF13.{suffix}','TF':  f'CHK.DEF14.{suffix}',
            'LNM':f'CHK.MNRR.{suffix}'
        }
        vals = {k: df[df['Equacao'] == v]["Valor"].values for k, v in eq.items()}
        data = [[
            vals['IN'], vals['REJ'], vals['V'], vals['CAL'], vals['PLG'],
            vals['PLA'], vals['OVA'], vals['E1'], vals['E2'], vals['E3'],
            vals['E4'], vals['TFC'], vals['TH'], vals['TV'], vals['TO'],
            vals['TF'], vals['LNM'], line_number, maq_label
        ]]
        df_mx4 = pd.DataFrame(data, columns=_MX4_COLS)
        return pd.melt(df_mx4, id_vars=['Line', 'Maq'],
                       value_vars=_MX4_COLS[:-2])

    df_meltedMX4A = _build_mx4('A', 'MX4A')
    df_meltedMX4B = _build_mx4('B', 'MX4B')
    df_meltedMX4C = _build_mx4('C', 'MX4C')

    return (df_meltedMX4C, df_meltedMX4B, df_meltedMX4A,
            df_meltedMULTIB, df_meltedMULTIA,
            df_meltedMCALB, df_meltedMCALA,
            df_meltedLEHR, df_meltedXPAR, df_meltedIS)

# ─────────────────────────────────────────────────────────────────────────────
# INSERÇÃO NO BANCO — em lote com executemany para melhor performance
# ─────────────────────────────────────────────────────────────────────────────
def inserir_dados(dataframes, cursor, conn, table_name):
    for dataframe in dataframes:
        registros = []
        for row in dataframe.itertuples(index=False):
            valor = float(row.value[0]) if hasattr(row.value, '__len__') and len(row.value) > 0 else 0.0
            registros.append((row.Line, row.Maq, row.variable, valor))
        if registros:
            cursor.executemany(
                f"INSERT INTO {table_name} (Line, Maq, variable, value) VALUES (?, ?, ?, ?)",
                registros
            )
            conn.commit()
    logger.info(f"Dados inseridos em {table_name} com sucesso.")

# ─────────────────────────────────────────────────────────────────────────────
# CONSULTA DE UMA LINHA  (API → processamento → SQL)
# ─────────────────────────────────────────────────────────────────────────────
def processar_linha(line_number, from_dt, until_dt):
    logger.info(f">>> Iniciando processamento da linha L{line_number}")
    try:
        table_name    = TABELA_POR_LINHA[line_number]
        response_json = APIJSON(line_number, from_dt, until_dt)
        dataframes    = processar_dados(response_json, line_number)
        cursor, conn  = SQL()
        inserir_dados(dataframes, cursor, conn, table_name)
        conn.close()
        logger.info(f"<<< Linha L{line_number} concluída → tabela {table_name}.")
    except Exception as e:
        logger.error(f"Erro ao processar L{line_number}: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# TAREFA AGENDADA  — roda toda hora no minuto :00
# ─────────────────────────────────────────────────────────────────────────────
def coletar_todas_as_linhas():
    agora     = datetime.datetime.now()
    # Janela = hora anterior completa  (ex.: 07:00:00 → 07:59:59)
    hora_ant  = agora.replace(minute=0, second=0, microsecond=0) - datetime.timedelta(hours=1)
    from_dt   = hora_ant
    until_dt  = hora_ant + datetime.timedelta(hours=1) - datetime.timedelta(seconds=1)

    logger.info(
        f"=== Ciclo iniciado às {agora.strftime('%H:%M:%S')} | "
        f"Janela: {from_dt.strftime('%Y-%m-%dT%H:%M:%S')} → "
        f"{until_dt.strftime('%Y-%m-%dT%H:%M:%S')} ==="
    )

    for i, linha in enumerate(LINHAS):
        processar_linha(linha, from_dt, until_dt)

        # Aguarda 30 s antes da próxima linha (exceto após a última)
        if i < len(LINHAS) - 1:
            logger.info(f"Aguardando {INTERVALO_ENTRE_LINHAS}s antes da próxima linha...")
            time.sleep(INTERVALO_ENTRE_LINHAS)

    logger.info("=== Ciclo finalizado. ===")

# ─────────────────────────────────────────────────────────────────────────────
# AGENDAMENTO
# ─────────────────────────────────────────────────────────────────────────────
schedule.every().hour.at(":00").do(coletar_todas_as_linhas)

logger.info("Script iniciado. Aguardando próximo ciclo no minuto :00...")

while True:
    schedule.run_pending()
    time.sleep(1)