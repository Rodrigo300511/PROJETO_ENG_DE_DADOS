"""
mcp_server.py
-------------
Servidor MCP (Model Context Protocol) que expõe os indicadores curados
da PNAD Contínua como tools consultáveis por um cliente de IA (ex: Claude).

Tools disponíveis:
1. consultar_pnad         — Consulta parametrizada (variável, UF, sexo, período)
2. listar_periodos        — Lista todos os períodos disponíveis
3. listar_ufs             — Lista todos os estados com dados
4. top_desocupacao        — Ranking de estados por taxa de desocupação
5. comparar_sexo          — Compara indicador entre Homens/Mulheres por período

Uso:
    python src/mcp/mcp_server.py

    Ou configure no Claude Desktop em claude_desktop_config.json:
    {
      "mcpServers": {
        "pnad-indicadores": {
          "command": "python",
          "args": ["caminho/para/src/mcp/mcp_server.py"]
        }
      }
    }
"""

import logging
import os
import re

from dotenv import load_dotenv
from fastmcp import FastMCP
from pymongo import MongoClient
from pymongo.server_api import ServerApi

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Inicialização do servidor MCP e conexão com MongoDB
# ------------------------------------------------------------------

mcp = FastMCP("pnad-indicadores")

_uri = os.getenv("MONGO_URI") or (
    f"mongodb+srv://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    "@cluster0.wmnutcj.mongodb.net/?appName=Cluster0"
)

_client = MongoClient(_uri, server_api=ServerApi("1"), serverSelectionTimeoutMS=10_000)
_db = _client[os.getenv("MONGO_DB_NAME", "ibge_pnad")]
_col = _db[os.getenv("MONGO_SILVER_COLLECTION", "silver_pnad_docs")]

_MAX_RESULTS = 50  # Limite de segurança para não estourar o contexto da IA

# ------------------------------------------------------------------
# Tool 1 — Consulta parametrizada principal
# ------------------------------------------------------------------

@mcp.tool()
def consultar_pnad(
    variavel: str = None,
    uf: str = None,
    sexo: str = None,
    periodo: str = None,
) -> list:
    """
    Consulta indicadores da PNAD Contínua no banco de dados consolidado.

    Args:
        variavel: Nome ou fragmento do nome da variável (ex: 'Taxa de desocupação')
        uf:       Nome do estado (ex: 'Pernambuco', 'São Paulo') ou 'Brasil'
        sexo:     'Homens', 'Mulheres' ou 'Total'
        periodo:  Trimestre no formato ANOTtrimestre (ex: '2023T1' para 1º tri de 2023)
                  Também aceita o formato bruto IBGE '202301'

    Returns:
        Lista de indicadores correspondentes aos filtros (máx. 50 resultados).

    Exemplos de uso pelo Claude:
        - "Qual a taxa de desocupação das mulheres em Pernambuco no 1T2023?"
          → consultar_pnad(variavel="desocupação", uf="Pernambuco", sexo="Mulheres", periodo="2023T1")
        - "Mostre a desocupação total no Brasil em todos os períodos disponíveis"
          → consultar_pnad(variavel="desocupação", uf="Brasil", sexo="Total")
    """
    query: dict = {}

    if variavel:
        query["variavel"] = {"$regex": variavel, "$options": "i"}
    if uf:
        query["uf"] = {"$regex": f"^{uf}$", "$options": "i"}
    if sexo:
        query["sexo"] = {"$regex": f"^{sexo}$", "$options": "i"}
    if periodo:
        if re.match(r'^\d{4}$', periodo):
            # Apenas o ano: "2023" → todos os trimestres
            query["periodo"] = {"$regex": f"^{periodo}T", "$options": "i"}
        else:
            # Normaliza "2023T1" → regex que aceita "2023T1" e "2023T01"
            m = re.match(r'^(\d{4})T0?(\d+)$', periodo)
            if m:
                ano, tri = m.group(1), int(m.group(2))
                query["$or"] = [
                    {"periodo": {"$regex": f"^{ano}T0?{tri}$", "$options": "i"}},
                    {"periodo_raw": f"{ano}{tri:02d}"},
                ]
            else:
                # Formato raw "202301"
                query["$or"] = [
                    {"periodo_raw": periodo},
                    {"periodo": {"$regex": periodo, "$options": "i"}},
                ]

    projection = {"_id": 0, "chave_unica": 0, "data_carga": 0, "camada": 0, "fonte": 0}
    resultados = list(_col.find(query, projection).limit(_MAX_RESULTS))

    if not resultados:
        return [{"mensagem": "Nenhum dado encontrado para os filtros informados."}]

    return resultados


# ------------------------------------------------------------------
# Tool 2 — Listar períodos disponíveis
# ------------------------------------------------------------------

@mcp.tool()
def listar_periodos() -> list:
    """
    Retorna todos os períodos (trimestres) disponíveis no banco de dados.

    Útil para descobrir qual é o período mais recente disponível.
    """
    periodos_raw = sorted(_col.distinct("periodo"))
    # Normaliza formato armazenado "2023T01" → "2023T1" para o modelo
    periodos = [re.sub(r'T0+(\d+)$', r'T\1', p) for p in periodos_raw]
    return [
        {"chave": "periodos_disponiveis", "valor": periodos},
        {"chave": "mais_recente",         "valor": periodos[-1] if periodos else None},
        {"chave": "mais_antigo",          "valor": periodos[0]  if periodos else None},
        {"chave": "total_periodos",       "valor": len(periodos)},
    ]


# ------------------------------------------------------------------
# Tool 3 — Listar UFs com dados
# ------------------------------------------------------------------

@mcp.tool()
def listar_ufs() -> list:
    """
    Retorna todos os estados (UFs) com dados disponíveis no banco.
    """
    pipeline = [
        {"$group": {"_id": "$uf", "codigo": {"$first": "$uf_codigo"}}},
        {"$sort": {"_id": 1}},
    ]
    return [{"uf": r["_id"], "codigo": r["codigo"]} for r in _col.aggregate(pipeline)]


# ------------------------------------------------------------------
# Tool 4 — Ranking de desocupação
# ------------------------------------------------------------------

@mcp.tool()
def top_desocupacao(periodo: str, sexo: str = "Total", top_n: int = 5) -> list:
    """
    Retorna o ranking dos estados com maior taxa de desocupação em um período.

    Args:
        periodo: Trimestre no formato '2023T1'
        sexo:    'Total', 'Homens' ou 'Mulheres' (padrão: Total)
        top_n:   Quantos estados retornar (padrão: 5, máx: 27)

    Returns:
        Lista ordenada do maior para o menor, com posição no ranking.

    Exemplo: "Quais os 5 estados com maior desocupação feminina em 2023T4?"
        → top_desocupacao(periodo="2023T4", sexo="Mulheres", top_n=5)
    """
    top_n = min(max(top_n, 1), 27)

    match_stage = {
        "variavel": {"$regex": "desocupa", "$options": "i"},
        "sexo": {"$regex": f"^{sexo}$", "$options": "i"},
        "valor_disponivel": True,
        "nivel_territorial": {"$regex": "Unidade|Estado|UF", "$options": "i"},
    }
    # Aplica filtro de período normalizado (aceita "2023T1", "2023T01", "202301")
    m = re.match(r'^(\d{4})T0?(\d+)$', periodo)
    if m:
        ano, tri = m.group(1), int(m.group(2))
        match_stage["$or"] = [
            {"periodo": {"$regex": f"^{ano}T0?{tri}$", "$options": "i"}},
            {"periodo_raw": f"{ano}{tri:02d}"},
        ]
    else:
        match_stage["periodo"] = {"$regex": periodo, "$options": "i"}

    pipeline = [
        {"$match": match_stage},
        {"$sort": {"valor": -1}},
        {"$limit": top_n},
        {
            "$project": {
                "_id": 0,
                "posicao": {"$literal": None},
                "uf": 1,
                "periodo": 1,
                "sexo": 1,
                "valor": 1,
                "unidade": 1,
            }
        },
    ]

    resultados = list(_col.aggregate(pipeline))
    for i, r in enumerate(resultados, start=1):
        r["posicao"] = i

    if not resultados:
        return [{"mensagem": f"Sem dados de desocupação para {periodo} / {sexo}."}]

    return resultados


# ------------------------------------------------------------------
# Tool 5 — Comparação por sexo
# ------------------------------------------------------------------

@mcp.tool()
def comparar_sexo(variavel: str, uf: str, periodo: str) -> list:
    """
    Compara um indicador entre Homens, Mulheres e Total para uma UF e período.

    Args:
        variavel: Fragmento do nome da variável (ex: 'desocupação')
        uf:       Nome do estado (ex: 'Pernambuco') ou 'Brasil'
        periodo:  Trimestre no formato '2023T1'

    Returns:
        Lista com os valores por sexo e a diferença (brecha de gênero).

    Exemplo: "Compare a desocupação por sexo em Pernambuco no 2T2024"
        → comparar_sexo(variavel="desocupação", uf="Pernambuco", periodo="2024T2")
    """
    filtro_periodo: dict = {}
    mp = re.match(r'^(\d{4})T0?(\d+)$', periodo)
    if mp:
        ano, tri = mp.group(1), int(mp.group(2))
        filtro_periodo["$or"] = [
            {"periodo": {"$regex": f"^{ano}T0?{tri}$", "$options": "i"}},
            {"periodo_raw": f"{ano}{tri:02d}"},
        ]
    else:
        filtro_periodo["periodo"] = {"$regex": periodo, "$options": "i"}

    resultados = list(
        _col.find(
            {
                "variavel": {"$regex": variavel, "$options": "i"},
                "uf":       {"$regex": f"^{uf}$", "$options": "i"},
                "valor_disponivel": True,
                **filtro_periodo,
            },
            {"_id": 0, "sexo": 1, "valor": 1, "unidade": 1, "variavel": 1, "periodo": 1},
        )
    )

    if not resultados:
        return [{"mensagem": f"Sem dados para {variavel} em {uf} no período {periodo}."}]

    por_sexo = {r["sexo"]: r["valor"] for r in resultados}
    unidade = resultados[0].get("unidade", "%")
    variavel_nome = resultados[0].get("variavel", variavel)

    brecha = None
    if "Mulheres" in por_sexo and "Homens" in por_sexo:
        brecha = round(por_sexo["Mulheres"] - por_sexo["Homens"], 2)

    interpretacao = (
        f"{'Mulheres' if brecha and brecha > 0 else 'Homens'} têm taxa "
        f"{abs(brecha) if brecha else 0}{unidade} "
        f"{'maior' if brecha and brecha > 0 else 'menor'} que o sexo oposto."
        if brecha is not None
        else "Comparação por sexo indisponível para este filtro."
    )

    return [
        {"chave": "variavel",         "valor": variavel_nome},
        {"chave": "uf",               "valor": uf},
        {"chave": "periodo",          "valor": periodo},
        {"chave": "unidade",          "valor": unidade},
        {"chave": "valores_por_sexo", "valor": por_sexo},
        {"chave": "brecha_genero_pp", "valor": brecha},
        {"chave": "interpretacao",    "valor": interpretacao},
    ]


# ------------------------------------------------------------------
# Ponto de entrada
# ------------------------------------------------------------------

if __name__ == "__main__":
    logger.info("Iniciando servidor MCP 'pnad-indicadores'...")
    mcp.run()