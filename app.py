"""
app.py
------
Interface Streamlit para o Pipeline ETL PNAD Contínua + Chat Analítico com IA.

Abas:
  📊 Pipeline ETL  — Extração, transformação e carga Bronze→Silver→Gold
  🤖 Analista IA   — Consulta analítica em linguagem natural via OpenAI + FastMCP
"""

import asyncio
import json
import os
from concurrent.futures import ThreadPoolExecutor

import streamlit as st
from dotenv import load_dotenv
from fastmcp import Client
from openai import OpenAI

from src.extract.ibge_extractor import IBGEExtractor
from src.load.mongo_loader import MongoLoader
from src.load.sqlite_loader import SQLiteLoader
from src.mcp.mcp_server import mcp
from src.transform.pnad_transformer import PNADTransformer

load_dotenv()

# ------------------------------------------------------------------
# Configuração da página
# ------------------------------------------------------------------

st.set_page_config(
    page_title="PNAD Contínua — IBGE",
    page_icon="📊",
    layout="centered",
)

# ------------------------------------------------------------------
# Parâmetros fixos do ETL
# ------------------------------------------------------------------

AGREGADO  = "4093"
VARIAVEIS = "4096|4099|12466"
PERIODOS  = "all"

COMBINACOES = [
    {"localidades": "N1[all]", "classificacao": "2[6794]", "label": "Brasil — Total"},
    {"localidades": "N1[all]", "classificacao": "2[4]",    "label": "Brasil — Homens"},
    {"localidades": "N1[all]", "classificacao": "2[5]",    "label": "Brasil — Mulheres"},
    {"localidades": "N3[all]", "classificacao": "2[6794]", "label": "UFs — Total"},
    {"localidades": "N3[all]", "classificacao": "2[4]",    "label": "UFs — Homens"},
    {"localidades": "N3[all]", "classificacao": "2[5]",    "label": "UFs — Mulheres"},
]

# ------------------------------------------------------------------
# Helpers async → sync para o Chat
# ------------------------------------------------------------------

def run_async(coro):
    with ThreadPoolExecutor() as pool:
        return pool.submit(asyncio.run, coro).result()

async def _list_tools():
    async with Client(mcp) as client:
        return await client.list_tools()

async def _call_tool(name: str, args: dict):
    async with Client(mcp) as client:
        return await client.call_tool(name, args)

def to_openai_tool(t) -> dict:
    return {
        "type": "function",
        "function": {
            "name": t.name,
            "description": t.description or "",
            "parameters": t.inputSchema,
        },
    }

# ------------------------------------------------------------------
# System prompt analítico
# ------------------------------------------------------------------

SYSTEM_PROMPT = """Você é um analista sênior de mercado de trabalho brasileiro, especializado
na PNAD Contínua do IBGE. Seu perfil é acadêmico e técnico: você interpreta dados com rigor,
contextualiza resultados e aponta implicações relevantes para pesquisa e política pública.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SOBRE OS DADOS DISPONÍVEIS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Fonte: PNAD Contínua — Tabela 4093 (IBGE)
Cobertura temporal: 2012T1 até o trimestre mais recente
Cobertura geográfica: Brasil + 27 Unidades Federativas
Desagregação por sexo: Total, Homens, Mulheres

Variáveis disponíveis:
- Taxa de desocupação (%): proporção de desocupados na força de trabalho
- Taxa de subutilização (%): desocupados + subocupados + força de trabalho potencial
- Pessoas na força de trabalho (mil pessoas)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
REGRAS PARA USAR AS TOOLS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

FORMATO DE PERÍODO — CRÍTICO:
- Use SEMPRE o formato ANOTtrimestre: '2018T1', '2018T2', '2018T3', '2018T4'
- NUNCA passe apenas o ano ('2018') como período — a tool não reconhece
- Quando o usuário mencionar apenas o ano (ex: "em 2018"):
    → Chame a tool para CADA trimestre: T1, T2, T3 e T4 separadamente
    → Depois consolide e compare os resultados na resposta
- Quando pedir "pior trimestre" ou "melhor trimestre" de um ano:
    → Busque os 4 trimestres e identifique o maior/menor valor

NOMES DE ESTADOS:
- Use SEMPRE o nome completo: 'Pernambuco', não 'PE'; 'São Paulo', não 'SP'
- Para o agregado nacional use: 'Brasil'

SEXO — valores aceitos exatamente:
- 'Total' (homens + mulheres)
- 'Homens'
- 'Mulheres'

QUANDO NÃO ENCONTRAR DADOS:
- NUNCA declare ausência de dados sem antes fazer ao menos uma chamada real à tool
- Se retornar vazio, tente variações: ajuste o formato do período, verifique o nome do estado
- Só informe ausência após tentativas reais

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
COMO RESPONDER — PADRÃO ANALÍTICO
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Para perguntas simples (um indicador, um período):
- Apresente o valor com unidade e período
- Adicione uma frase de contexto (ex: acima/abaixo da média nacional)

Para perguntas comparativas (entre estados, sexos, períodos):
- Use tabela ou lista numerada com os valores lado a lado
- Calcule e destaque a diferença absoluta e relativa quando relevante
- Aponte qual grupo está em situação mais vulnerável e por quê isso importa

Para perguntas de tendência (evolução ao longo do tempo):
- Organize cronologicamente
- Identifique pontos de inflexão (ex: impacto da pandemia em 2020T2)
- Mencione fatores estruturais conhecidos quando pertinente (reformas, crises, sazonalidade)

Para rankings:
- Apresente tabela com posição, UF e valor
- Compare com a média nacional
- Aponte padrões regionais (Norte/Nordeste vs Sul/Sudeste)

Sempre termine respostas analíticas com uma frase de síntese ou implicação relevante.
Responda sempre em português. Use linguagem técnica mas acessível.
"""

# ------------------------------------------------------------------
# Sugestões organizadas por categoria analítica
# ------------------------------------------------------------------

SUGESTOES = {
    "📍 Ponto no tempo": [
        "Qual era a taxa de desocupação no Brasil no 2T2020 (auge da pandemia)?",
        "Quais os 5 estados com maior desocupação no 4T2023?",
        "Compare desocupação entre Nordeste e Sul no 1T2019",
    ],
    "📈 Tendência temporal": [
        "Como evoluiu a desocupação no Brasil de 2019T1 a 2021T4?",
        "A desocupação em Pernambuco melhorou entre 2020 e 2023?",
        "Qual foi o pior trimestre para o mercado de trabalho brasileiro?",
    ],
    "⚧ Desigualdade de gênero": [
        "Compare a desocupação entre homens e mulheres no Brasil em 2023T4",
        "A brecha de gênero na desocupação aumentou ou diminuiu entre 2018 e 2023?",
        "Em quais estados as mulheres têm maior desvantagem no mercado de trabalho?",
    ],
    "🗺️ Disparidades regionais": [
        "Quais estados têm desocupação estruturalmente acima da média nacional?",
        "Compare a taxa de subutilização entre Maranhão e Santa Catarina",
        "Qual a diferença de desocupação entre o estado mais alto e mais baixo do ranking?",
    ],
}

# ------------------------------------------------------------------
# Função de chat com loop de tool calls
# ------------------------------------------------------------------

def chat(historico: list, pergunta: str) -> str:
    mcp_tools    = run_async(_list_tools())
    openai_tools = [to_openai_tool(t) for t in mcp_tools]

    messages = (
        [{"role": "system", "content": SYSTEM_PROMPT}]
        + historico
        + [{"role": "user", "content": pergunta}]
    )

    while True:
        response = OpenAI(api_key=os.getenv("OPENAI_API_KEY")).chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            tools=openai_tools,
            tool_choice="auto",
        )
        msg = response.choices[0].message
        messages.append(msg)

        if not msg.tool_calls:
            return msg.content

        for tc in msg.tool_calls:
            resultado = run_async(
                _call_tool(tc.function.name, json.loads(tc.function.arguments))
            )
            if hasattr(resultado, "content") and resultado.content:
                content = resultado.content[0].text
            elif hasattr(resultado, "__iter__"):
                items = list(resultado)
                content = items[0].text if items else "sem resultado"
            else:
                content = str(resultado)
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": content,
            })

# ------------------------------------------------------------------
# Abas principais
# ------------------------------------------------------------------

aba_etl, aba_chat = st.tabs(["📊 Pipeline ETL", "🤖 Analista IA"])

# ══════════════════════════════════════════════════════════════════
# ABA 1 — Pipeline ETL
# ══════════════════════════════════════════════════════════════════

with aba_etl:
    st.title("📊 Pipeline ETL — PNAD Contínua (IBGE)")
    st.caption("Arquitetura Medalhão: Bronze → Silver (MongoDB Atlas) → Gold (SQLite)")
    st.write("---")

    st.info(
        "Clique no botão abaixo para carregar **todos os dados disponíveis** da PNAD Contínua "
        f"(tabela {AGREGADO}) nas camadas Bronze, Silver e Gold."
    )
    st.write("---")

    if st.button("🚀 Carregar Todos os Dados", type="primary", use_container_width=True):
        with st.status("Iniciando pipeline...", expanded=True) as status:
            try:
                extractor     = IBGEExtractor()
                transformer   = PNADTransformer()
                mongo_loader  = MongoLoader()
                sqlite_loader = SQLiteLoader()

                raw_data_completo = []
                for i, c in enumerate(COMBINACOES, start=1):
                    st.write(f"🌐 [{i}/{len(COMBINACOES)}] Extraindo **{c['label']}**...")
                    raw = extractor.extract_pnad(
                        agregado=AGREGADO,
                        periodos=PERIODOS,
                        variaveis=VARIAVEIS,
                        localidades=c["localidades"],
                        classificacao=c["classificacao"],
                    )
                    raw_data_completo.extend(raw)
                    st.write(f"   ✅ {len(raw)} bloco(s) retornado(s).")

                st.write(f"📦 Total extraído: **{len(raw_data_completo)}** blocos.")

                st.write("🟫 Gravando camada **Bronze**...")
                bronze_count = mongo_loader.load_bronze(raw_data_completo)
                st.write(f"✅ Bronze: **{bronze_count}** documentos inseridos.")

                st.write("⚙️ Transformando Bronze → Silver...")
                documents = transformer.transform_to_documents(raw_data_completo)
                st.write(f"✅ **{len(documents)}** documentos estruturados.")

                st.write("🥈 Gravando camada **Silver** (upsert idempotente)...")
                silver_count = mongo_loader.load_documents(documents)
                st.write(f"✅ Silver: **{silver_count}** documentos gravados/atualizados.")

                st.write("🥇 Gravando camada **Gold** (SQLite)...")
                gold_count = sqlite_loader.load_documents(documents)
                st.write(f"✅ Gold: **{gold_count}** linhas gravadas.")

                status.update(label="🚀 Pipeline concluído com sucesso!", state="complete")
                st.write("---")
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Chamadas API",     len(COMBINACOES))
                m2.metric("Blocos Extraídos", len(raw_data_completo))
                m3.metric("Docs Silver",      len(documents))
                m4.metric("Linhas Gold",      gold_count)

            except Exception as exc:
                status.update(label="❌ Falha na execução do Pipeline", state="error")
                st.error(f"Erro detectado: {exc}")
                st.exception(exc)

# ══════════════════════════════════════════════════════════════════
# ABA 2 — Analista IA
# ══════════════════════════════════════════════════════════════════

with aba_chat:
    st.title("🤖 Analista PNAD Contínua")
    st.caption(
        "Análise do mercado de trabalho brasileiro em linguagem natural · "
        "gpt-4o-mini + FastMCP · Dados: IBGE 2012–presente"
    )
    st.write("---")

    if not os.getenv("OPENAI_API_KEY"):
        st.error("⚠️ Variável **OPENAI_API_KEY** não encontrada. Adicione-a no arquivo `.env`.")
        st.stop()

    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Sugestões por categoria (só aparecem sem histórico)
    if not st.session_state.messages:
        for categoria, perguntas in SUGESTOES.items():
            st.markdown(f"**{categoria}**")
            cols = st.columns(len(perguntas))
            for i, pergunta in enumerate(perguntas):
                if cols[i].button(pergunta, key=f"sug_{categoria}_{i}", use_container_width=True):
                    st.session_state["sugestao"] = pergunta
                    st.rerun()
        st.write("---")

    prompt = (
        st.chat_input("Faça sua pergunta sobre mercado de trabalho, desocupação, gênero, regiões...")
        or st.session_state.pop("sugestao", None)
    )

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    if prompt:
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Consultando e analisando dados..."):
                try:
                    resposta = chat(st.session_state.messages, prompt)
                    st.markdown(resposta)
                    st.session_state.messages.append({"role": "user",      "content": prompt})
                    st.session_state.messages.append({"role": "assistant", "content": resposta})
                except Exception as e:
                    st.error(f"Erro: {e}")
                    st.exception(e)

    if st.session_state.messages:
        st.write("---")
        if st.button("🗑️ Limpar conversa", use_container_width=False):
            st.session_state.messages = []
            st.rerun()