"""
pipeline.py
-----------
Orquestrador principal do ETL PNAD Contínua usando Prefect 3.

Fluxo:
    task_extract  →  task_load_bronze  →  task_transform  →  task_load_silver  →  task_load_gold

Camadas (Arquitetura Medalhão):
    Bronze — JSON bruto da API, imutável (MongoDB Atlas)
    Silver — Documentos normalizados, tipados, sem nulos (MongoDB Atlas)
    Gold   — Tabela fato relacional pré-agregada (SQLite local)

Execução manual:
    python pipeline.py

Deployment agendado (trimestral):
    python pipeline.py --deploy
"""

import logging
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
from prefect import flow, get_run_logger, task
from prefect.schedules import CronSchedule

from src.extract.ibge_extractor import IBGEExtractor
from src.load.mongo_loader import MongoLoader
from src.load.sqlite_loader import SQLiteLoader
from src.transform.pnad_transformer import PNADTransformer

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# ------------------------------------------------------------------
# Parâmetros padrão do pipeline (configuráveis por deployment)
# ------------------------------------------------------------------

DEFAULT_PARAMS = {
    "agregado": "4099",             # 4099 = Taxa de desocupação
    "periodos": "-4",               # Últimos 4 trimestres disponíveis
    "variaveis": "4090",            # 4090 = Taxa de desocupação (variável específica)
    "localidades": "N3[all]",       # Todas as UFs (nível estado = N3)
    "classificacao": "2[0]|86[0]",  # Sexo Total + Raça Total (evita erro 500)
}

# ------------------------------------------------------------------
# Tasks do Prefect
# ------------------------------------------------------------------

@task(
    retries=3,
    retry_delay_seconds=10,
    name="Extração IBGE",
    description="Consome a API de Agregados v3 do IBGE com retry exponencial.",
    tags=["bronze", "extract"],
)
def task_extract(
    agregado: str,
    periodos: str,
    variaveis: str,
    localidades: str,
    classificacao: str,
) -> list:
    logger = get_run_logger()
    logger.info("Iniciando extração. Agregado: %s | Períodos: %s", agregado, periodos)

    with IBGEExtractor() as extractor:
        raw_data = extractor.extract_pnad(
            agregado=agregado,
            periodos=periodos,
            variaveis=variaveis,
            localidades=localidades,
            classificacao=classificacao,
        )

    logger.info("Extração concluída: %d bloco(s) retornado(s).", len(raw_data))
    return raw_data


@task(
    name="Carga Bronze (MongoDB)",
    description="Persiste o JSON bruto na camada Bronze do MongoDB Atlas.",
    tags=["bronze", "load"],
)
def task_load_bronze(raw_data: list) -> int:
    logger = get_run_logger()
    logger.info("Iniciando carga Bronze (%d blocos)...", len(raw_data))

    with MongoLoader() as loader:
        count = loader.load_bronze(raw_data)

    logger.info("Bronze: %d documentos inseridos.", count)
    return count


@task(
    name="Transformação e Limpeza (Silver)",
    description="Normaliza, tipia e enriquece os dados brutos em documentos Silver.",
    tags=["silver", "transform"],
)
def task_transform(raw_data: list) -> list:
    logger = get_run_logger()
    logger.info("Iniciando transformação Bronze → Silver...")

    transformer = PNADTransformer()
    documents = transformer.transform_to_documents(raw_data)

    logger.info("Transformação concluída: %d documentos gerados.", len(documents))
    return documents


@task(
    name="Carga Silver (MongoDB Atlas)",
    description="Persiste documentos curados com upsert idempotente no MongoDB.",
    tags=["silver", "load"],
)
def task_load_silver(documents: list) -> int:
    logger = get_run_logger()
    logger.info("Iniciando carga Silver (%d documentos)...", len(documents))

    with MongoLoader() as loader:
        count = loader.load_documents(documents)

    logger.info("Silver: %d documentos gravados/atualizados.", count)
    return count


@task(
    name="Carga Gold (SQLite)",
    description="Persiste dados agregados na camada Gold relacional (SQLite).",
    tags=["gold", "load"],
)
def task_load_gold(documents: list) -> int:
    logger = get_run_logger()
    logger.info("Iniciando carga Gold (%d documentos disponíveis)...", len(documents))

    loader = SQLiteLoader()
    count = loader.load_documents(documents)

    logger.info("Gold: %d linhas gravadas.", count)
    return count


# ------------------------------------------------------------------
# Flow principal
# ------------------------------------------------------------------

@flow(
    name="ETL_PNAD_Continua",
    description=(
        "Pipeline ETL completo da PNAD Contínua — IBGE. "
        "Arquitetura Medalhão: Bronze → Silver (MongoDB Atlas) → Gold (SQLite)."
    ),
    log_prints=True,
)
def run_etl_pipeline(
    agregado: str = DEFAULT_PARAMS["agregado"],
    periodos: str = DEFAULT_PARAMS["periodos"],
    variaveis: str = DEFAULT_PARAMS["variaveis"],
    localidades: str = DEFAULT_PARAMS["localidades"],
    classificacao: str = DEFAULT_PARAMS["classificacao"],
):
    inicio = datetime.now(timezone.utc)
    print(f"[{inicio.isoformat()}] Pipeline ETL PNAD iniciado.")
    print(f"Parâmetros: agregado={agregado}, períodos={periodos}, localidades={localidades}")

    # ── Extração ──────────────────────────────────────────────────────
    raw_data = task_extract(
        agregado=agregado,
        periodos=periodos,
        variaveis=variaveis,
        localidades=localidades,
        classificacao=classificacao,
    )
    print(f"Extraídos: {len(raw_data)} bloco(s) de metadados.")

    # ── Carga Bronze ──────────────────────────────────────────────────
    bronze_count = task_load_bronze(raw_data)
    print(f"Bronze: {bronze_count} documentos brutos persistidos.")

    # ── Transformação Silver ──────────────────────────────────────────
    silver_docs = task_transform(raw_data)
    print(f"Silver: {len(silver_docs)} documentos estruturados gerados.")

    # ── Carga Silver ──────────────────────────────────────────────────
    silver_count = task_load_silver(silver_docs)
    print(f"Silver: {silver_count} documentos gravados/atualizados no MongoDB.")

    # ── Carga Gold ────────────────────────────────────────────────────
    gold_count = task_load_gold(silver_docs)
    print(f"Gold: {gold_count} linhas gravadas no SQLite.")

    # ── Resumo ────────────────────────────────────────────────────────
    fim = datetime.now(timezone.utc)
    duracao = (fim - inicio).total_seconds()
    print(
        f"\n{'='*50}\n"
        f"Pipeline finalizado em {duracao:.1f}s.\n"
        f"  Bronze inserido : {bronze_count} docs\n"
        f"  Silver upserted : {silver_count} docs\n"
        f"  Gold gravado    : {gold_count} linhas\n"
        f"{'='*50}"
    )

    return {
        "bronze": bronze_count,
        "silver": silver_count,
        "gold": gold_count,
        "duracao_segundos": round(duracao, 1),
    }


# ------------------------------------------------------------------
# Deployment agendado (execução com: python pipeline.py --deploy)
# ------------------------------------------------------------------

def deploy_scheduled():
    run_etl_pipeline.serve(
        name="agendamento-trimestral-pnad",
        schedule=CronSchedule(cron="0 0 1 1,4,7,10 *"),
        parameters=DEFAULT_PARAMS,
        tags=["pnad", "ibge", "trimestral"],
    )


# ------------------------------------------------------------------
# Ponto de entrada
# ------------------------------------------------------------------

if __name__ == "__main__":
    if "--deploy" in sys.argv:
        print("Registrando deployment trimestral no Prefect...")
        deploy_scheduled()
    else:
        run_etl_pipeline()