"""
sqlite_loader.py
----------------
Loader para a camada Gold no SQLite local.

A camada Gold contém dados pré-agregados e prontos para análise,
modelados de forma relacional (tabular) para consultas analíticas eficientes.

Por que SQLite para o Gold?
- Demonstra uso de múltiplos paradigmas (NoSQL + Relacional)
- Permite queries SQL padrão sobre os indicadores consolidados
- Arquivo único, sem infraestrutura adicional — ideal para carga Gold local

Tabelas criadas:
- `dim_uf`       — Dimensão de estados (código, nome, região)
- `dim_variavel` — Dimensão de variáveis da PNAD
- `fato_pnad`    — Tabela fato com as observações (período × UF × variável × sexo)
"""

import logging
import os
import sqlite3
from datetime import datetime, timezone

from dotenv import load_dotenv

from .base_loader import BaseLoader

load_dotenv()
logger = logging.getLogger(__name__)


class SQLiteLoader(BaseLoader):
    """
    Loader para a camada Gold no SQLite.

    Configuração via variável de ambiente:
        SQLITE_DB_PATH — Caminho do arquivo .db (padrão: pnad_gold.db)
    """

    def __init__(self):
        self.db_path = os.getenv("SQLITE_DB_PATH", "pnad_gold.db")
        self._initialize_schema()
        logger.info("SQLiteLoader inicializado. Banco: %s", self.db_path)

    # ------------------------------------------------------------------
    # Interface pública (contrato BaseLoader)
    # ------------------------------------------------------------------

    def load_documents(self, documents: list[dict]) -> int:
        """
        Persiste os documentos Silver nas tabelas Gold via INSERT OR REPLACE.
        Popula dim_variavel e dim_uf antes da fato_pnad para satisfazer as FKs.
        A chave primária (chave_unica) garante idempotência.
        """
        if not documents:
            return 0

        rows = [self._doc_to_row(doc) for doc in documents if doc.get("valor_disponivel")]

        dim_variaveis = list({
            doc["variavel_codigo"]: (
                doc["variavel_codigo"],
                doc.get("variavel"),
                doc.get("unidade"),
            )
            for doc in documents if doc.get("variavel_codigo")
        }.values())

        dim_ufs = list({
            doc["uf_codigo"]: (
                doc["uf_codigo"],
                doc.get("uf"),
                doc.get("nivel_territorial"),
            )
            for doc in documents if doc.get("uf_codigo")
        }.values())

        with sqlite3.connect(self.db_path) as con:
            con.executemany(
                "INSERT OR REPLACE INTO dim_variavel (variavel_codigo, variavel, unidade) VALUES (?,?,?)",
                dim_variaveis,
            )
            con.executemany(
                "INSERT OR REPLACE INTO dim_uf (uf_codigo, uf, nivel_territorial) VALUES (?,?,?)",
                dim_ufs,
            )
            con.executemany(
                """
                INSERT OR REPLACE INTO fato_pnad
                    (chave_unica, variavel_codigo, variavel, uf_codigo, uf,
                     nivel_territorial, periodo, sexo_codigo, sexo, valor,
                     unidade, data_carga)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                rows,
            )
            con.commit()

        logger.info(
            "Carga Gold concluída: %d fatos, %d variáveis, %d UFs gravados.",
            len(rows), len(dim_variaveis), len(dim_ufs),
        )
        return len(rows)

    def health_check(self) -> bool:
        try:
            with sqlite3.connect(self.db_path) as con:
                con.execute("SELECT 1")
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _initialize_schema(self):
        """Cria as tabelas do modelo relacional Gold se não existirem."""
        with sqlite3.connect(self.db_path) as con:
            con.executescript("""
                CREATE TABLE IF NOT EXISTS dim_variavel (
                    variavel_codigo TEXT PRIMARY KEY,
                    variavel        TEXT NOT NULL,
                    unidade         TEXT
                );

                CREATE TABLE IF NOT EXISTS dim_uf (
                    uf_codigo TEXT PRIMARY KEY,
                    uf        TEXT NOT NULL,
                    nivel_territorial TEXT
                );

                CREATE TABLE IF NOT EXISTS fato_pnad (
                    chave_unica       TEXT PRIMARY KEY,
                    variavel_codigo   TEXT NOT NULL,
                    variavel          TEXT NOT NULL,
                    uf_codigo         TEXT NOT NULL,
                    uf                TEXT NOT NULL,
                    nivel_territorial TEXT,
                    periodo           TEXT NOT NULL,
                    sexo_codigo       TEXT NOT NULL,
                    sexo              TEXT NOT NULL,
                    valor             REAL,
                    unidade           TEXT,
                    data_carga        TEXT,
                    FOREIGN KEY (variavel_codigo) REFERENCES dim_variavel(variavel_codigo),
                    FOREIGN KEY (uf_codigo) REFERENCES dim_uf(uf_codigo)
                );

                CREATE INDEX IF NOT EXISTS idx_fato_periodo ON fato_pnad(periodo);
                CREATE INDEX IF NOT EXISTS idx_fato_uf ON fato_pnad(uf_codigo);
                CREATE INDEX IF NOT EXISTS idx_fato_variavel ON fato_pnad(variavel_codigo);
                CREATE INDEX IF NOT EXISTS idx_fato_sexo ON fato_pnad(sexo);
            """)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _doc_to_row(self, doc: dict) -> tuple:
        """Converte um documento Silver para tupla de inserção na fato_pnad."""
        data_carga = doc.get("data_carga")
        if isinstance(data_carga, datetime):
            data_carga = data_carga.isoformat()

        return (
            doc.get("chave_unica"),
            doc.get("variavel_codigo"),
            doc.get("variavel"),
            doc.get("uf_codigo"),
            doc.get("uf"),
            doc.get("nivel_territorial"),
            doc.get("periodo"),
            doc.get("sexo_codigo"),
            doc.get("sexo"),
            doc.get("valor"),
            doc.get("unidade"),
            data_carga or datetime.now(timezone.utc).isoformat(),
        )
