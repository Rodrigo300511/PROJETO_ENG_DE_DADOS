"""
mongo_loader.py
---------------
Loader responsável pela persistência no MongoDB Atlas.

Estratégia de carga:
- Bronze: insert_many simples (snapshot imutável do dado bruto)
- Silver: upsert por `chave_unica` (idempotente — reexecuções seguras)

Índices criados automaticamente na primeira execução:
- Silver: índice único em `chave_unica`
- Silver: índices compostos em (uf, periodo) e (variavel_codigo, periodo)
"""

import logging
import os

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, ServerSelectionTimeoutError
from pymongo.server_api import ServerApi

from .base_loader import BaseLoader

load_dotenv()
logger = logging.getLogger(__name__)

_BATCH_SIZE = 500  # Tamanho do lote para bulk operations


class MongoLoader(BaseLoader):
    """
    Loader para o MongoDB Atlas.

    Configuração injetada via variáveis de ambiente:
        MONGO_URI               — Connection string do Atlas
        MONGO_DB_NAME           — Nome do banco (ex: ibge_pnad)
        MONGO_BRONZE_COLLECTION — Coleção de dados brutos
        MONGO_SILVER_COLLECTION — Coleção de dados curados
    """

    def __init__(self):
        uri = os.getenv("MONGO_URI")
        if not uri:
            # Constrói a URI a partir de usuário/senha se MONGO_URI não estiver definido
            user = os.getenv("DB_USER")
            password = os.getenv("DB_PASSWORD")
            uri = f"mongodb+srv://{user}:{password}@cluster0.wmnutcj.mongodb.net/?appName=Cluster0"

        self._client = MongoClient(uri, server_api=ServerApi("1"), serverSelectionTimeoutMS=10_000)
        db_name = os.getenv("MONGO_DB_NAME", "ibge_pnad")
        self._db = self._client[db_name]

        self._bronze_col = self._db[os.getenv("MONGO_BRONZE_COLLECTION", "bronze_pnad_raw")]
        self._silver_col = self._db[os.getenv("MONGO_SILVER_COLLECTION", "silver_pnad_docs")]

        self._ensure_indexes()
        logger.info("MongoLoader conectado ao banco '%s'.", db_name)

    # ------------------------------------------------------------------
    # Interface pública (contrato BaseLoader)
    # ------------------------------------------------------------------

    def load_documents(self, documents: list[dict]) -> int:
        """
        Persiste documentos Silver com upsert idempotente.

        Executa em lotes (bulk_write) para eficiência.
        Retorna o total de documentos inseridos + atualizados.
        """
        if not documents:
            logger.warning("Nenhum documento recebido para carga.")
            return 0

        total = 0
        for lote in self._chunked(documents, _BATCH_SIZE):
            total += self._upsert_silver(lote)

        logger.info("Carga Silver concluída: %d documentos gravados/atualizados.", total)
        return total

    def load_bronze(self, raw_data: list) -> int:
        """
        Persiste os dados brutos na camada Bronze (insert_many simples).
        Cada chamada cria um novo snapshot — dados imutáveis, append-only.
        """
        if not raw_data:
            return 0
        result = self._bronze_col.insert_many(raw_data, ordered=False)
        count = len(result.inserted_ids)
        logger.info("Carga Bronze concluída: %d documentos inseridos.", count)
        return count

    def health_check(self) -> bool:
        """Verifica conectividade com o Atlas."""
        try:
            self._client.admin.command("ping")
            return True
        except ServerSelectionTimeoutError:
            return False

    # ------------------------------------------------------------------
    # Implementação interna
    # ------------------------------------------------------------------

    def _upsert_silver(self, lote: list[dict]) -> int:
        """
        Executa bulk_write com UpdateOne + upsert=True para cada documento.
        A chave de filtro é `chave_unica`, garantindo idempotência.
        """
        operacoes = [
            UpdateOne(
                filter={"chave_unica": doc["chave_unica"]},
                update={"$set": doc},
                upsert=True,
            )
            for doc in lote
        ]
        try:
            result = self._silver_col.bulk_write(operacoes, ordered=False)
            return result.upserted_count + result.modified_count
        except BulkWriteError as bwe:
            logger.error("Erro em bulk_write: %s", bwe.details)
            raise

    def _ensure_indexes(self):
        """
        Cria índices na coleção Silver se ainda não existirem.
        Chamado uma única vez na inicialização do loader.
        """
        # Índice único na chave de upsert
        self._silver_col.create_index("chave_unica", unique=True, background=True)
        # Índices compostos para as consultas mais comuns do MCP
        self._silver_col.create_index([("uf", 1), ("periodo", 1)], background=True)
        self._silver_col.create_index([("variavel_codigo", 1), ("periodo", 1)], background=True)
        self._silver_col.create_index([("sexo", 1), ("uf", 1), ("periodo", -1)], background=True)
        logger.debug("Índices MongoDB verificados/criados.")

    @staticmethod
    def _chunked(lst: list, size: int):
        """Divide uma lista em lotes de tamanho `size`."""
        for i in range(0, len(lst), size):
            yield lst[i : i + size]

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
