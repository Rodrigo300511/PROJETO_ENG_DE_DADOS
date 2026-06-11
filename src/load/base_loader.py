"""
base_loader.py
--------------
Interface abstrata para todos os loaders do pipeline.
Define o contrato mínimo que qualquer destino de dados deve implementar.
"""

from abc import ABC, abstractmethod
from typing import Any


class BaseLoader(ABC):
    """
    Interface abstrata para loaders de dados.

    Permite que o pipeline seja agnóstico ao destino de armazenamento,
    facilitando trocas de banco sem alterar a orquestração.
    """

    @abstractmethod
    def load_documents(self, documents: list[dict]) -> int:
        """
        Persiste uma lista de documentos no destino configurado.

        Args:
            documents: Lista de dicionários a serem persistidos.

        Returns:
            int: Número de documentos efetivamente gravados/atualizados.
        """
        raise NotImplementedError

    @abstractmethod
    def health_check(self) -> bool:
        """
        Verifica se a conexão com o destino está ativa.

        Returns:
            bool: True se a conexão está operacional.
        """
        raise NotImplementedError
