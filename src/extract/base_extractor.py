"""
base_extractor.py
-----------------
Classe base abstrata para todos os extratores do pipeline.
Define o contrato (interface) que qualquer extrator deve implementar,
garantindo substituibilidade (Liskov) e desacoplamento.
"""

from abc import ABC, abstractmethod
from typing import Any


class BaseExtractor(ABC):
    """
    Interface abstrata para extratores de dados.

    Qualquer fonte de dados (IBGE, RAIS, Caged, etc.) deve herdar
    desta classe e implementar o método `extract`.
    """

    @abstractmethod
    def extract(self, **kwargs) -> Any:
        """
        Executa a extração de dados da fonte configurada.

        Returns:
            Any: Dados brutos no formato nativo da fonte (JSON, CSV, etc.)
        """
        raise NotImplementedError
