"""
ibge_extractor.py
-----------------
Cliente HTTP para a API de Agregados v3 do IBGE.

Responsabilidades:
- Montar e executar requisições à API do IBGE com segurança
- Tratar erros de rede (timeout, conexão) com retry exponencial
- Tratar erros de resposta da API (4xx, 5xx)
- Retornar dados brutos (Bronze) sem modificações de conteúdo
"""

import logging
import os
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

from .base_extractor import BaseExtractor

logger = logging.getLogger(__name__)


class IBGEExtractor(BaseExtractor):
    """
    Extrator de dados da API de Agregados v3 do IBGE.

    Configuração via variáveis de ambiente (injeção de dependência):
        IBGE_BASE_URL   — URL base da API (padrão: URL oficial do IBGE)
        IBGE_TIMEOUT    — Timeout HTTP em segundos (padrão: 30)
        IBGE_MAX_RETRIES — Máximo de tentativas em caso de erro (padrão: 3)
    """

    DEFAULT_BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados"

    def __init__(self):
        self.base_url = os.getenv("IBGE_BASE_URL", self.DEFAULT_BASE_URL).rstrip("/")
        self.timeout = int(os.getenv("IBGE_TIMEOUT", "30"))
        self.max_retries = int(os.getenv("IBGE_MAX_RETRIES", "3"))
        self._session = requests.Session()
        logger.info("IBGEExtractor inicializado. Base URL: %s", self.base_url)

    # ------------------------------------------------------------------
    # Método principal (contrato da interface BaseExtractor)
    # ------------------------------------------------------------------

    def extract(self, **kwargs) -> Any:
        """Alias de `extract_pnad` para conformidade com a interface base."""
        return self.extract_pnad(**kwargs)

    # ------------------------------------------------------------------
    # Método de extração PNAD
    # ------------------------------------------------------------------

    def extract_pnad(
        self,
        agregado: str,
        periodos: str,
        variaveis: str,
        localidades: str,
        classificacao: str = "",
    ) -> list:
        """
        Extrai dados da PNAD Contínua da API de Agregados v3 do IBGE.

        Args:
            agregado:      Código do agregado IBGE (ex: "4099" = desocupação)
            periodos:      Períodos desejados (ex: "all", "-4", "202301-202304")
            variaveis:     Código(s) de variável (ex: "4090" ou "all")
            localidades:   Filtro de território no padrão IBGE (ex: "N3[all]")
            classificacao: Filtro de classificação (ex: "2[all]|86[0]")

        Returns:
            list: JSON bruto retornado pela API (lista de blocos de metadados)

        Raises:
            requests.HTTPError:    Quando a API retorna 4xx/5xx
            requests.Timeout:      Quando a requisição excede o timeout
            requests.ConnectionError: Quando não é possível conectar à API
        """
        url = f"{self.base_url}/{agregado}/periodos/{periodos}/variaveis/{variaveis}"

        params: dict[str, str] = {"localidades": localidades}
        if classificacao and classificacao.strip():
            params["classificacao"] = classificacao.strip()

        logger.info("Iniciando extração — URL: %s | Params: %s", url, params)

        response = self._request_with_retry(url, params)
        data = response.json()

        logger.info(
            "Extração concluída. %d bloco(s) retornado(s).",
            len(data) if isinstance(data, list) else 1,
        )
        return data

    # ------------------------------------------------------------------
    # Requisição HTTP com retry exponencial via tenacity
    # ------------------------------------------------------------------

    def _request_with_retry(self, url: str, params: dict) -> requests.Response:
        """
        Executa GET com retry automático em erros de rede ou timeout.

        Estratégia: backoff exponencial (2s → 4s → 8s …) até max_retries.
        Erros HTTP (4xx/5xx) são levantados após a última tentativa.
        """

        @retry(
            reraise=True,
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=2, max=30),
            retry=retry_if_exception_type(
                (requests.ConnectionError, requests.Timeout)
            ),
            before_sleep=before_sleep_log(logger, logging.WARNING),
        )
        def _do_request() -> requests.Response:
            resp = self._session.get(url, params=params, timeout=self.timeout)
            resp.raise_for_status()  # Lança HTTPError em 4xx/5xx
            return resp

        return _do_request()

    def close(self):
        """Fecha a sessão HTTP (use em finally ou como context manager)."""
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
