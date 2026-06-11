"""
pnad_transformer.py
-------------------
Transforma os dados brutos (Bronze) da API IBGE em documentos
estruturados e curados (Silver).

Responsabilidades:
- Normalizar o JSON aninhado em documentos planos (um por observação)
- Converter taxas de string para float (tipagem correta)
- Tratar valores ausentes/indisponíveis ("...", "-", "X", "")
- Enriquecer cada documento com metadados (variável, UF, período, sexo, data_carga)
- Gerar chave única idempotente para upsert no MongoDB
"""

import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# Marcadores de valor ausente usados pelo IBGE
_IBGE_NULL_MARKERS = frozenset({"...", "-", "X", "", "NA", "nd"})

# Mapeamento de código de sexo → rótulo legível
_SEXO_MAP = {
    "0": "Total",
    "1": "Homens",
    "2": "Mulheres",
}


class PNADTransformer:
    """
    Transforma a resposta bruta da API IBGE (lista de blocos de metadados)
    em uma lista plana de documentos prontos para carga no MongoDB (Silver).

    Cada documento representa uma única observação:
        período × UF × variável × sexo → valor (taxa)
    """

    def transform_to_documents(self, raw_data: list) -> list[dict]:
        """
        Ponto de entrada principal.

        Args:
            raw_data: Lista retornada pela IBGEExtractor (JSON bruto da API)

        Returns:
            Lista de documentos Silver, um por observação.
        """
        documents = []
        erros = 0

        for bloco in raw_data:
            try:
                docs = self._process_bloco(bloco)
                documents.extend(docs)
            except Exception as exc:
                logger.warning("Bloco ignorado por erro de transformação: %s", exc)
                erros += 1

        logger.info(
            "Transformação concluída. %d documentos gerados, %d bloco(s) com erro.",
            len(documents),
            erros,
        )
        return documents

    # ------------------------------------------------------------------
    # Processamento interno
    # ------------------------------------------------------------------

    def _process_bloco(self, bloco: dict) -> list[dict]:
        """Processa um bloco de metadados (uma variável) retornado pela API."""
        variavel_codigo = str(bloco.get("id", ""))
        variavel_nome = bloco.get("variavel", "")
        unidade = bloco.get("unidade", "")
        resultados = bloco.get("resultados", [])

        documents = []

        for resultado in resultados:
            # Extrai o contexto de classificação (sexo, cor, etc.)
            classificacoes = resultado.get("classificacoes", [])
            sexo_codigo, sexo_nome = self._extract_sexo(classificacoes)

            # Itera sobre cada localidade (UF ou Brasil)
            for serie in resultado.get("series", []):
                localidade = serie.get("localidade", {})
                uf_codigo = str(localidade.get("id", ""))
                uf_nome = localidade.get("nome", "")
                nivel = localidade.get("nivel", {}).get("nome", "")

                # Itera sobre cada período dentro da série temporal
                for periodo_str, valor_str in serie.get("serie", {}).items():
                    valor = self._parse_valor(valor_str)
                    doc = self._build_document(
                        variavel_codigo=variavel_codigo,
                        variavel_nome=variavel_nome,
                        unidade=unidade,
                        uf_codigo=uf_codigo,
                        uf_nome=uf_nome,
                        nivel_territorial=nivel,
                        periodo=self._normalizar_periodo(periodo_str),
                        periodo_raw=periodo_str,
                        sexo_codigo=sexo_codigo,
                        sexo_nome=sexo_nome,
                        valor=valor,
                    )
                    documents.append(doc)

        return documents

    def _build_document(self, **fields) -> dict:
        """Monta o documento Silver e adiciona metadados de controle."""
        chave = self._gerar_chave_unica(
            fields["periodo_raw"],
            fields["uf_codigo"],
            fields["variavel_codigo"],
            fields["sexo_codigo"],
        )
        return {
            # --- Identificação ---
            "chave_unica": chave,
            # --- Dimensões analíticas ---
            "variavel_codigo": fields["variavel_codigo"],
            "variavel": fields["variavel_nome"],
            "unidade": fields["unidade"],
            "uf_codigo": fields["uf_codigo"],
            "uf": fields["uf_nome"],
            "nivel_territorial": fields["nivel_territorial"],
            "periodo": fields["periodo"],
            "periodo_raw": fields["periodo_raw"],
            "sexo_codigo": fields["sexo_codigo"],
            "sexo": fields["sexo_nome"],
            # --- Medida ---
            "valor": fields["valor"],
            "valor_disponivel": fields["valor"] is not None,
            # --- Metadados de carga ---
            "data_carga": datetime.now(timezone.utc),
            "camada": "silver",
            "fonte": "IBGE_PNAD_Continua_API_v3",
        }

    # ------------------------------------------------------------------
    # Helpers de parsing e normalização
    # ------------------------------------------------------------------

    def _parse_valor(self, valor_str: str) -> Optional[float]:
        """
        Converte o valor textual da API para float.
        Retorna None para marcadores de ausência do IBGE ("...", "-", etc.).
        """
        if valor_str is None or str(valor_str).strip() in _IBGE_NULL_MARKERS:
            return None
        try:
            return float(str(valor_str).replace(",", ".").strip())
        except (ValueError, TypeError):
            logger.debug("Valor não convertível para float: '%s'", valor_str)
            return None

    def _extract_sexo(self, classificacoes: list) -> tuple[str, str]:
        """
        Extrai o código e nome do sexo a partir das classificações do resultado.
        Retorna ("0", "Total") se a classificação não estiver presente.
        """
        for clf in classificacoes:
            if clf.get("id") in ("2", 2):  # Classificação 2 = Sexo no IBGE
                categorias = clf.get("categoria", {})
                for codigo, nome in categorias.items():
                    return str(codigo), _SEXO_MAP.get(str(codigo), nome)
        return "0", "Total"

    def _normalizar_periodo(self, periodo_raw: str) -> str:
        """
        Normaliza o período trimestral do IBGE (ex: "202301") para
        um formato legível: "2023T1".
        """
        periodo_raw = str(periodo_raw).strip()
        if len(periodo_raw) == 6 and periodo_raw.isdigit():
            ano = periodo_raw[:4]
            trimestre = str(int(periodo_raw[4:]))  # "01" → "1"
            return f"{ano}T{trimestre}"
        return periodo_raw  # Mantém o original se não reconhecer o formato

    @staticmethod
    def _gerar_chave_unica(
        periodo: str, uf_codigo: str, variavel_codigo: str, sexo_codigo: str
    ) -> str:
        """
        Gera uma chave única determinística para uso como filtro de upsert.
        Formato: hash MD5 de "periodo_uf_variavel_sexo".
        Garante idempotência em reexecuções do pipeline.
        """
        raw = f"{periodo}_{uf_codigo}_{variavel_codigo}_{sexo_codigo}"
        return hashlib.md5(raw.encode()).hexdigest()
