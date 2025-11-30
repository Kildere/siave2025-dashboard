"""
BaseEstruturalLoader (versão DEFINITIVA)
Normaliza TUDO. Nunca mais haverá diferença de nomes de colunas.
"""

import pandas as pd
from pathlib import Path
from src.data_paths import ARQ_TURMAS_GRE, ARQ_BASE_FINAL
from src.utils import log
import unicodedata


def normalize_col(name: str) -> str:
    """Normaliza qualquer nome de coluna para camelCase sem acento."""
    # remover acentos
    nfkd = unicodedata.normalize("NFKD", name)
    no_accents = "".join([c for c in nfkd if not unicodedata.combining(c)])

    # remover espaços e deixar tudo minusculo
    clean = no_accents.replace(" ", "").replace("_", "")

    # primeira letra sempre minúscula
    clean = clean[0].lower() + clean[1:]

    return clean


class BaseEstruturalLoader:

    def run_all(self):
        log("Carregando base...")
        df = pd.read_excel(ARQ_TURMAS_GRE)

        log("Normalizando colunas...")
        # aplicar normalização definitiva
        df.columns = [normalize_col(c) for c in df.columns]

        log("Normalização aplicada com sucesso.")
        log(f"Colunas finais: {df.columns.tolist()}")

        # salvar parquet
        ARQ_BASE_FINAL.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(ARQ_BASE_FINAL, index=False)

        log(f"Base final salva em: {ARQ_BASE_FINAL}")
        log("Processo concluído.")
