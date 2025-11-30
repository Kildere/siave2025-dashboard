"""
Loader oficial da base de agendamentos e merge com a base estrutural.
Gera os arquivos:
 - data/processado/base_agendamentos.parquet
 - data/processado/base_aplicacoes.parquet
"""

from __future__ import annotations

from pathlib import Path
import unicodedata

import pandas as pd

from src.data_paths import (
    ARQ_AGENDAMENTOS,
    ARQ_BASE_AGENDAMENTOS,
    ARQ_BASE_APLICACOES,
    ARQ_BASE_FINAL_NORMALIZADO,
)
from src.utils import log


def normalize_col(name: str) -> str:
    """Normaliza nomes de colunas para camelCase sem acentos."""
    nfkd = unicodedata.normalize("NFKD", name)
    no_accents = "".join([c for c in nfkd if not unicodedata.combining(c)])
    clean = no_accents.replace(" ", "").replace("_", "").replace("-", "")
    clean = clean[0].lower() + clean[1:]
    return clean


def _coerce_time(series: pd.Series) -> pd.Series:
    """Converte textos de hora para objetos time."""
    parsed = pd.to_datetime(
        series.astype(str).str.strip(),
        format="%H:%M",
        errors="coerce",
    )
    return parsed.dt.time


def _load_raw_agendamentos(source_path: Path = ARQ_AGENDAMENTOS) -> pd.DataFrame:
    if not source_path.exists():
        raise FileNotFoundError(f"Arquivo de agendamentos nao encontrado: {source_path}")
    log(f"Lendo planilha de agendamentos: {source_path.name}")
    df = pd.read_excel(source_path)
    df.columns = [normalize_col(c) for c in df.columns]

    log("Convertendo campos de data e hora...")
    if "dataAgendmento" in df.columns:
        df["dataAgendmento"] = pd.to_datetime(
            df["dataAgendmento"], dayfirst=True, errors="coerce"
        )
    if "horaAgendamento" in df.columns:
        df["horaAgendamento"] = _coerce_time(df["horaAgendamento"])

    log("Criando chave unica da aplicacao (aplicacaoId)...")
    df["diaAplicacao"] = df["diaAplicacao"].astype(str).str.strip()
    df["aplicacaoId"] = (
        df["coTurmaCenso"].astype(str).str.strip()
        + "_"
        + df["diaAplicacao"]
    )

    return df


def gerar_base_agendamentos(
    source_path: Path = ARQ_AGENDAMENTOS,
    dest_path: Path = ARQ_BASE_AGENDAMENTOS,
) -> pd.DataFrame:
    """Processa a planilha original e salva o parquet normalizado."""
    df = _load_raw_agendamentos(source_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dest_path, index=False)
    log(f"Base de agendamentos salva em {dest_path}")
    return df


def carregar_agendamentos_processados(path: Path = ARQ_BASE_AGENDAMENTOS) -> pd.DataFrame:
    if not path.exists():
        log("Parquet de agendamentos nao encontrado. Gerando novamente...")
        return gerar_base_agendamentos(dest_path=path)
    return pd.read_parquet(path)


def merge_aplicacoes(
    base_estrutural_path: Path = ARQ_BASE_FINAL_NORMALIZADO,
    base_agendamentos_path: Path = ARQ_BASE_AGENDAMENTOS,
    dest_path: Path = ARQ_BASE_APLICACOES,
) -> pd.DataFrame:
    """Realiza o merge final entre base estrutural e agendamentos."""
    if not base_estrutural_path.exists():
        raise FileNotFoundError(
            "Base estrutural normalizada nao encontrada. Execute o normalizador antes."
        )

    df_ag = carregar_agendamentos_processados(base_agendamentos_path)
    df_base = pd.read_parquet(base_estrutural_path)

    join_keys = ["coTurmaCenso", "coEscolaCenso", "municipio", "polo", "uF"]
    faltando = [c for c in join_keys if c not in df_ag.columns]
    if faltando:
        raise ValueError(f"Colunas ausentes na base de agendamentos: {faltando}")

    faltando_base = [c for c in join_keys if c not in df_base.columns]
    if faltando_base:
        raise ValueError(f"Colunas ausentes na base estrutural: {faltando_base}")

    extras_base = [
        col
        for col in ["gRE", "turno", "serie", "rede", "localizacao"]
        if col in df_base.columns
    ]

    log("Mesclando bases de agendamentos e estrutural...")
    df_merge = pd.merge(
        df_ag,
        df_base[join_keys + extras_base].drop_duplicates(subset=join_keys),
        on=join_keys,
        how="left",
        suffixes=("", "_base"),
    )

    # ordenar colunas principais primeiro
    ordered_cols = [
        "aplicacaoId",
        "coTurmaCenso",
        "turma",
        "serie",
        "turno",
        "coEscolaCenso",
        "escola",
        "municipio",
        "polo",
        "uF",
        "gRE",
        "diaAplicacao",
        "dataAgendmento",
        "horaAgendamento",
        "statusAplicacao",
        "rede",
        "localizacao",
    ]
    ordered_cols = [c for c in ordered_cols if c in df_merge.columns]
    remainder = [c for c in df_merge.columns if c not in ordered_cols]
    df_merge = df_merge[ordered_cols + remainder]

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    df_merge.to_parquet(dest_path, index=False)
    log(f"Base final de aplicacoes salva em {dest_path}")
    return df_merge


def run():
    gerar_base_agendamentos()
    merge_aplicacoes()


if __name__ == "__main__":
    run()
