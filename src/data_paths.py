
from pathlib import Path

DATA_ORIGEM = Path("data/origem")
DATA_PROCESSADO = Path("data/processado")

ARQ_TURMAS = DATA_ORIGEM / "Tuma_Escola_Polo-2025.xlsx"
ARQ_TURMAS_GRE = DATA_ORIGEM / "GRE_Polo_Turma_Escola.xlsx"
ARQ_BASE_FINAL = DATA_PROCESSADO / "base_estrutural.parquet"
ARQ_BASE_FINAL_NORMALIZADO = DATA_PROCESSADO / "base_estrutural_normalizado.parquet"

ARQ_AGENDAMENTOS = DATA_ORIGEM / "Agendamentos-2025-11-24T13_16_36.058Z.xlsx"
ARQ_BASE_AGENDAMENTOS = DATA_PROCESSADO / "base_agendamentos.parquet"
ARQ_BASE_APLICACOES = DATA_PROCESSADO / "base_aplicacoes.parquet"
