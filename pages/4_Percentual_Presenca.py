from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from src.data_paths import ARQ_BASE_AGENDAMENTOS
from src.utils import format_timestamp_brazil

DATA_PROCESSADO = Path("data/processado")
PRESENCE_PARQUET = DATA_PROCESSADO / "base_percentual_presenca.parquet"

EXPECTED_COLS = [
    "coEscolaCenso",
    "escola",
    "municipio",
    "polo",
    "GRE",
    "dataAplicacaoReal",
    "previstos",
    "presentes",
    "percentual",
    "dia",
    "mes",
]

WEEKDAY_LABELS = [
    "Segunda",
    "Terca",
    "Quarta",
    "Quinta",
    "Sexta",
    "Sabado",
    "Domingo",
]


def format_int(value: float | int | None) -> str:
    if value is None:
        return "0"
    try:
        if pd.isna(value):
            return "0"
    except Exception:
        return "0"
    try:
        inteiro = int(round(float(value)))
    except (TypeError, ValueError):
        return "0"
    return f"{inteiro:,}".replace(",", ".")


def format_percent(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:.1f}%"


def _safe_load_parquet(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


def _prepare_base(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in EXPECTED_COLS:
        if col not in df.columns:
            df[col] = pd.NA

    for col in ["coEscolaCenso", "escola", "municipio", "polo", "GRE"]:
        df[col] = df[col].astype(str).str.strip()

    df["previstos"] = pd.to_numeric(df["previstos"], errors="coerce").fillna(0).astype(int)
    df["presentes"] = pd.to_numeric(df["presentes"], errors="coerce").fillna(0).astype(int)

    df["percentual"] = pd.to_numeric(df["percentual"], errors="coerce")
    calc_percentual = (
        (df["presentes"].astype("float64") / df["previstos"].astype("float64")) * 100
    ).where(df["previstos"] > 0)
    df["percentual"] = df["percentual"].combine_first(calc_percentual)

    df["dataAplicacaoReal"] = pd.to_datetime(df["dataAplicacaoReal"], errors="coerce")
    df["dia"] = pd.to_numeric(df["dia"], errors="coerce")
    df["mes"] = pd.to_numeric(df["mes"], errors="coerce")
    if "dataAplicacaoReal" in df.columns:
        df["ano"] = df["dataAplicacaoReal"].dt.year
        df["dia"] = df["dataAplicacaoReal"].dt.day
        df["mes"] = df["dataAplicacaoReal"].dt.month
        df["dia_da_semana"] = df["dataAplicacaoReal"].dt.weekday.map(
            lambda x: WEEKDAY_LABELS[x] if pd.notna(x) and x < len(WEEKDAY_LABELS) else None
        )
        iso_week = df["dataAplicacaoReal"].dt.isocalendar()
        df["semana"] = iso_week.week.astype("Int64")
        df["dataStr"] = df["dataAplicacaoReal"].dt.strftime("%d/%m/%Y")
    else:
        df["ano"] = pd.NA
        df["dia_da_semana"] = pd.NA
        df["semana"] = pd.NA
        df["dataStr"] = "-"
    return df


@st.cache_data
def load_agendamentos() -> pd.DataFrame | None:
    df = _safe_load_parquet(ARQ_BASE_AGENDAMENTOS)
    if df is None:
        return None
    return _prepare_base(df)


@st.cache_data
def load_presence() -> pd.DataFrame | None:
    df = _safe_load_parquet(PRESENCE_PARQUET)
    if df is None:
        return None
    return _prepare_base(df)


def merge_bases(df_ag: pd.DataFrame, df_presence: pd.DataFrame | None) -> pd.DataFrame:
    df_base = df_ag.copy()
    df_base["coEscolaCenso"] = df_base["coEscolaCenso"].astype(str).str.strip()
    if df_presence is None or df_presence.empty:
        return df_base

    df_presence = df_presence.copy()
    df_presence["coEscolaCenso"] = df_presence["coEscolaCenso"].astype(str).str.strip()

    merged = df_base.merge(
        df_presence,
        on="coEscolaCenso",
        how="left",
        suffixes=("", "_presence"),
    )

    combine_cols = [
        "escola",
        "municipio",
        "polo",
        "GRE",
        "dataAplicacaoReal",
        "previstos",
        "presentes",
        "percentual",
        "dia",
        "mes",
    ]
    for col in combine_cols:
        pres_col = f"{col}_presence"
        if pres_col in merged.columns:
            merged[col] = merged[pres_col].combine_first(merged[col])
            merged = merged.drop(columns=[pres_col])

    merged["previstos"] = pd.to_numeric(merged["previstos"], errors="coerce").fillna(0).astype(int)
    merged["presentes"] = pd.to_numeric(merged["presentes"], errors="coerce").fillna(0).astype(int)
    merged["percentual"] = pd.to_numeric(merged["percentual"], errors="coerce")
    calc_percentual = (
        (merged["presentes"].astype("float64") / merged["previstos"].astype("float64")) * 100
    ).where(merged["previstos"] > 0)
    merged["percentual"] = merged["percentual"].combine_first(calc_percentual)

    merged["dataAplicacaoReal"] = pd.to_datetime(merged["dataAplicacaoReal"], errors="coerce")
    merged["ano"] = merged["dataAplicacaoReal"].dt.year
    merged["dia"] = merged["dataAplicacaoReal"].dt.day
    merged["mes"] = merged["dataAplicacaoReal"].dt.month
    merged["dia_da_semana"] = merged["dataAplicacaoReal"].dt.weekday.map(
        lambda x: WEEKDAY_LABELS[x] if pd.notna(x) and x < len(WEEKDAY_LABELS) else None
    )
    iso_week = merged["dataAplicacaoReal"].dt.isocalendar()
    merged["semana"] = iso_week.week.astype("Int64")
    merged["dataStr"] = merged["dataAplicacaoReal"].dt.strftime("%d/%m/%Y")

    return merged


def build_group_percent(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    if group_col not in df.columns:
        return pd.DataFrame()
    grouped = (
        df.dropna(subset=[group_col])
        .groupby(group_col, as_index=False)
        .agg(
            previstos=("previstos", "sum"),
            presentes=("presentes", "sum"),
        )
    )
    grouped["percentual"] = (
        (grouped["presentes"].astype("float64") / grouped["previstos"].astype("float64")) * 100
    ).where(grouped["previstos"] > 0)
    grouped["percentual"] = grouped["percentual"].fillna(0)
    grouped = grouped.sort_values("percentual", ascending=False)
    grouped["percentualFmt"] = grouped["percentual"].map(lambda x: f"{x:.1f}%")
    grouped["previstos"] = grouped["previstos"].fillna(0).round().astype(int)
    grouped["presentes"] = grouped["presentes"].fillna(0).round().astype(int)
    return grouped


st.title("Registro de Aplicacoes - Presenca")

df_ag = load_agendamentos()
df_presence = load_presence()

if df_ag is None:
    st.warning("Parquet base_agendamentos.parquet nao encontrado. Execute o loader para gerar os dados.")
    st.stop()

if df_presence is None:
    st.info("Parquet base_percentual_presenca.parquet nao encontrado. Continuando somente com base_agendamentos.")

df = merge_bases(df_ag, df_presence)

nome_ag = ARQ_BASE_AGENDAMENTOS.name if ARQ_BASE_AGENDAMENTOS.exists() else "Nao encontrado"
dt_ag = (
    format_timestamp_brazil(datetime.fromtimestamp(ARQ_BASE_AGENDAMENTOS.stat().st_mtime))
    if ARQ_BASE_AGENDAMENTOS.exists()
    else "Data nao identificada"
)
nome_pres = PRESENCE_PARQUET.name if PRESENCE_PARQUET.exists() else "Nao encontrado"
dt_pres = (
    format_timestamp_brazil(datetime.fromtimestamp(PRESENCE_PARQUET.stat().st_mtime))
    if PRESENCE_PARQUET.exists()
    else "Data nao identificada"
)

st.markdown(
    f"""
<div style="
    margin-top:15px;
    margin-bottom:25px;
    padding:14px 18px;
    background-color:#e8f1ff;
    border-radius:10px;
    border:1px solid #9db5ff;
    font-size:1rem;
    font-weight:600;
    color:#0f2a47;">
Parquet agendamentos: {nome_ag} (atualizado em {dt_ag})<br>
Parquet presenca: {nome_pres} (atualizado em {dt_pres})
</div>
""",
    unsafe_allow_html=True,
)

if df.empty:
    st.info("Nenhum registro encontrado nas bases padronizadas.")
    st.stop()

total_previstos = int(df["previstos"].sum())
total_presentes = int(df["presentes"].sum())
percentual_geral = (total_presentes / total_previstos * 100) if total_previstos else None

col1, col2, col3 = st.columns(3)
col1.metric("Previstos (total)", format_int(total_previstos))
col2.metric("Presentes (total)", format_int(total_presentes))
col3.metric("% medio geral", format_percent(percentual_geral))

st.divider()
st.subheader("Presenca por GRE")
percent_gre = build_group_percent(df, "GRE")
if percent_gre.empty:
    st.info("Nao ha dados de GRE para exibir.")
else:
    fig_gre = px.bar(
        percent_gre,
        x="GRE",
        y="percentual",
        text=percent_gre["percentualFmt"],
        labels={"percentual": "% Presenca", "GRE": "GRE"},
    )
    fig_gre.update_layout(yaxis=dict(range=[0, 100]))
    st.plotly_chart(fig_gre, use_container_width=True)

st.subheader("Presenca por Polo")
percent_polo = build_group_percent(df, "polo")
if percent_polo.empty:
    st.info("Nao ha dados de Polo para exibir.")
else:
    fig_polo = px.bar(
        percent_polo,
        x="polo",
        y="percentual",
        text=percent_polo["percentualFmt"],
        labels={"percentual": "% Presenca", "polo": "Polo"},
    )
    fig_polo.update_layout(yaxis=dict(range=[0, 100]))
    st.plotly_chart(fig_polo, use_container_width=True)

st.subheader("Evolucao diaria de presenca")
if "dataAplicacaoReal" not in df.columns or df["dataAplicacaoReal"].dropna().empty:
    st.info("Sem datas de aplicacao para montar a evolucao diaria.")
else:
    daily = (
        df.dropna(subset=["dataAplicacaoReal"])
        .copy()
    )
    daily["diaReal"] = daily["dataAplicacaoReal"].dt.date
    daily_group = (
        daily.groupby("diaReal", as_index=False)
        .agg(previstos=("previstos", "sum"), presentes=("presentes", "sum"))
    )
    daily_group["percentual"] = (
        (daily_group["presentes"].astype("float64") / daily_group["previstos"].astype("float64")) * 100
    ).where(daily_group["previstos"] > 0)
    daily_group["percentual"] = daily_group["percentual"].fillna(0)
    fig_daily = px.line(
        daily_group,
        x="diaReal",
        y="percentual",
        markers=True,
        labels={"diaReal": "Data", "percentual": "% Presenca"},
    )
    fig_daily.update_layout(yaxis=dict(range=[0, 100]))
    st.plotly_chart(fig_daily, use_container_width=True)
