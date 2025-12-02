from pathlib import Path
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st

from src.data_paths import ARQ_BASE_AGENDAMENTOS
from src.utils import format_timestamp_brazil
from src.firebase_client import load_collection_df

DATA_PROCESSADO = Path("data/processado")
PRESENCE_PARQUET = DATA_PROCESSADO / "base_percentual_presenca.parquet"


def prep(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza colunas de agendamentos e presença para o padrão do Loader.
    """
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    rename_map = {
        "coescolacenso": "coEscolaCenso",
        "municipio": "municipio",
        "gre": "gRE",
        "polo": "polo",

        "previstos": "previstos",
        "qtdalunosprevistos": "previstos",
        "presentes": "presentes",
        "qtdalunospresentes": "presentes",

        "percentual": "percentual",

        "dataagendamento": "dataAgendamento",
        "dataagendmento": "dataAgendamento",
        "dataaplicacaoreal": "dataAplicacaoReal",
        "datareal": "dataAplicacaoReal",
        "qtdiasaplicacao": "qtdDiasAplicacao",
    }

    df.rename(columns=rename_map, inplace=True)
    return df


def load_df(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


# Tenta ler do Firestore primeiro
base_df_raw = load_collection_df("siave_agendamentos")
presence_df_raw = load_collection_df("siave_presenca")

# Se Firestore vier vazio, usa fallback em parquet
if base_df_raw is None or base_df_raw.empty:
    if ARQ_BASE_AGENDAMENTOS.exists():
        try:
            base_df_raw = pd.read_parquet(ARQ_BASE_AGENDAMENTOS)
        except Exception as exc:
            st.error(f"Falha ao ler base_agendamentos.parquet: {exc}")
            st.stop()
    else:
        base_df_raw = None

if presence_df_raw is None or presence_df_raw.empty:
    if PRESENCE_PARQUET.exists():
        try:
            presence_df_raw = pd.read_parquet(PRESENCE_PARQUET)
        except Exception as exc:
            st.error(f"Falha ao ler base_percentual_presenca.parquet: {exc}")
            st.stop()
    else:
        presence_df_raw = None

st.title("Registro de Aplicacoes - Presenca")

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

if base_df_raw is None:
    st.warning("Parquet base_agendamentos.parquet nao encontrado. Execute o loader para gerar os dados.")
    st.stop()

base_df = prep(base_df_raw)
presence_df = prep(presence_df_raw) if presence_df_raw is not None else pd.DataFrame()

def merge_presence(base: pd.DataFrame, presence: pd.DataFrame) -> pd.DataFrame:
    """
    Junta agendamentos (base) com presença (presence) usando o padrão do Loader.
    Chaves do merge: coEscolaCenso + municipio + gRE + polo.
    """
    if base is None or base.empty:
        return base

    if presence is None or presence.empty:
        df = base.copy()
        for col in ["previstos", "presentes", "percentual", "dataAplicacaoReal"]:
            if col not in df.columns:
                df[col] = pd.NA
        return df

    df = base.merge(
        presence,
        on=["coEscolaCenso", "municipio", "gRE", "polo"],
        how="left",
        suffixes=("", "_pres"),
    )

    for col in ["previstos", "presentes", "percentual", "dataAplicacaoReal"]:
        pres_col = f"{col}_pres"
        if pres_col in df.columns:
            df[col] = df[pres_col].where(df[pres_col].notna(), df.get(col))
            df.drop(columns=[pres_col], inplace=True)
        elif col not in df.columns:
            df[col] = pd.NA

    return df


df = merge_presence(base_df, presence_df)

if "dataAplicacaoReal" in df.columns:
    df["dataAplicacaoReal"] = pd.to_datetime(df["dataAplicacaoReal"], errors="coerce")
if "dataAgendamento" in df.columns:
    df["dataAgendamento"] = pd.to_datetime(df["dataAgendamento"], errors="coerce")
for num_col in ["previstos", "presentes"]:
    if num_col in df.columns:
        df[num_col] = pd.to_numeric(df[num_col], errors="coerce").fillna(0)

if df.empty:
    st.info("Nenhum registro encontrado nas bases padronizadas.")
    st.stop()

total_previstos = int(df["previstos"].sum()) if "previstos" in df.columns else 0
total_presentes = int(df["presentes"].sum()) if "presentes" in df.columns else 0
percentual_geral = (total_presentes / max(total_previstos, 1) * 100) if total_previstos else 0

col1, col2, col3 = st.columns(3)
col1.metric("Previstos (total)", f"{total_previstos:,}".replace(",", "."))
col2.metric("Presentes (total)", f"{total_presentes:,}".replace(",", "."))
col3.metric("% medio geral", f"{percentual_geral:.1f}%")

st.divider()
st.subheader("Presenca por GRE")
if "gRE" in df.columns:
    percent_gre = (
        df.groupby("gRE", as_index=False)
        .agg(previstos=("previstos", "sum"), presentes=("presentes", "sum"))
    )
    percent_gre["percentual"] = (percent_gre["presentes"] / percent_gre["previstos"].replace(0, 1)) * 100
    fig_gre = px.bar(
        percent_gre,
        x="gRE",
        y="percentual",
        text=percent_gre["percentual"].map(lambda x: f"{x:.1f}%"),
        labels={"percentual": "% Presenca", "gRE": "GRE"},
    )
    fig_gre.update_layout(yaxis=dict(range=[0, 100]))
    st.plotly_chart(fig_gre, use_container_width=True)
else:
    st.info("Base sem coluna gRE.")

st.subheader("Presenca por Polo")
if "polo" in df.columns:
    percent_polo = (
        df.groupby("polo", as_index=False)
        .agg(previstos=("previstos", "sum"), presentes=("presentes", "sum"))
    )
    percent_polo["percentual"] = (percent_polo["presentes"] / percent_polo["previstos"].replace(0, 1)) * 100
    fig_polo = px.bar(
        percent_polo,
        x="polo",
        y="percentual",
        text=percent_polo["percentual"].map(lambda x: f"{x:.1f}%"),
        labels={"percentual": "% Presenca", "polo": "Polo"},
    )
    fig_polo.update_layout(yaxis=dict(range=[0, 100]))
    st.plotly_chart(fig_polo, use_container_width=True)
else:
    st.info("Base sem coluna polo.")

st.subheader("Evolucao diaria de presenca")
if "dataAplicacaoReal" not in df.columns or df["dataAplicacaoReal"].replace("", pd.NA).dropna().empty:
    st.warning("Sem datas de aplicação na base processada.")
else:
    df_plot = df[df["dataAplicacaoReal"] != ""].copy()
    df_plot["dataAplicacaoReal"] = pd.to_datetime(df_plot["dataAplicacaoReal"], errors="coerce")
    daily = (
        df_plot.groupby("dataAplicacaoReal")
        .agg({"presentes": "sum", "previstos": "sum"})
        .reset_index()
    )
    daily["percentual"] = (daily["presentes"] / daily["previstos"].replace(0, 1)) * 100
    fig_daily = px.line(
        daily,
        x="dataAplicacaoReal",
        y="percentual",
        markers=True,
        labels={"dataAplicacaoReal": "Data", "percentual": "% Presenca"},
    )
    fig_daily.update_layout(yaxis=dict(range=[0, 100]))
    st.plotly_chart(fig_daily, use_container_width=True)
