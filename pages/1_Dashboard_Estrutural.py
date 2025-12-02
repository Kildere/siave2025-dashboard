import unicodedata
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

BASE_PARQUET = Path("data/processado/base_estrutural_normalizado.parquet")
from src.firebase_client import load_collection_df

st.title("Dashboard Estrutural - SIAVE 2025")


@st.cache_data(show_spinner=False)
def load_base_estrutural() -> pd.DataFrame:
    # 1) Tenta Firestore
    df_fs = load_collection_df("siave_estrutural")
    if df_fs is not None and not df_fs.empty:
        return df_fs

    # 2) Fallback para parquet local
    if not BASE_PARQUET.exists():
        st.error("Base estrutural não encontrada. Execute o loader para gerar os dados.")
        st.stop()

    try:
        return pd.read_parquet(BASE_PARQUET)
    except Exception as exc:
        st.error(f"Falha ao ler o parquet processado: {exc}")
        st.stop()


df = load_base_estrutural()

def remove_accents(text):
    if text is None:
        return text
    text = str(text)
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join([c for c in nfkd if not unicodedata.combining(c)])


# Normalizar coluna GRE padronizada
if "GRE" not in df.columns:
    gre_alt = [c for c in df.columns if c.lower() == "gre"]
    if gre_alt:
        df = df.rename(columns={gre_alt[0]: "GRE"})
    else:
        df["GRE"] = pd.NA
df["GRE"] = df["GRE"].apply(remove_accents).str.upper().str.strip()

# colunas obrigatórias para exibição
required = ["municipio", "escola", "polo", "GRE"]
missing = [c for c in required if c not in df.columns]
if missing:
    st.warning(
        "Colunas ausentes na base: "
        + ", ".join(missing)
        + ". Algumas visões podem ficar incompletas."
    )
    for col in missing:
        df[col] = pd.NA

# KPIs
col1, col2, col3, col4 = st.columns(4)

col1.metric("GREs", df["GRE"].nunique())
col2.metric("Polos", df["polo"].nunique())
col3.metric("Municipios", df["municipio"].nunique())
col4.metric("Escolas", df["coEscolaCenso"].nunique())

st.divider()

# Turmas por GRE
st.subheader("Turmas por GRE")

g1 = (
    df.groupby("GRE")["coTurmaCenso"]
    .nunique()
    .reset_index(name="total_turmas")
    .sort_values("total_turmas", ascending=True)
)

fig = px.bar(
    g1,
    x="GRE",
    y="total_turmas",
    text_auto=True,
    title="Total de Turmas por GRE",
    labels={"GRE": "GRE", "total_turmas": "Turmas"},
)

st.plotly_chart(fig, use_container_width=True)

# Exploracao por GRE
st.subheader("Exploracao por GRE")

gre_escolhida = st.selectbox(
    "Selecione a GRE:",
    ["(Todas)"] + sorted(df["GRE"].dropna().astype(str).unique()),
)

# --- Correcao do filtro da GRE ---
if gre_escolhida != "(Todas)":
    df = df[df["GRE"].astype(str) == gre_escolhida]

# --- Visao por Polo ---
st.subheader("Visao por Polo")

polo_opcoes = ["(Todos)"] + sorted(df["polo"].dropna().astype(str).unique())
polo_escolhido = st.selectbox("Selecione o polo:", polo_opcoes)
df_polo = df
if polo_escolhido != "(Todos)":
    df_polo = df[df["polo"].astype(str) == polo_escolhido]

polos = (
    df_polo.groupby("polo")["coTurmaCenso"].nunique().reset_index(name="total_turmas")
)
fig_polo = px.bar(
    polos,
    text_auto=True,
    x="polo",
    y="total_turmas",
    title="Turmas por Polo",
)
st.plotly_chart(fig_polo, use_container_width=True)

# --- Visao por Municipio ---
st.subheader("Visao por Municipio")

municipio_opcoes = ["(Todos)"] + sorted(
    df_polo["municipio"].dropna().astype(str).unique()
)
municipio_escolhido = st.selectbox("Selecione o municipio:", municipio_opcoes)
df_municipio = df_polo
if municipio_escolhido != "(Todos)":
    df_municipio = df_municipio[
        df_municipio["municipio"].astype(str) == municipio_escolhido
    ]

municipios = (
    df_municipio.groupby("municipio")["coTurmaCenso"]
    .nunique()
    .reset_index(name="total_turmas")
)
fig_municipio = px.bar(
    municipios,
    text_auto=True,
    x="municipio",
    y="total_turmas",
    title="Turmas por Municipio",
)
st.plotly_chart(fig_municipio, use_container_width=True)

# --- Visao por Escola ---
st.subheader("Visao por Escola")

escola_opcoes = ["(Todas)"] + sorted(df_municipio["escola"].dropna().astype(str).unique())
escola_escolhida = st.selectbox("Selecione a escola:", escola_opcoes)
df_escola = df_municipio
if escola_escolhida != "(Todos)":
    df_escola = df_escola[df_escola["escola"].astype(str) == escola_escolhida]

escolas = (
    df_escola.groupby(["coEscolaCenso", "escola"])["coTurmaCenso"]
    .nunique()
    .reset_index(name="total_turmas")
)
fig_escolas = px.bar(
    escolas.sort_values("total_turmas", ascending=False).head(50),
    x="escola",
    y="total_turmas",
    text_auto=True,
    title="Top 50 Escolas com Mais Turmas",
)
st.plotly_chart(fig_escolas, use_container_width=True)

# --- Visao por Turma ---
st.subheader("Visao por Turma")
turmas = (
    df_escola.groupby(["serie", "turno"])["coTurmaCenso"]
    .nunique()
    .reset_index(name="total_turmas")
)
fig_turmas = px.bar(
    turmas,
    text_auto=True,
    x="serie",
    y="total_turmas",
    color="turno",
    barmode="group",
    title="Turmas por Serie e Turno",
)
st.plotly_chart(fig_turmas, use_container_width=True)

st.dataframe(df_escola)
