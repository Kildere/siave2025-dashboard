import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

BASE_PARQUET = Path("data/processado/base_estrutural_normalizado.parquet")

st.title("Dashboard Estrutural - SIAVE 2025")

if not BASE_PARQUET.exists():
    st.error("Parquet nao encontrado. Execute o loader primeiro.")
    st.stop()

df = pd.read_parquet(BASE_PARQUET)

# colunas esperadas DEFINITIVAS
expected = [
    "uF",
    "polo",
    "coEscolaCenso",
    "escola",
    "municipio",
    "localizacao",
    "rede",
    "telefone1",
    "telefone2",
    "coTurmaCenso",
    "turma",
    "serie",
    "turno",
    "observacoesDaEscola",
    "temCiencias",
    "qtdDiasAplicacao",
    "gRE",
]

# verificar
missing = [c for c in expected if c not in df.columns]
if missing:
    st.error(f"Colunas ausentes na base: {missing}")
    st.write("Colunas disponiveis:", list(df.columns))
    st.stop()

# KPIs
col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("GREs", df["gRE"].nunique())
col2.metric("Polos", df["polo"].nunique())
col3.metric("Municipios", df["municipio"].nunique())
col4.metric("Escolas", df["coEscolaCenso"].nunique())
col5.metric("Turmas", df["coTurmaCenso"].nunique())

st.divider()

# Turmas por GRE
st.subheader("Turmas por GRE")

g1 = (
    df.groupby("gRE")["coTurmaCenso"]
    .nunique()
    .reset_index(name="total_turmas")
    .sort_values("total_turmas", ascending=True)
)

fig = px.bar(
    g1,
    x="gRE",
    y="total_turmas",
    text_auto=True,
    title="Total de Turmas por GRE",
    labels={"gRE": "GRE", "total_turmas": "Turmas"},
)

st.plotly_chart(fig, use_container_width=True)

# Exploracao por GRE
st.subheader("Exploracao por GRE")

gre_escolhida = st.selectbox(
    "Selecione a GRE:",
    ["(Todas)"] + sorted(df["gRE"].dropna().astype(str).unique()),
)

# --- Correcao do filtro da GRE ---
if gre_escolhida != "(Todas)":
    df = df[df["gRE"].astype(str) == gre_escolhida]

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
