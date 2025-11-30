import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from io import BytesIO
import unicodedata
import math

# ======================
# CONFIGURAÇÕES DA PÁGINA
# ======================
st.set_page_config(page_title="Registro de Aplicações – Presença por Data da Aplicação", layout="wide")

DATA_ORIGEM = Path("data/origem")
UPLOAD_PATH = DATA_ORIGEM / "Registro_Aplicacoes.xlsx"
PRESENCE_GLOB = "Percentual_Presenca-2025-11*.xlsx"

from src.data_paths import ARQ_BASE_APLICACOES

TEAM_GRE_GROUPS = {
    "Equipe 1 - Iara e Sely": {"1", "12", "16"},
    "Equipe 2 - Rodrigo e Kildere": {"7", "9", "10", "11"},
    "Equipe 3 - Andrea e Juvaneide": {"8", "13", "6", "5"},
    "Equipe 4 - Angelica e Janaina": {"3", "4", "2", "14", "15"},
}
TEAM_POLO_GROUPS = {
    "Equipe 1 - Iara e Sely": {
        "ITABAIANA 01",
        "ITABAIANA 02",
        "JOAO PESSOA 01",
        "JOAO PESSOA 02",
        "JOAO PESSOA 03",
        "JOAO PESSOA 04",
        "JOAO PESSOA 05",
        "JOAO PESSOA 06",
        "JOAO PESSOA 07",
        "SANTA RITA 01",
        "SANTA RITA 02",
        "SANTA RITA 03",
        "SANTA RITA 4",
        "SANTA RITA 5",
        "SANTA RITA 6",
        "SANTA RITA 7",
    },
    "Equipe 2 - Rodrigo e Kildere": {
        "CAJAZEIRAS 01",
        "CAJAZEIRAS 02",
        "ITAPORANGA 01",
        "ITAPORANGA 02",
        "PRINCESA ISABEL",
        "SOUSA 01",
        "SOUSA 02",
    },
    "Equipe 3 - Andrea e Juvaneide": {
        "CATOLE DO ROCHA 01",
        "CATOLE DO ROCHA 02",
        "MONTEIRO 01",
        "MONTEIRO 02",
        "PATOS 01",
        "PATOS 02",
        "PATOS 03",
        "POMBAL",
    },
    "Equipe 4 - Angelica e Janaina": {
        "CAMPINA GRANDE 01",
        "CAMPINA GRANDE 02",
        "CAMPINA GRANDE 03",
        "CAMPINA GRANDE 04",
        "CAMPINA GRANDE 05",
        "CAMPINA GRANDE 06",
        "CAMPINA GRANDE 07",
        "CAMPINA GRANDE 08",
        "CAMPINA GRANDE 09",
        "CUITE 01",
        "CUITE 02",
        "GUARABIRA 01",
        "GUARABIRA 02",
        "GUARABIRA 03",
        "GUARABIRA 04",
        "MAMANGUAPE 01",
        "MAMANGUAPE 02",
        "QUEIMADAS 01",
        "QUEIMADAS 02",
        "QUEIMADAS 03",
    },
}

SENHA_CORRETA = "A9C3B"

# ======================
# FUNÇÕES AUXILIARES
# ======================

def normalize_col(name: str) -> str:
    nfkd = unicodedata.normalize("NFKD", name)
    no_accents = "".join(c for c in nfkd if not unicodedata.combining(c))
    clean = (
        no_accents.strip()
        .lower()
        .replace(" ", "")
        .replace("_", "")
        .replace("-", "")
    )
    return clean


def extract_gre_digits(label: str | None) -> str | None:
    if label is None:
        return None
    digits = "".join(ch for ch in str(label) if ch.isdigit())
    return digits or None


def calcular_limite_superior(valor: float | int | None) -> float:
    if valor is None:
        return 1.0
    try:
        numero = float(valor)
    except (TypeError, ValueError):
        return 1.0
    if numero <= 0:
        return 1.0
    return max(1.0, math.ceil(numero * 1.1))


def format_int(value: float | int | None) -> str:
    if value is None:
        return "0"
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return "0"
    except TypeError:
        return "0"
    try:
        inteiro = int(round(float(value)))
    except (TypeError, ValueError):
        return "0"
    return f"{inteiro:,}".replace(",", ".")


@st.cache_data
def load_base_aplicacoes(path: Path = ARQ_BASE_APLICACOES) -> pd.DataFrame:
    if not path.exists():
        st.error("Base de aplicações não encontrada.")
        st.stop()
    df = pd.read_parquet(path)
    if "dataAgendmento" in df.columns:
        df["dataAgendmento"] = pd.to_datetime(df["dataAgendmento"])
    return df


def process_presence_file(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path)
    df.columns = [normalize_col(c) for c in df.columns]

    required = {"codigoescola", "aplicacao", "qtdalunospresentes", "qtdalunosprevistos"}
    if not required.issubset(df.columns):
        raise ValueError(f"Arquivo {path.name} sem colunas necessárias: {required}")

    df["qtdalunosprevistos"] = pd.to_numeric(df["qtdalunosprevistos"], errors="coerce").astype("Int64")
    df["qtdalunospresentes"] = pd.to_numeric(df["qtdalunospresentes"], errors="coerce").astype("Int64")
    df["aplicacaoid"] = (
        df["codigoescola"].astype(str).str.strip()
        + "_"
        + df["aplicacao"].astype(str).str.strip()
    )
    aggregated = (
        df.groupby("aplicacaoid", as_index=False)
        .agg(
            qtdPrevistos=("qtdalunosprevistos", "sum"),
            qtdPresentes=("qtdalunospresentes", "sum"),
        )
    )
    aggregated["presenceKey"] = aggregated["aplicacaoid"]
    aggregated["origem"] = path.name
    aggregated["percentual"] = (
        (
            aggregated["qtdPresentes"].astype("float64")
            / aggregated["qtdPrevistos"].astype("float64")
        )
        * 100
    ).where(aggregated["qtdPrevistos"] > 0)
    return aggregated


def process_registro_planilha(path: Path = UPLOAD_PATH) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        df = pd.read_excel(path)
    except Exception:
        return None
    df.columns = [normalize_col(c) for c in df.columns]
    required = {"codigoescola", "aplicacao", "qtdalunosprevistos", "qtdalunospresentes"}
    missing = [col for col in required if col not in df.columns]
    if missing:
        return None
    df["qtdalunosprevistos"] = pd.to_numeric(df["qtdalunosprevistos"], errors="coerce").astype("Int64")
    df["qtdalunospresentes"] = pd.to_numeric(df["qtdalunospresentes"], errors="coerce").astype("Int64")
    if "percentual" in df.columns:
        percent_series = (
            df["percentual"]
            .astype(str)
            .str.replace("%", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        df["percentual_calc"] = pd.to_numeric(percent_series, errors="coerce")
    else:
        prev = df["qtdalunosprevistos"].astype("float64")
        pres = df["qtdalunospresentes"].astype("float64")
        df["percentual_calc"] = (pres / prev * 100).where(prev > 0)
    df["aplicacaoid"] = (
        df["codigoescola"].astype(str).str.strip()
        + "_"
        + df["aplicacao"].astype(str).str.strip()
    )
    aggregated = (
        df.groupby("aplicacaoid", as_index=False)
        .agg(
            qtdPrevistos=("qtdalunosprevistos", "sum"),
            qtdPresentes=("qtdalunospresentes", "sum"),
        )
    )
    aggregated["presenceKey"] = aggregated["aplicacaoid"]
    aggregated["percentual"] = (
        (
            aggregated["qtdPresentes"].astype("float64")
            / aggregated["qtdPrevistos"].astype("float64")
        )
        * 100
    ).where(aggregated["qtdPrevistos"] > 0)
    aggregated["origem"] = path.name
    return aggregated


def load_presence_data() -> tuple[pd.DataFrame | None, str | None]:
    # Usa apenas o arquivo mais recente de presença para evitar divergências de versões.
    files = sorted(DATA_ORIGEM.glob(PRESENCE_GLOB))
    if not files:
        return None, None
    latest = files[-1]
    try:
        df = process_presence_file(latest)
    except Exception:
        return None, None
    return df, latest.name


def merge_presence(base_df: pd.DataFrame, presence_df: pd.DataFrame | None) -> pd.DataFrame:
    df = base_df.copy()
    df["presenceKey"] = (
        df["coEscolaCenso"].astype(str).str.strip()
        + "_"
        + df["diaAplicacao"].astype(str).str.strip()
    )
    if presence_df is None or presence_df.empty:
        df["qtdPrevistos"] = pd.NA
        df["qtdPresentes"] = pd.NA
        df["percentual"] = pd.NA
        return df.drop(columns=["presenceKey"])
    merged = df.merge(
        presence_df.drop(columns=["aplicacaoid"]),
        on="presenceKey",
        how="left",
        suffixes=("", "_presence"),
    )
    merged = merged.drop(columns=["presenceKey"])
    calc_percentual = (
        (
            merged["qtdPresentes"].astype("float64")
            / merged["qtdPrevistos"].astype("float64")
        )
        * 100
    ).where(merged["qtdPrevistos"].astype("float64") > 0)
    if "percentual" in merged.columns:
        merged["percentual"] = merged["percentual"].combine_first(calc_percentual)
    else:
        merged["percentual"] = calc_percentual
    return merged


def summarize_percentual(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    if group_col not in df.columns:
        return pd.DataFrame()
    grouped = (
        df.dropna(subset=[group_col])
        .groupby(group_col, as_index=False)
        .agg(
            previstos=("qtdPrevistos", "sum"),
            presentes=("qtdPresentes", "sum"),
        )
    )
    grouped["percentual"] = (
        (grouped["presentes"].astype("float64") / grouped["previstos"].astype("float64"))
        * 100
    ).where(grouped["previstos"] > 0)
    grouped["percentual"] = grouped["percentual"].fillna(0)
    grouped = grouped.sort_values("percentual", ascending=False)
    grouped["percentualFormat"] = grouped["percentual"].map(lambda x: f"{x:.1f}%")
    grouped["previstos"] = grouped["previstos"].fillna(0).astype(int)
    grouped["presentes"] = grouped["presentes"].fillna(0).astype(int)
    return grouped


# ======================
# INTERFACE
# ======================

st.title("Registro de Aplicações – Presença por Data da Aplicação")

from src.utils import get_latest_file, parse_timestamp_from_filename, format_timestamp_brazil

PASTA_PP = "data/origem/Percentual_Presenca"
prefixo = "Percentual_Presenca"

arquivo_recente = get_latest_file(PASTA_PP, prefixo)
nome_arq = arquivo_recente.name if arquivo_recente else "Nenhum arquivo encontrado"
dt_extraido = parse_timestamp_from_filename(nome_arq)
dt_br = format_timestamp_brazil(dt_extraido)

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
📂 Pasta: {PASTA_PP}<br>
🗂️ Arquivo carregado: {nome_arq}<br>
⏱️ Atualizado em: {dt_br}
</div>
""",
    unsafe_allow_html=True,
)

st.caption(
    "Integração automática entre a base de aplicações e a planilha Percentual_Presença para análise de presença por data real."
)

# ======================
# CARREGAR BASE CONSOLIDADA
# ======================

base_df = load_base_aplicacoes()
presence_df, _presence_sources = load_presence_data()
df = merge_presence(base_df, presence_df)

if "dataAgendmento" in df.columns:
    df["dataAgendmento"] = pd.to_datetime(df["dataAgendmento"], errors="coerce")
    df["dataFormatada"] = df["dataAgendmento"].dt.strftime("%d/%m/%Y")
else:
    df["dataFormatada"] = "-"

# ======================
# TABELA DE CONVERSÃO DIA → DATA REAL
# ======================

tabela_conv = (
    df.groupby(["diaAplicacao", "dataFormatada"])
    .size()
    .reset_index()
    [["diaAplicacao", "dataFormatada"]]
    .drop_duplicates()
)

st.subheader("Tabela de Conversão: Dia Aplicação → Data Real")
st.dataframe(tabela_conv, use_container_width=True)

# ======================
# PRESENÇA POR POLO > MUNICÍPIO > ESCOLA
# ======================

# Reaproveita o dataframe consolidado (df) e permite chegar até a escola, com filtro por Polo.
st.divider()
st.subheader("Visão por Polo > Município > Escola")
st.caption(
    "Fonte: arquivos Percentual_Presenca (inclui Percentual_Presenca-2025-11-29T14_47_06.986Z.xlsx)."
)

if presence_df is None or presence_df.empty:
    st.info("Nenhum arquivo Percentual_Presenca encontrado para montar a visão por Polo.")
    st.stop()

# Usar os valores originais da planilha de presença (presence_df), vinculando ao Polo/Município/Escola via base.
base_map = base_df.copy()
base_map["presenceKey"] = (
    base_map["coEscolaCenso"].astype(str).str.strip()
    + "_"
    + base_map["diaAplicacao"].astype(str).str.strip()
)
base_map = base_map[
    ["presenceKey", "polo", "municipio", "escola"]
].drop_duplicates(subset=["presenceKey"], keep="last")

df_pme = presence_df.copy()
if "percentual" not in df_pme.columns:
    df_pme["percentual"] = (
        (df_pme["qtdPresentes"].astype("float64") / df_pme["qtdPrevistos"].astype("float64"))
        * 100
    ).where(df_pme["qtdPrevistos"].astype("float64") > 0)

df_pme = df_pme.merge(base_map, on="presenceKey", how="left")
df_pme["qtdPrevistos"] = pd.to_numeric(df_pme["qtdPrevistos"], errors="coerce").fillna(0)
df_pme["qtdPresentes"] = pd.to_numeric(df_pme["qtdPresentes"], errors="coerce").fillna(0)
df_pme["percentual"] = pd.to_numeric(df_pme["percentual"], errors="coerce").fillna(0)
df_pme["percentualFmt"] = df_pme["percentual"].map(lambda x: f"{x:.1f}%")

polo_options = ["(Todos)"] + sorted(df_pme["polo"].dropna().astype(str).unique())
polo_selected = st.selectbox("Filtrar Polo", polo_options, index=0)

if polo_selected != "(Todos)":
    df_pme = df_pme[df_pme["polo"].astype(str) == polo_selected]

if df_pme.empty:
    st.info("Nenhum registro encontrado para o filtro de Polo selecionado.")
else:
    total_previstos = int(df_pme["qtdPrevistos"].sum())
    total_presentes = int(df_pme["qtdPresentes"].sum())
    percentual_total = (total_presentes / total_previstos * 100) if total_previstos else 0
    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric("Previstos (filtro)", f"{total_previstos:,}".replace(",", "."))
    kpi2.metric("Presentes (filtro)", f"{total_presentes:,}".replace(",", "."))
    kpi3.metric("% Presença (filtro)", f"{percentual_total:.1f}%")

    # Gráfico de previstos x presentes por Polo
    agrup_polo = (
        df_pme.groupby("polo", as_index=False)
        .agg(previstos=("qtdPrevistos", "sum"), presentes=("qtdPresentes", "sum"))
    )
    agrup_polo["percentual"] = (
        (agrup_polo["presentes"] / agrup_polo["previstos"]) * 100
    ).where(agrup_polo["previstos"] > 0)
    agrup_polo["percentual"] = agrup_polo["percentual"].fillna(0)

    st.markdown("#### Previstos x Presentes por Polo")
    fig_polo = go.Figure()
    fig_polo.add_bar(
        name="Previstos",
        x=agrup_polo["polo"],
        y=agrup_polo["previstos"],
        marker_color="#1f77b4",
        text=[f"{v:,.0f}".replace(",", ".") for v in agrup_polo["previstos"]],
        textposition="outside",
        opacity=0.9,
    )
    fig_polo.add_bar(
        name="Presentes",
        x=agrup_polo["polo"],
        y=agrup_polo["presentes"],
        marker_color="#6baed6",
        text=[f"{v:,.0f}".replace(",", ".") for v in agrup_polo["presentes"]],
        textposition="outside",
        opacity=0.85,
    )
    fig_polo.update_layout(
        barmode="group",
        yaxis_title="Quantidade de alunos",
        xaxis_title="Polo",
    )
    st.plotly_chart(fig_polo, use_container_width=True, key="polo_prev_pres_data_real")

    st.markdown("#### Percentual de Presença por Polo")
    fig_percentual_polo = px.bar(
        agrup_polo,
        x="polo",
        y="percentual",
        text=agrup_polo["percentual"].map(lambda x: f"{x:.1f}%"),
        labels={"percentual": "% Presença", "polo": "Polo"},
    )
    fig_percentual_polo.update_layout(yaxis=dict(range=[0, 100]))
    st.plotly_chart(fig_percentual_polo, use_container_width=True, key="polo_percentual_data_real")

    # Tabela e mapa hierárquico Polo > Município > Escola
    tabela_pme = (
        df_pme.groupby(["polo", "municipio", "escola"], as_index=False)
        .agg(previstos=("qtdPrevistos", "sum"), presentes=("qtdPresentes", "sum"))
    )
    tabela_pme["percentual"] = (
        (tabela_pme["presentes"] / tabela_pme["previstos"]) * 100
    ).where(tabela_pme["previstos"] > 0)
    tabela_pme["percentual"] = tabela_pme["percentual"].fillna(0)
    tabela_pme["percentualFmt"] = tabela_pme["percentual"].map(lambda x: f"{x:.1f}%")
    tabela_pme["previstos"] = tabela_pme["previstos"].fillna(0).round().astype(int)
    tabela_pme["presentes"] = tabela_pme["presentes"].fillna(0).round().astype(int)

    st.markdown("#### Tabela detalhada (Polo > Município > Escola)")
    tabela_mostrar = tabela_pme[["polo", "municipio", "escola", "previstos", "presentes", "percentualFmt"]].copy()
    st.dataframe(tabela_mostrar, use_container_width=True)
