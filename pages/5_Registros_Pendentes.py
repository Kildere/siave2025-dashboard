import unicodedata
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from src.data_paths import ARQ_TURMAS_GRE

SENHA_CORRETA = "A9C3B"
PENDENTES_PATTERN = "Registros_Pendentes-*.xlsx"
PARQUET_REGISTROS = Path("data/processado/base_registros_pendentes.parquet")

COLUMN_HINTS = {
    "gre": {"gre", "regional", "gerenciaregional", "gerencia", "gremetro"},
    "polo": {"polo"},
    "municipio": {"municipio", "cidade"},
    "escola": {"escola", "nomeescola", "unidadeescolar"},
    "tipo": {"tipopendencia", "pendencia", "tipopendencias", "pendencias", "tipo"},
    "data": {
        "data",
        "dia",
        "dataagendamento",
        "dataaplicacao",
        "dataregistro",
        "dataaplicacaoavaliacao",
    },
}
COLUMN_LABELS = {
    "gre": "Coluna GRE",
    "polo": "Coluna Polo",
    "municipio": "Coluna Município",
    "escola": "Coluna Escola",
    "tipo": "Coluna Tipo de Pendência",
    "data": "Coluna da Data do Registro",
}
DIAS_ANALISE = [
    date(2025, 11, 24),
    date(2025, 11, 25),
    date(2025, 11, 26),
    date(2025, 11, 27),
    date(2025, 11, 28),
    date(2025, 12, 1),
    date(2025, 12, 2),
    date(2025, 12, 3),
    date(2025, 12, 4),
    date(2025, 12, 5),
]
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
REF_HINTS = {
    "gre": COLUMN_HINTS["gre"],
    "polo": COLUMN_HINTS["polo"],
    "escola": COLUMN_HINTS["escola"],
}
COLUMN_LABELS = {
    "gre": "Coluna GRE",
    "polo": "Coluna Polo",
    "municipio": "Coluna Município",
    "escola": "Coluna Escola",
    "tipo": "Coluna Tipo de Pendência",
    "data": "Coluna da Data do Registro",
}


def normalize_col(name: str) -> str:
    text = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
    return (
        text.strip()
        .lower()
        .replace(" ", "")
        .replace("_", "")
        .replace("-", "")
    )


def normalize_value(value) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode()
    clean = text.strip().lower()
    return clean or None


def extract_gre_digits(value) -> str | None:
    if value is None:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits or None


def descobrir_colunas(df: pd.DataFrame) -> dict[str, str]:
    col_map: dict[str, str] = {}
    normalized = {col: normalize_col(col) for col in df.columns}
    for chave, possibilidades in COLUMN_HINTS.items():
        for original, norm in normalized.items():
            if norm in possibilidades:
                col_map[chave] = original
                break
    return col_map


def localizar_arquivo_padrao() -> Path | None:
    return PARQUET_REGISTROS if PARQUET_REGISTROS.exists() else None


@st.cache_data(show_spinner=False)
def carregar_planilha(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)


def carregar_df_pendentes(uploaded_file) -> tuple[pd.DataFrame | None, str | None]:
    arquivo_padrao = localizar_arquivo_padrao()
    if arquivo_padrao is None:
        st.warning("Nenhum arquivo base_registros_pendentes.parquet foi encontrado em data/processado.")
        return None, None
    try:
        df = carregar_planilha(str(arquivo_padrao))
        return df, arquivo_padrao.name
    except Exception as exc:
        st.error(f"Não foi possível ler {arquivo_padrao.name}: {exc}")
        return None, arquivo_padrao.name


@st.cache_data(show_spinner=False)
def carregar_referencia_gre() -> tuple[pd.DataFrame | None, dict[str, str]]:
    if not ARQ_TURMAS_GRE.exists():
        return None, {}
    try:
        df = pd.read_excel(ARQ_TURMAS_GRE)
    except Exception:
        return None, {}
    col_map: dict[str, str] = {}
    normalized = {col: normalize_col(col) for col in df.columns}
    for chave, possibilidades in REF_HINTS.items():
        for original, norm in normalized.items():
            if norm in possibilidades:
                col_map[chave] = original
                break
    return df, col_map


def aplicar_mapeamentos_auxiliares(df: pd.DataFrame, col_map: dict[str, str]) -> dict[str, str]:
    novo_map = dict(col_map)
    ref_df, ref_cols = carregar_referencia_gre()
    if ref_df is None:
        return novo_map

    def mapear(origem_chave: str, destino_chave: str, nome_coluna: str) -> bool:
        origem_ref = ref_cols.get(origem_chave)
        destino_ref = ref_cols.get(destino_chave)
        origem_df = novo_map.get(origem_chave)
        if (
            not origem_ref
            or not destino_ref
            or not origem_df
            or origem_df not in df.columns
            or origem_ref not in ref_df.columns
            or destino_ref not in ref_df.columns
        ):
            return False
        referencia = (
            ref_df[[origem_ref, destino_ref]]
            .dropna(subset=[origem_ref, destino_ref])
            .copy()
        )
        if referencia.empty:
            return False
        referencia["__key"] = referencia[origem_ref].apply(normalize_value)
        referencia = referencia.dropna(subset=["__key"])
        if referencia.empty:
            return False
        mapping = (
            referencia.drop_duplicates("__key").set_index("__key")[destino_ref].astype(str)
        )
        serie = df[origem_df].apply(normalize_value)
        df[nome_coluna] = serie.map(mapping)
        if df[nome_coluna].notna().any():
            novo_map[destino_chave] = nome_coluna
            return True
        df.drop(columns=[nome_coluna], inplace=True)
        return False

    if "gre" not in novo_map:
        if mapear("polo", "gre", "__gre_por_polo"):
            return novo_map
    if "gre" not in novo_map:
        mapear("escola", "gre", "__gre_por_escola")
    if "polo" not in novo_map:
        mapear("escola", "polo", "__polo_por_escola")
    return novo_map


def agrupar_contagens(
    df: pd.DataFrame,
    coluna: str | None,
    titulo: str,
    limite: int | None = None,
    chart_suffix: str | None = None,
):
    if not coluna or coluna not in df.columns:
        st.info(f"A planilha não possui coluna identificada como {titulo.lower()} para gerar o gráfico.")
        return
    serie = df[coluna].fillna("Sem informação").astype(str)
    if serie.empty:
        st.info(f"Sem dados suficientes para montar o gráfico de {titulo.lower()}.")
        return
    contagem = serie.value_counts().reset_index(name="Registros")
    valor_coluna = contagem.columns[0]
    contagem = contagem.rename(columns={valor_coluna: titulo})
    if limite is not None:
        contagem = contagem.head(limite)
    fig = px.bar(contagem, x=titulo, y="Registros", text_auto=True)
    fig.update_layout(title=titulo, yaxis_title="Registros", xaxis_title=titulo)
    chart_key = f"graf_{normalize_col(titulo)}"
    if chart_suffix:
        chart_key = f"{chart_key}_{normalize_col(chart_suffix)}"
    st.plotly_chart(fig, use_container_width=True, key=chart_key)


def destacar_label_dia(label: str) -> None:
    st.markdown(
        f"""
        <div style="
            margin:0.5rem 0 1rem;
            padding:0.6rem 0.9rem;
            background-color:#ecf2ff;
            border:1px solid #9db5ff;
            border-radius:10px;
            font-weight:600;
            font-size:1.1rem;
            color:#0f2a47;
            letter-spacing:0.5px;
        ">
            Dia {label}
        </div>
        """,
        unsafe_allow_html=True,
    )


def obter_colunas_para_tabela(df: pd.DataFrame, col_map: dict[str, str]) -> list[str]:
    ordered: list[str] = []
    for chave in ["gre", "polo", "municipio", "escola", "tipo"]:
        coluna = col_map.get(chave)
        if coluna and coluna in df.columns and coluna not in ordered:
            ordered.append(coluna)
    demais = [col for col in df.columns if col not in ordered]
    return ordered + demais


st.title("Registros Pendentes - SIAVE 2025")

uploaded_file = st.file_uploader("Envie o arquivo de Registros Pendentes (.xlsx)", type=["xlsx"])

from pathlib import Path
import os

PASTA = Path("data/origem/Registros_Pendentes")
PASTA.mkdir(parents=True, exist_ok=True)

if uploaded_file:
    nome_arquivo = PASTA / uploaded_file.name

    with open(nome_arquivo, "wb") as f:
        f.write(uploaded_file.getbuffer())

    st.success(f"Arquivo salvo em: {nome_arquivo}")
    st.info(f"Atualizado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}")

arquivo_recente = PARQUET_REGISTROS if PARQUET_REGISTROS.exists() else None
nome_arq = arquivo_recente.name if arquivo_recente else "Nenhum arquivo encontrado"
dt_br = (
    datetime.fromtimestamp(os.path.getmtime(arquivo_recente)).strftime("%d/%m/%Y %H:%M")
    if arquivo_recente
    else "Data não identificada"
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
📂 Pasta: {PARQUET_REGISTROS.parent}<br>
📄 Arquivo carregado: {nome_arq}<br>
🕒 Atualizado em: {dt_br}
</div>
""",
    unsafe_allow_html=True,
)

st.caption("Monitoramento dos registros pendentes enviados pelas GREs e polos.")

df_raw, fonte_dados = carregar_df_pendentes(uploaded_file)
if df_raw is None or df_raw.empty:
    st.stop()

if fonte_dados:
    st.caption(f"Fonte dos dados: {fonte_dados}")

col_map = descobrir_colunas(df_raw)
df = df_raw.copy()

with st.expander("Configurar colunas da planilha", expanded=False):
    col_opcoes = ["(Não usar)"] + list(df.columns)
    for chave, label in COLUMN_LABELS.items():
        default = col_map.get(chave)
        indice = col_opcoes.index(default) if default in col_opcoes else 0
        selecionado = st.selectbox(label, col_opcoes, index=indice, key=f"map_{chave}")
        if selecionado == "(Não usar)":
            col_map.pop(chave, None)
        else:
            col_map[chave] = selecionado

col_map = aplicar_mapeamentos_auxiliares(df, col_map)

# KPIs
total_registros = len(df)

def contar_unicos(chave: str) -> int:
    coluna = col_map.get(chave)
    if not coluna or coluna not in df.columns:
        return 0
    return df[coluna].dropna().astype(str).nunique()

kpi_cols = st.columns(5)
kpi_cols[0].metric("Total registros pendentes", total_registros)
kpi_cols[1].metric("Total de GREs", contar_unicos("gre"))
kpi_cols[2].metric("Total de Polos", contar_unicos("polo"))
kpi_cols[3].metric("Total de Escolas", contar_unicos("escola"))
kpi_cols[4].metric("Tipos de Pendência", contar_unicos("tipo"))

st.divider()
st.subheader("Filtros")

filtros = {
    "gre": "(Todos)",
    "equipe": "(Todas)",
    "polo": "(Todos)",
    "municipio": "(Todos)",
    "escola": "(Todas)",
    "tipo": "(Todos)",
}

filtered_df = df.copy()

col_filtros = st.columns(6)

colon_gre = col_map.get("gre")
if colon_gre:
    opcoes = ["(Todos)"] + sorted(filtered_df[colon_gre].dropna().astype(str).unique())
    filtros["gre"] = col_filtros[0].selectbox("GRE", opcoes, index=0)
    if filtros["gre"] != "(Todos)":
        filtered_df = filtered_df[filtered_df[colon_gre].astype(str) == filtros["gre"]]
else:
    col_filtros[0].info("Sem coluna de GRE.")

if colon_gre:
    equipe_opcoes = ["(Todas)"] + list(TEAM_GRE_GROUPS.keys())
    filtros["equipe"] = col_filtros[1].selectbox("Equipe (agrupamento de GREs)", equipe_opcoes, index=0)
    if filtros["equipe"] != "(Todas)":
        permitidos = TEAM_GRE_GROUPS[filtros["equipe"]]
        gre_series = filtered_df[colon_gre].apply(extract_gre_digits)
        filtered_df = filtered_df[gre_series.isin(permitidos)]
else:
    col_filtros[1].info("Mapeie a coluna de GRE para habilitar este filtro.")

colon_polo = col_map.get("polo")
if colon_polo:
    opcoes = ["(Todos)"] + sorted(filtered_df[colon_polo].dropna().astype(str).unique())
    filtros["polo"] = col_filtros[2].selectbox("Polo", opcoes, index=0)
    if filtros["polo"] != "(Todos)":
        filtered_df = filtered_df[filtered_df[colon_polo].astype(str) == filtros["polo"]]
else:
    col_filtros[2].info("Sem coluna de Polo.")

colon_mun = col_map.get("municipio")
if colon_mun:
    opcoes = ["(Todos)"] + sorted(filtered_df[colon_mun].dropna().astype(str).unique())
    filtros["municipio"] = col_filtros[3].selectbox("Município", opcoes, index=0)
    if filtros["municipio"] != "(Todos)":
        filtered_df = filtered_df[filtered_df[colon_mun].astype(str) == filtros["municipio"]]
else:
    col_filtros[3].info("Sem coluna de Município.")

colon_escola = col_map.get("escola")
if colon_escola:
    opcoes = ["(Todas)"] + sorted(filtered_df[colon_escola].dropna().astype(str).unique())
    filtros["escola"] = col_filtros[4].selectbox("Escola", opcoes, index=0)
    if filtros["escola"] != "(Todas)":
        filtered_df = filtered_df[filtered_df[colon_escola].astype(str) == filtros["escola"]]
else:
    col_filtros[4].info("Sem coluna de Escola.")

colon_tipo = col_map.get("tipo")
if colon_tipo:
    opcoes = ["(Todos)"] + sorted(filtered_df[colon_tipo].dropna().astype(str).unique())
    filtros["tipo"] = col_filtros[5].selectbox("Tipo de pendência", opcoes, index=0)
    if filtros["tipo"] != "(Todos)":
        filtered_df = filtered_df[filtered_df[colon_tipo].astype(str) == filtros["tipo"]]
else:
    col_filtros[5].info("Sem coluna de Tipo.")

st.divider()
st.subheader("Visão analítica de pendências e tabela detalhada por dia")

if filtered_df.empty:
    st.info("Nenhum registro encontrado para os filtros selecionados.")
else:
    colunas_tabela = obter_colunas_para_tabela(filtered_df, col_map)
    coluna_data = col_map.get("data")
    if not coluna_data or coluna_data not in filtered_df.columns:
        st.warning("Selecione uma coluna de data na configuração da planilha para habilitar os resultados por dia.")
    else:
        datas_normalizadas = pd.to_datetime(
            filtered_df[coluna_data], dayfirst=True, errors="coerce"
        ).dt.date
        if not datas_normalizadas.notna().any():
            st.warning("Nenhum valor de data válido foi encontrado na coluna selecionada.")
        else:
            tab_labels = [dia.strftime("%d/%m") for dia in DIAS_ANALISE]
            abas = st.tabs(tab_labels)
            for aba, dia in zip(abas, DIAS_ANALISE):
                with aba:
                    dia_legivel = dia.strftime("%d/%m/%Y")
                    destacar_label_dia(dia_legivel)
                    dia_df = filtered_df.loc[datas_normalizadas == dia]
                    if dia_df.empty:
                        st.info("Nenhum registro encontrado para este dia com os filtros selecionados.")
                        continue
                    slug = dia.strftime("%Y%m%d")
                    st.markdown("##### Visão analítica de pendências")
                    agrupar_contagens(dia_df, col_map.get("gre"), "Pendências por GRE", chart_suffix=slug)
                    polo_chart_df = dia_df
                    coluna_polo = col_map.get("polo")
                    if coluna_polo and coluna_polo in polo_chart_df.columns:
                        equipe_chart_opcoes = ["(Todas)"] + list(TEAM_POLO_GROUPS.keys())
                        equipe_chart = st.selectbox(
                            "Equipe (agrupamento de Polos)",
                            equipe_chart_opcoes,
                            index=0,
                            key=f"polo_chart_team_{slug}",
                        )
                        if equipe_chart != "(Todas)":
                            permitidos = TEAM_POLO_GROUPS.get(equipe_chart, set())
                            polo_chart_df = polo_chart_df[
                                polo_chart_df[coluna_polo].astype(str).isin(permitidos)
                            ]
                        polo_opcoes = ["(Todos)"] + sorted(
                            polo_chart_df[coluna_polo].dropna().astype(str).unique()
                        )
                        polo_selecionado = st.selectbox(
                            "Filtrar grafico de pendencias por Polo",
                            polo_opcoes,
                            index=0,
                            key=f"polo_chart_filter_{slug}",
                        )
                        if polo_selecionado != "(Todos)":
                            polo_chart_df = polo_chart_df[
                                polo_chart_df[coluna_polo].astype(str) == polo_selecionado
                            ]
                    agrupar_contagens(
                        polo_chart_df,
                        col_map.get("polo"),
                        "Pendencias por Polo",
                        chart_suffix=slug,
                    )
                    agrupar_contagens(
                        dia_df,
                        col_map.get("municipio"),
                        "Pendências por Município",
                        chart_suffix=slug,
                    )
                    agrupar_contagens(
                        dia_df,
                        col_map.get("escola"),
                        "Pendências por Escola (TOP 50)",
                        limite=50,
                        chart_suffix=slug,
                    )
                    agrupar_contagens(
                        dia_df,
                        col_map.get("tipo"),
                        "Pendências por Tipo de Pendência",
                        chart_suffix=slug,
                    )
                    st.markdown("##### Tabela detalhada")
                    st.caption(f"Total de registros exibidos: {len(dia_df)}")
                    st.dataframe(dia_df[colunas_tabela], use_container_width=True)
