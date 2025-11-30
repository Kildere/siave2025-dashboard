import ast
import json
import re
import unicodedata
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from src.gre_palette import (
    GRE_COLOR_MAP,
    gre_order_index,
    ordered_gre_labels,
    build_gre_legend_html,
)

def build_single_gre_legend_html(gre_label: str) -> str:
    """
    Gera legenda reduzida exibindo apenas a GRE selecionada,
    usando o mesmo padrão visual da legenda completa.
    """
    try:
        num = int(gre_label)
    except:
        return ""

    color = GRE_COLOR_MAP.get(str(num))
    if not color:
        return ""

    nome = f"{num}ª GERÊNCIA REGIONAL DA EDUCAÇÃO"

    return f"""
    <div style="display:flex; align-items:center; gap:8px; margin:8px 0;">
        <div style="width:18px; height:18px; background-color:{color};
                    border-radius:4px; border:1px solid #0002;"></div>
        <span style="font-size:0.9rem; color:#333; font-weight:600;">
            {nome}
        </span>
    </div>
    """

BASE_PARQUET = Path("data/processado/base_estrutural_normalizado.parquet")
GEOJSON_MUN = Path("src/geojs-25-mun.json")

# Nomes atuais x nomenclatura do geojson (que traz o nome antigo)
ALIASES_MUNICIPIOS_RAW = {
    "Joca Claudino": "Santar\u00e9m",
    "Tacima": "Campo de Santana",
    "S\u00e3o Domingos de Pombal": "S\u00e3o Domingos",
    "S\u00e3o Vicente do Serido": "Serid\u00f3",
}

# Dados de logistica (adaptado do rascunho fornecido)
LOGISTICA_DIR = Path("data/logistica")


def normalize(txt: str) -> str:
    """Remove acentos e padroniza para minusculas/trim."""
    return (
        unicodedata.normalize("NFKD", str(txt))
        .encode("ascii", "ignore")
        .decode()
        .lower()
        .strip()
    )


def parse_gre(value) -> str | None:
    """Extrai o numero da GRE de strings como '11a GRE'."""
    match = re.search(r"\d+", str(value))
    return match.group(0) if match else None


# Mapas de aliases: um para merge normalizado, outro para exibir com acento
ALIASES_MUNICIPIOS = {
    normalize(src): normalize(dst) for src, dst in ALIASES_MUNICIPIOS_RAW.items()
}
ALIASES_MUNICIPIOS_DISPLAY = {
    normalize(src): src for src in ALIASES_MUNICIPIOS_RAW.keys()
}


@st.cache_data
def load_base() -> pd.DataFrame:
    if not BASE_PARQUET.exists():
        st.error("Base processada nao encontrada. Execute o loader primeiro.")
        st.stop()
    return pd.read_parquet(BASE_PARQUET)


@st.cache_data
def load_geojson(path: Path) -> dict:
    if not path.exists():
        st.error(f"GeoJSON nao encontrado em {path}")
        st.stop()
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


@st.cache_data
def load_info_por_cidade(path: Path = Path("src/Infor_cidades.txt")) -> tuple[dict, dict]:
    """Carrega o dicionario de informacoes e retorna tambem uma versao normalizada."""
    if not path.exists():
        return {}, {}
    text = path.read_text(encoding="utf-8")
    body = text.split("=", 1)[1].strip()
    raw_info = ast.literal_eval(body)
    norm_map = {normalize(k): v for k, v in raw_info.items()}
    return raw_info, norm_map


@st.cache_data
def load_logistica(dir_path: Path = LOGISTICA_DIR) -> pd.DataFrame:
    """Lê todas as planilhas de logística e normaliza colunas/chaves."""
    if not dir_path.exists():
        st.error(f"Pasta de logistica nao encontrada: {dir_path}")
        st.stop()
    files = sorted([f for f in dir_path.glob("*.xlsx") if not f.name.startswith("~$")])
    if not files:
        st.error("Nenhum arquivo de logistica encontrado.")
        st.stop()

    frames = []
    for f in files:
        gre_num = "".join(ch for ch in f.stem if ch.isdigit()) or f.stem
        df = pd.read_excel(f)
        # normalizar nomes de colunas
        df.columns = [
            unicodedata.normalize("NFKD", c).encode("ascii", "ignore").decode().strip()
            for c in df.columns
        ]
        df["GRE_file"] = gre_num
        frames.append(df)

    df_log = pd.concat(frames, ignore_index=True)
    return df_log


def preparar_mapa(df: pd.DataFrame, geojson_mun: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Prepara dataframe com id do municipio e GRE para o choropleth."""
    geo_records = []
    for feat in geojson_mun["features"]:
        props = feat.get("properties", {})
        name = props.get("name") or props.get("NM_MUN")
        geo_records.append(
            {
                "id": props.get("id"),
                "name_geo": name,
                "name_norm": normalize(name),
            }
        )
    geo_df = pd.DataFrame(geo_records)

    df_mun = df[["municipio", "gRE"]].dropna().copy()
    df_mun["municipio_base"] = df_mun["municipio"].astype(str)
    df_mun["name_norm_base"] = df_mun["municipio_base"].apply(normalize)
    df_mun["name_norm"] = df_mun["name_norm_base"].apply(
        lambda x: ALIASES_MUNICIPIOS.get(x, x)
    )
    df_mun["municipio_display"] = df_mun.apply(
        lambda row: ALIASES_MUNICIPIOS_DISPLAY.get(
            row["name_norm_base"], row["municipio_base"]
        ),
        axis=1,
    )
    df_mun["gRE_label"] = df_mun["gRE"].apply(parse_gre)
    df_mun["gRE_label"] = df_mun["gRE_label"].fillna(df_mun["gRE"].astype(str))
    df_mun["gRE_label"] = df_mun["gRE_label"].astype(str)
    df_mun = df_mun.drop_duplicates(subset=["name_norm"])

    merged = pd.merge(df_mun, geo_df, on="name_norm", how="left")
    missing_geo = merged[merged["id"].isna()][
        ["municipio_base", "gRE", "name_norm"]
    ]

    merged = merged.dropna(subset=["id"]).copy()

    return merged, missing_geo


def preparar_logistica(df_map: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Concilia dados logísticos com municipios/GREs da base."""
    df_log = load_logistica()
    # Renomear campos principais para os nomes desejados (Regional -> Polo, Instituicao -> Escola)
    rename_map = {
        "Regional": "Polo",
        "Instituicao": "Escola",
    }
    df_log = df_log.rename(columns=rename_map)
    # Ajustar campos ausentes nas planilhas (peso/entrega não existem nas abas fornecidas)
    if "PESO" not in df_log.columns:
        df_log["PESO"] = 0
    if "ENTREGA" not in df_log.columns:
        df_log["ENTREGA"] = "N/A"
    if "COD" not in df_log.columns:
        df_log["COD"] = df_log["Pacote"] if "Pacote" in df_log.columns else None

    df_log["municipio_base"] = df_log["Municipio"].astype(str)
    df_log["name_norm_base"] = df_log["municipio_base"].apply(normalize)
    df_log["name_norm"] = df_log["name_norm_base"].apply(
        lambda x: ALIASES_MUNICIPIOS.get(x, x)
    )
    df_log = pd.merge(
        df_log,
        df_map[
            ["name_norm", "gRE", "gRE_label", "municipio_display", "municipio_base"]
        ],
        on="name_norm",
        how="left",
        suffixes=("", "_map"),
    )
    missing = df_log[df_log["gRE"].isna()][["Municipio", "Polo", "Escola"]]
    df_log["gRE_label"] = df_log["gRE_label"].fillna("N/A")
    df_log["municipio_display"] = df_log["municipio_display"].fillna(
        df_log["Municipio"]
    )
    return df_log, missing


def desenhar_mapa(df_map: pd.DataFrame, geojson_mun: dict) -> px.choropleth:
    ordered_labels = ordered_gre_labels(df_map["gRE_label"].unique())
    fig = px.choropleth(
        df_map,
        geojson=geojson_mun,
        locations="id",
        featureidkey="properties.id",
        color="gRE_label",
        hover_name="municipio_display",
        hover_data={
            "gRE": True,
            "gRE_label": False,
            "municipio_base": True,
            "id": False,
            "name_norm": False,
        },
        category_orders={"gRE_label": ordered_labels},
        color_discrete_map=GRE_COLOR_MAP,
    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        height=720,
        autosize=True,
        showlegend=False,
    )
    return fig


st.title("Logistica - SIAVE 2025")
st.write(
    """
    Painel de logistica com entregas por municipio/GRE usando a mesma base do aplicativo.
    """
)

df_base = load_base()
geojson_mun = load_geojson(GEOJSON_MUN)
info_raw, info_norm = load_info_por_cidade()

df_map, missing_geo = preparar_mapa(df_base, geojson_mun)
df_log, missing_log = preparar_logistica(df_map)

available_gres = ordered_gre_labels(df_map["gRE_label"].unique())
gre_opcoes = ["(Todas)"] + available_gres
gre_escolhida = st.selectbox("Filtrar por GRE:", gre_opcoes)

map_df = df_map.copy()
log_df = df_log.copy()
if gre_escolhida != "(Todas)":
    map_df = map_df[map_df["gRE_label"] == gre_escolhida]
    log_df = log_df[log_df["gRE_label"] == gre_escolhida]

col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)
with col_kpi1:
    st.metric("Entregas", len(log_df))
with col_kpi2:
    st.metric("Peso total (kg)", f"{log_df['PESO'].sum():,.1f}")
with col_kpi3:
    st.metric("GREs cobertas", log_df["gRE_label"].nunique())
with col_kpi4:
    st.metric(
        "Turnos",
        f"Manha: { (log_df['ENTREGA'] == 'MANHA').sum() } / Tarde: { (log_df['ENTREGA'] == 'TARDE').sum() }",
    )

st.subheader("Mapa das GREs (base logistica)")
fig = desenhar_mapa(map_df, geojson_mun)
st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
# Exibir legenda completa quando todas as GREs estiverem selecionadas
if gre_escolhida == "(Todas)":
    st.markdown(build_gre_legend_html(), unsafe_allow_html=True)
# Exibir legenda reduzida apenas para GRE selecionada
else:
    st.markdown(build_single_gre_legend_html(gre_escolhida), unsafe_allow_html=True)

st.subheader("Informacoes do ponto de entrega")
cidades_opcoes = ["(Selecione)"] + sorted(log_df["municipio_display"].unique())
cidade_escolhida = st.selectbox("Cidade/GRE", cidades_opcoes)

if cidade_escolhida != "(Selecione)":
    key = normalize(cidade_escolhida)
    key = ALIASES_MUNICIPIOS.get(key, key)
    html_info = info_norm.get(key)
    if html_info:
        st.markdown(html_info, unsafe_allow_html=True)
    else:
        st.info("Nenhuma informacao cadastrada para esta cidade no arquivo Infor_cidades.txt.")

if not missing_log.empty or not missing_geo.empty:
    with st.expander("Avisos de concilia\u00e7\u00e3o"):
        if not missing_log.empty:
            st.warning(
                "Entregas sem GRE ou municipio encontrado na base: "
                + ", ".join(sorted(set(missing_log["Municipio"].astype(str))))
            )
        if not missing_geo.empty:
            st.warning(
                "Municipios da base sem correspondencia no geojson: "
                + ", ".join(sorted(set(missing_geo["municipio_base"].astype(str))))
            )
