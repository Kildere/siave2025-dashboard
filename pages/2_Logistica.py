import ast
import json
import unicodedata
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

BASE_PARQUET = Path("data/processado/base_estrutural_normalizado.parquet")
GEOJSON_MUN = Path("src/geojs-25-mun.json")

ALIASES_MUNICIPIOS_RAW = {
    "Joca Claudino": "Santarem",
    "Tacima": "Campo de Santana",
    "Sao Domingos de Pombal": "Sao Domingos",
    "Sao Vicente do Serido": "Serido",
}

LOGISTICA_DIR = Path("data/logistica")

st.set_page_config(page_title="Logistica SIAVE 2025", layout="wide")


def remove_accents(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", str(text))
    return "".join([c for c in nfkd if not unicodedata.combining(c)])


def normalize_upper(txt: str) -> str:
    return remove_accents(txt).upper().strip()


ALIASES_MUNICIPIOS = {
    normalize_upper(src): normalize_upper(dst) for src, dst in ALIASES_MUNICIPIOS_RAW.items()
}


@st.cache_data
def load_base() -> pd.DataFrame:
    if not BASE_PARQUET.exists():
        st.error("Base processada nao encontrada. Execute o loader primeiro.")
        st.stop()
    df = pd.read_parquet(BASE_PARQUET)
    gre_cols = [c for c in df.columns if c.lower() == "gre"]
    if gre_cols:
        df = df.rename(columns={gre_cols[0]: "gRE"})
    if "gRE" not in df.columns:
        df["gRE"] = pd.NA
    df["gRE"] = df["gRE"].apply(normalize_upper)
    if "municipio" not in df.columns:
        df["municipio"] = pd.NA
    df["municipio"] = df["municipio"].apply(normalize_upper)
    df["municipio_norm"] = df["municipio"].apply(lambda x: ALIASES_MUNICIPIOS.get(x, x))
    return df


@st.cache_data
def load_geojson(path: Path) -> dict:
    if not path.exists():
        st.error(f"GeoJSON nao encontrado em {path}")
        st.stop()
    with path.open("r", encoding="utf-8") as f:
        geojson = json.load(f)
    for feat in geojson.get("features", []):
        props = feat.get("properties", {})
        name_raw = props.get("name") or props.get("NM_MUN")
        name_norm = normalize_upper(name_raw)
        props["name"] = name_norm
        feat["properties"] = props
    return geojson


@st.cache_data
def load_info_por_cidade(path: Path = Path("src/Infor_cidades.txt")) -> tuple[dict, dict]:
    if not path.exists():
        return {}, {}
    text = path.read_text(encoding="utf-8")
    body = text.split("=", 1)[1].strip()
    raw_info = ast.literal_eval(body)
    norm_map = {normalize_upper(k): v for k, v in raw_info.items()}
    return raw_info, norm_map


@st.cache_data
def load_logistica(dir_path: Path = LOGISTICA_DIR) -> pd.DataFrame:
    if not dir_path.exists():
        st.error(f"Pasta de logistica nao encontrada: {dir_path}")
        st.stop()
    files = sorted([f for f in dir_path.glob("*.xlsx") if not f.name.startswith("~$")])
    if not files:
        st.error("Nenhum arquivo de logistica encontrado.")
        st.stop()

    frames = []
    for f in files:
        df = pd.read_excel(f)
        df.columns = [
            unicodedata.normalize("NFKD", c).encode("ascii", "ignore").decode().strip()
            for c in df.columns
        ]
        df["GRE_file"] = "".join(ch for ch in f.stem if ch.isdigit()) or f.stem
        frames.append(df)

    df_log = pd.concat(frames, ignore_index=True)
    return df_log


def preparar_mapa(df: pd.DataFrame, geojson_mun: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return pd.DataFrame(columns=["municipio", "gRE", "municipio_norm"]), pd.DataFrame()

    geo_names = {
        normalize_upper(feat.get("properties", {}).get("name", ""))
        for feat in geojson_mun.get("features", [])
    }

    df_map = (
        df[["municipio", "gRE", "municipio_norm"]]
        .dropna(subset=["municipio", "gRE"])
        .copy()
    )
    df_map["municipio"] = df_map["municipio"].apply(normalize_upper)
    df_map["municipio_norm"] = df_map["municipio"].apply(lambda x: ALIASES_MUNICIPIOS.get(x, x))
    df_map["gRE"] = df_map["gRE"].apply(normalize_upper)
    df_map = df_map.drop_duplicates(subset=["municipio_norm"])

    missing_geo = df_map[~df_map["municipio_norm"].isin(geo_names)][
        ["municipio", "gRE", "municipio_norm"]
    ]

    return df_map, missing_geo


def preparar_logistica(df_map: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_log = load_logistica()
    rename_map = {
        "Regional": "Polo",
        "Instituicao": "Escola",
    }
    df_log = df_log.rename(columns=rename_map)
    if "PESO" not in df_log.columns:
        df_log["PESO"] = 0
    if "ENTREGA" not in df_log.columns:
        df_log["ENTREGA"] = "N/A"
    if "COD" not in df_log.columns:
        df_log["COD"] = df_log["Pacote"] if "Pacote" in df_log.columns else None

    df_log["municipio"] = df_log["Municipio"].apply(normalize_upper)
    df_log["municipio_norm"] = df_log["municipio"].apply(lambda x: ALIASES_MUNICIPIOS.get(x, x))

    df_log = df_log.merge(
        df_map[["municipio_norm", "gRE", "municipio"]],
        on="municipio_norm",
        how="left",
        suffixes=("", "_map"),
    )
    missing = df_log[df_log["gRE"].isna()][["Municipio", "Polo", "Escola"]]
    df_log["gRE"] = df_log["gRE"].fillna("N/A")
    df_log["municipio"] = df_log["municipio"].fillna(df_log["Municipio"])
    return df_log, missing


def build_palette_legend(palette: dict[str, str]) -> str:
    if not palette:
        return ""
    html_legend = "".join(
        [
            f"<div style='display:flex;align-items:center;margin:4px 0;'>"
            f"<div style='width:16px;height:16px;background:{cor};margin-right:6px;border:1px solid #000;'></div>"
            f"{gre}</div>"
            for gre, cor in palette.items()
        ]
    )
    return html_legend


def desenhar_mapa(df_plot: pd.DataFrame, geo: dict) -> tuple[go.Figure, dict[str, str]]:
    if df_plot.empty:
        return go.Figure(), {}

    df_plot = df_plot.copy()
    df_plot["GRE_cat"] = df_plot["gRE"].astype(str).str.upper().str.strip()
    df_plot["GRE_cat"] = pd.Categorical(df_plot["GRE_cat"], ordered=True)
    df_plot["id"] = df_plot["municipio"].astype(str).str.upper().str.strip()

    categories = list(df_plot["GRE_cat"].cat.categories)
    palette = {
        gre: px.colors.qualitative.Vivid[i % len(px.colors.qualitative.Vivid)]
        for i, gre in enumerate(categories)
    }
    colorscale = (
        [[i / (len(palette) - 1), color] for i, color in enumerate(palette.values())]
        if palette
        else []
    )

    locations = []
    zvals = []
    for gre in categories:
        subset = df_plot[df_plot["GRE_cat"] == gre]
        for muni in subset["id"].unique():
            locations.append(muni)
            zvals.append(categories.index(gre) + 1)

    fig = go.Figure(
        go.Choropleth(
            geojson=geo,
            locations=locations,
            z=zvals,
            featureidkey="properties.name",
            colorscale=colorscale,
            marker_line_width=0.8,
            marker_line_color="black",
            showscale=False,
        )
    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(
        margin={"l": 0, "r": 0, "t": 0, "b": 0},
        height=680,
    )

    return fig, palette


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

available_gres = sorted(df_map["gRE"].dropna().unique().tolist())
gre_opcoes = ["(Todas)"] + available_gres
gre_escolhida = st.selectbox("Filtrar por GRE:", gre_opcoes)

map_df = df_map.copy()
log_df = df_log.copy()
if gre_escolhida != "(Todas)":
    map_df = map_df[map_df["gRE"] == gre_escolhida]
    log_df = log_df[log_df["gRE"] == gre_escolhida]

col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)
with col_kpi1:
    st.metric("Entregas", len(log_df))
with col_kpi2:
    st.metric("Peso total (kg)", f"{log_df['PESO'].sum():,.1f}")
with col_kpi3:
    st.metric("GREs cobertas", log_df["gRE"].nunique())
with col_kpi4:
    st.metric(
        "Turnos",
        f"Manha: { (log_df['ENTREGA'] == 'MANHA').sum() } / Tarde: { (log_df['ENTREGA'] == 'TARDE').sum() }",
    )

st.subheader("Mapa das GREs (base logistica)")
if map_df.empty:
    st.warning("Sem dados para gerar o mapa.")
else:
    fig, palette = desenhar_mapa(map_df, geojson_mun)
    st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
    legend_html = build_palette_legend(palette)
    if legend_html:
        st.markdown(legend_html, unsafe_allow_html=True)

st.subheader("Informacoes do ponto de entrega")
cidades_opcoes = ["(Selecione)"] + sorted(log_df["municipio"].dropna().unique())
cidade_escolhida = st.selectbox("Cidade/GRE", cidades_opcoes)

if cidade_escolhida != "(Selecione)":
    key = normalize_upper(cidade_escolhida)
    key = ALIASES_MUNICIPIOS.get(key, key)
    html_info = info_norm.get(key)
    if html_info:
        st.markdown(html_info, unsafe_allow_html=True)
    else:
        st.info("Nenhuma informacao cadastrada para esta cidade no arquivo Infor_cidades.txt.")

if not missing_log.empty or not missing_geo.empty:
    with st.expander("Avisos de conciliacao"):
        if not missing_log.empty:
            st.warning(
                "Entregas sem GRE ou municipio encontrado na base: "
                + ", ".join(sorted(set(missing_log["Municipio"].astype(str))))
            )
        if not missing_geo.empty:
            st.warning(
                "Municipios da base sem correspondencia no geojson: "
                + ", ".join(sorted(set(missing_geo["municipio"].astype(str))))
            )
