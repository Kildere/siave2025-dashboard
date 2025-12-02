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

st.set_page_config(page_title="SIAVE 2025", layout="wide")


def remove_accents(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", str(text))
    return "".join([c for c in nfkd if not unicodedata.combining(c)])


def normalize_upper(txt: str) -> str:
    """Remove acentos, coloca em maiusculas e tira espacos extras."""
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
    df_map["municipio_norm"] = df_map["municipio_norm"].apply(lambda x: ALIASES_MUNICIPIOS.get(x, x))
    df_map["gRE"] = df_map["gRE"].apply(normalize_upper)
    df_map = df_map.drop_duplicates(subset=["municipio_norm"])

    missing_geo = df_map[~df_map["municipio_norm"].isin(geo_names)][
        ["municipio", "gRE", "municipio_norm"]
    ]

    return df_map, missing_geo


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


st.title("Mapa das GREs - SIAVE 2025")
st.write(
    """
    Mapa interativo com os limites municipais da Paraiba coloridos pela GRE correspondente.
    
    """
)

df_base = load_base()
geojson_mun = load_geojson(GEOJSON_MUN)

df_map, missing_geo = preparar_mapa(df_base, geojson_mun)

st.subheader("Distribuicao das GREs pelos municipios")
if df_map.empty:
    st.warning("Sem dados para gerar o mapa.")
else:
    fig, palette = desenhar_mapa(df_map, geojson_mun)
    st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
    legend_html = build_palette_legend(palette)
    if legend_html:
        st.markdown(legend_html, unsafe_allow_html=True)

if not missing_geo.empty:
    with st.expander("Municipios sem correspondencia no geojson", expanded=True):
        st.warning(
            "Municipios da base sem correspondencia no geojson (apos normalizacao): "
            + ", ".join(sorted(set(missing_geo["municipio"].astype(str))))
        )
