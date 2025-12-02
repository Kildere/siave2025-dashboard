import json
import unicodedata
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

BASE_PARQUET = Path("data/processado/base_estrutural_normalizado.parquet")
GEOJSON_MUN = Path("src/geojs-25-mun.json")

# Nomes atuais x nomenclatura do geojson (que traz o nome antigo)
ALIASES_MUNICIPIOS_RAW = {
    "Joca Claudino": "Santarém",
    "Tacima": "Campo de Santana",
    "São Domingos de Pombal": "São Domingos",
    "São Vicente do Serido": "Seridó",
}

st.set_page_config(page_title="SIAVE 2025", layout="wide")


def normalize_upper(txt: str) -> str:
    """Remove acentos, coloca em maiúsculas e tira espaços extras."""
    return (
        unicodedata.normalize("NFKD", str(txt))
        .encode("ascii", "ignore")
        .decode()
        .upper()
        .strip()
    )


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
        df = df.rename(columns={gre_cols[0]: "GRE"})
    if "GRE" not in df.columns:
        df["GRE"] = pd.NA
    df["GRE"] = df["GRE"].apply(normalize_upper)
    if "municipio" not in df.columns:
        df["municipio"] = pd.NA
    df["municipio_norm"] = df["municipio"].apply(normalize_upper)
    df["municipio_norm"] = df["municipio_norm"].apply(lambda x: ALIASES_MUNICIPIOS.get(x, x))
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
        name = props.get("name") or props.get("NM_MUN")
        props["name_norm"] = normalize_upper(name)
        feat["properties"] = props
    return geojson


def preparar_mapa(df: pd.DataFrame, geojson_mun: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Prepara dataframe com municipio_norm e GRE para o choropleth."""
    if df.empty:
        return pd.DataFrame(columns=["municipio", "GRE", "municipio_norm"]), pd.DataFrame()

    geo_names = {
        normalize_upper(feat.get("properties", {}).get("name_norm", ""))
        for feat in geojson_mun.get("features", [])
    }

    df_map = (
        df[["municipio", "GRE", "municipio_norm"]]
        .dropna(subset=["municipio", "GRE"])
        .copy()
    )
    df_map["municipio_norm"] = df_map["municipio_norm"].apply(lambda x: ALIASES_MUNICIPIOS.get(x, x))
    df_map["GRE"] = df_map["GRE"].apply(normalize_upper)
    df_map = df_map.drop_duplicates(subset=["municipio_norm"])

    missing_geo = df_map[~df_map["municipio_norm"].isin(geo_names)][
        ["municipio", "GRE", "municipio_norm"]
    ]

    return df_map, missing_geo


def build_palette_legend(labels: list[str], palette: list[str]) -> str:
    if not labels:
        return ""
    items = []
    for idx, label in enumerate(labels):
        color = palette[idx % len(palette)]
        items.append(
            f"""
            <div style="display:flex;align-items:center;gap:8px;margin:4px 0;">
                <div style="width:16px;height:16px;background:{color};
                            border:1px solid #000;border-radius:3px;"></div>
                <span style="font-size:0.9rem;color:#222;font-weight:600;">{label}</span>
            </div>
            """
        )
    return "<div style='margin-top:8px;'>" + "\n".join(items) + "</div>"


def desenhar_mapa(df_map: pd.DataFrame, geojson_mun: dict) -> tuple[go.Figure, list[str], list[str]]:
    if df_map.empty:
        return go.Figure(), [], []

    df_plot = df_map.copy()
    df_plot["GRE_cat"] = pd.Categorical(df_plot["GRE"])
    categories = list(df_plot["GRE_cat"].categories)
    z_values = df_plot["GRE_cat"].codes

    palette = px.colors.qualitative.Safe if px.colors.qualitative.Safe else px.colors.qualitative.Set3
    if not palette:
        palette = ["#636efa"]

    n_colors = max(len(categories), 1)
    denom = max(n_colors - 1, 1)
    colorscale = [
        (idx / denom, palette[idx % len(palette)]) for idx in range(n_colors)
    ]

    fig = go.Figure(
        go.Choropleth(
            geojson=geojson_mun,
            featureidkey="properties.name_norm",
            locations=df_plot["municipio_norm"],
            z=z_values,
            text=df_plot["municipio"],
            customdata=df_plot[["municipio", "GRE"]],
            hovertemplate="<b>%{customdata[0]}</b><br>GRE: %{customdata[1]}<extra></extra>",
            colorscale=colorscale,
            showscale=False,
            marker_line_width=0.8,
            marker_line_color="black",
        )
    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        height=720,
        autosize=True,
        showlegend=False,
    )
    return fig, categories, palette


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
fig, labels, palette = desenhar_mapa(df_map, geojson_mun)
st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
legend_html = build_palette_legend(labels, palette)
if legend_html:
    st.markdown(legend_html, unsafe_allow_html=True)

if not missing_geo.empty:
    with st.expander("Municipios sem correspondencia no geojson", expanded=True):
        st.warning(
            "Municipios da base sem correspondencia no geojson (apos normalizacao): "
            + ", ".join(sorted(set(missing_geo["municipio"].astype(str))))
        )
