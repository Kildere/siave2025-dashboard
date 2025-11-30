import json
import re
import unicodedata
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from src.gre_palette import GRE_COLOR_MAP, ordered_gre_labels, build_gre_legend_html

BASE_PARQUET = Path("data/processado/base_estrutural_normalizado.parquet")
GEOJSON_MUN = Path("src/geojs-25-mun.json")

# Nomes atuais x nomenclatura do geojson (que traz o nome antigo)
ALIASES_MUNICIPIOS_RAW = {
    "Joca Claudino": "Santar\u00e9m",
    "Tacima": "Campo de Santana",
    "S\u00e3o Domingos de Pombal": "S\u00e3o Domingos",
    "S\u00e3o Vicente do Serido": "Serid\u00f3",
}

st.set_page_config(page_title="SIAVE 2025", layout="wide")


def normalize(txt: str) -> str:
    """Remove acentos e padroniza para minusculas/trim."""
    return (
        unicodedata.normalize("NFKD", str(txt))
        .encode("ascii", "ignore")
        .decode()
        .lower()
        .strip()
    )


# Mapas de aliases: um para merge normalizado, outro para exibir com acento
ALIASES_MUNICIPIOS = {
    normalize(src): normalize(dst) for src, dst in ALIASES_MUNICIPIOS_RAW.items()
}
ALIASES_MUNICIPIOS_DISPLAY = {
    normalize(src): src for src in ALIASES_MUNICIPIOS_RAW.keys()
}


def parse_gre(value) -> str | None:
    """Extrai o numero da GRE de strings como '11ª GRE'."""
    match = re.search(r"\d+", str(value))
    return match.group(0) if match else None


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
fig = desenhar_mapa(df_map, geojson_mun)
st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
st.markdown(build_gre_legend_html(), unsafe_allow_html=True)

if not missing_geo.empty:
    with st.expander("Nomes conciliados (apenas quando ha divergencias)", expanded=True):
        st.write("Alias aplicados para casar base x geojson:")
        st.json(ALIASES_MUNICIPIOS_RAW)
        st.warning(
            "Municipios da base sem correspondencia no geojson (apos aliases): "
            + ", ".join(sorted(set(missing_geo["municipio_base"].astype(str))))
        )
