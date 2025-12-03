import io
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Union

import pandas as pd
import streamlit as st

from src.base_estrutural_loader import normalize_col
from src.firebase_client import save_dataframe as firestore_save_dataframe


def normalize_columns(df):
    """
    Converte colunas para:
    - lowercase
    - sem acentos
    - substitui espaços por _
    - remove caracteres especiais
    """
    new_cols = []
    for col in df.columns:
        c = unicodedata.normalize("NFKD", col)
        c = "".join(ch for ch in c if not unicodedata.combining(ch))
        c = c.lower().replace(" ", "_")
        c = "".join(ch for ch in c if ch.isalnum() or ch == "_")
        new_cols.append(c)
    df.columns = new_cols
    return df


def save_dataframe(df: pd.DataFrame, collection: str, document: str | None = None) -> None:
    firestore_save_dataframe(collection, df)

st.set_page_config(page_title="Loader - SIAVE 2025", layout="wide")

PASSWORD_CORRECT = "A9C3B"
PROCESSADO_DIR = Path("data/processado")

UPLOAD_CONFIGS = [
    {
        "tab": "Base Estrutural",
        "title": "Base Estrutural",
        "prefix": "Base_Estrutural",
        "folder": Path("data/origem/Base_Estrutural"),
        "folder_display": "data/origem/Base_Estrutural",
    },
    {
        "tab": "Alocacoes",
        "title": "Alocacoes",
        "prefix": "Alocacoes",
        "folder": Path("data/origem/Alocacoes"),
        "folder_display": "data/origem/Alocacoes",
    },
    {
        "tab": "Percentual de Presenca",
        "title": "Percentual de Presenca",
        "prefix": "Percentual_Presenca",
        "folder": Path("data/origem/Percentual_Presenca"),
        "folder_display": "data/origem/Percentual_Presenca",
    },
    {
        "tab": "Registros Pendentes",
        "title": "Registros Pendentes",
        "prefix": "Registros_Pendentes",
        "folder": Path("data/origem/Registros_Pendentes"),
        "folder_display": "data/origem/Registros_Pendentes",
    },
]

ESTRUTURAL_SCHEMA = [
    "UF",
    "Polo",
    "coEscolaCenso",
    "Escola",
    "Municipio",
    "Localizacao",
    "Rede",
    "Telefone1",
    "Telefone2",
    "CoTurmaCenso",
    "Turma",
    "Serie",
    "Turno",
    "ObservacoesDaEscola",
    "TemCiencias",
    "QtdDiasAplicacao",
    "gRE",
    "diaAplicacao",
]

AGENDAMENTOS_SCHEMA = [
    "uf",
    "polo",
    "coEscolaCenso",
    "coTurmaCenso",
    "escola",
    "municipio",
    "turma",
    "serie",
    "turno",
    "tipoAplic",
    "statusAplicacao",
    "localizacao",
    "tipoRede",
    "aplicador",
    "cpf",
    "qtdAlunosPrevistos",
    "diaAplicacao",
    "dataAgendamento",
    "gRE",
    "aplicacaoId",
]

PRESENCA_SCHEMA = [
    "uf",
    "polo",
    "coEscolaCenso",
    "escola",
    "municipio",
    "tipoRede",
    "localizacao",
    "diaAplicacao",
    "serie",
    "tipoAplic",
    "turno",
    "coTurmaCenso",
    "turma",
    "dataReal",
    "qtdAlunosPrevistos",
    "qtdAlunosPresentes",
    "percentual",
    "gRE",
]

ORDINAL_SUFFIX = "\u00AA"

POLO_TO_GRE = {
    "JOAO PESSOA 01": "1",
    "JOAO PESSOA 02": "1",
    "JOAO PESSOA 03": "1",
    "JOAO PESSOA 04": "1",
    "JOAO PESSOA 05": "1",
    "JOAO PESSOA 06": "1",
    "JOAO PESSOA 07": "1",
    "GUARABIRA 01": "2",
    "GUARABIRA 02": "2",
    "GUARABIRA 03": "2",
    "GUARABIRA 04": "2",
    "CAMPINA GRANDE 01": "3",
    "CAMPINA GRANDE 02": "3",
    "CAMPINA GRANDE 03": "3",
    "CAMPINA GRANDE 04": "3",
    "CAMPINA GRANDE 05": "3",
    "CAMPINA GRANDE 06": "3",
    "CAMPINA GRANDE 07": "3",
    "CAMPINA GRANDE 08": "3",
    "CAMPINA GRANDE 09": "3",
    "CUITE 01": "4",
    "CUITE 02": "4",
    "MONTEIRO 01": "5",
    "MONTEIRO 02": "5",
    "PATOS 01": "6",
    "PATOS 02": "6",
    "PATOS 03": "6",
    "ITAPORANGA 01": "7",
    "ITAPORANGA 02": "7",
    "CATOLE 01": "8",
    "CATOLE 02": "8",
    "CAJAZEIRAS 01": "9",
    "CAJAZEIRAS 02": "9",
    "SOUSA 01": "10",
    "SOUSA 02": "10",
    "PRINCESA ISABEL": "11",
    "ITABAIANA 01": "12",
    "ITABAIANA 02": "12",
    "POMBAL": "13",
    "MAMANGUAPE 01": "14",
    "MAMANGUAPE 02": "14",
    "QUEIMADAS 01": "15",
    "QUEIMADAS 02": "15",
    "QUEMADAS 03": "15",
    "SANTA RITA 01": "16",
    "SANTA RITA 02": "16",
    "SANTA RITA 03": "16",
    "SANTA RITA 04": "16",
    "SANTA RITA 05": "16",
    "SANTA RITA 06": "16",
    "SANTA RITA 07": "16",
}


def ensure_folder(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def remove_accents(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", str(text))
    return "".join([c for c in nfkd if not unicodedata.combining(c)])


def normalizar_nome(nome: str) -> str:
    texto = remove_accents(str(nome)).lower()
    return "".join(ch for ch in texto if ch.isalnum())


def normalizar_municipio(x) -> Union[str, None]:
    if pd.isna(x):
        return pd.NA
    txt = remove_accents(str(x)).lower()
    txt = re.sub(r"[^a-z0-9 ]", " ", txt)
    txt = " ".join(txt.split())
    return txt if txt else pd.NA


def normalizar_gre(valor) -> Union[str, None]:
    """
    Converte entradas como '11a GRE', '11 a gre', '11\\u00AA Gre', '11 a gre', '11\\u00AA GRE'
    para o formato oficial '11\\u00AA GRE'.
    """
    if pd.isna(valor):
        return pd.NA
    texto = remove_accents(str(valor)).upper()
    digitos = re.findall(r"\d+", texto)
    if not digitos:
        return pd.NA
    numero = digitos[0].lstrip("0") or "0"
    try:
        numero_int = int(numero)
    except ValueError:
        return pd.NA
    return f"{numero_int}{ORDINAL_SUFFIX} GRE"


def normalizar_string(x) -> Union[str, None]:
    if pd.isna(x):
        return None
    txt = remove_accents(str(x)).strip()
    return txt if txt else None


def require_columns(df, required, name):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"[ERRO LOADER] Base '{name}' está faltando colunas obrigatórias: {missing}"
        )


def validate_all_bases(df_estrutural, df_agendamentos, df_presenca):
    require_columns(
        df_estrutural,
        ["municipio", "polo", "gre", "coescolacenso", "turma", "serie", "turno", "localizacao"],
        "Estrutural",
    )

    require_columns(
        df_agendamentos,
        ["municipio", "polo", "gre", "coescolacenso", "coturmacenso", "turma", "diaaplicacao", "dataagendamento"],
        "Agendamentos",
    )

    require_columns(
        df_presenca,
        PRESENCA_SCHEMA,
        "Presenca",
    )


def gerar_bytes_parquet(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    return buffer.getvalue()


def latest_file_with_prefix(folder: Path, prefix: str) -> Path | None:
    ensure_folder(folder)
    arquivos = sorted(folder.glob(f"{prefix}-*.xlsx"), key=lambda f: f.stat().st_mtime)
    return arquivos[-1] if arquivos else None


def list_files(folder: Path) -> list[Path]:
    ensure_folder(folder)
    return sorted([f for f in folder.glob("*.xlsx") if not f.name.startswith("~$")], key=lambda f: f.stat().st_mtime, reverse=True)


def render_history(folder: Path) -> None:
    st.markdown("**Historico dos ultimos 5 arquivos**")
    files = list_files(folder)[:5]
    if not files:
        st.info("Nenhum arquivo encontrado.")
        return
    data = []
    for file in files:
        created = datetime.fromtimestamp(file.stat().st_mtime)
        data.append({"Arquivo": file.name, "Criado em": created.strftime("%d/%m/%Y %H:%M:%S")})
    st.table(data)


def render_delete_section(folder: Path, prefix: str) -> None:
    files = list_files(folder)
    if not files:
        st.info("Nenhum arquivo disponivel para exclusao.")
        return
    options = [file.name for file in files]
    selected = st.selectbox("Selecione um arquivo para excluir", options, key=f"delete_select_{prefix}")
    if st.button("Excluir arquivo selecionado", key=f"delete_button_{prefix}"):
        target = folder / selected
        if target.exists():
            target.unlink()
            st.success("Arquivo removido com sucesso.")
            st.session_state["deleted_file"] = True
            st.stop()


def save_uploaded_file(uploaded_file, folder: Path, prefix: str) -> Path:
    ensure_folder(folder)
    now = datetime.utcnow()
    timestamp = now.strftime("%Y-%m-%dT%H_%M_%S.%f")[:23]
    filename = f"{prefix}-{timestamp}Z.xlsx"
    destination = folder / filename
    destination.write_bytes(uploaded_file.getbuffer())
    return destination


def pick_column(df: pd.DataFrame, norm_map: dict[str, str], candidates: list[str]) -> pd.Series | None:
    for cand in candidates:
        if cand in norm_map:
            return df[norm_map[cand]]
    return None


def serie_texto(df: pd.DataFrame, norm_map: dict[str, str], candidates: list[str]) -> pd.Series:
    col = pick_column(df, norm_map, candidates)
    if col is None:
        return pd.Series(pd.NA, index=df.index, dtype="string")
    return col.astype("string").str.strip().replace({"": pd.NA})


def serie_numero(df: pd.DataFrame, norm_map: dict[str, str], candidates: list[str]) -> pd.Series:
    col = pick_column(df, norm_map, candidates)
    if col is None:
        return pd.Series(pd.NA, index=df.index, dtype="Int64")
    return pd.to_numeric(col, errors="coerce").astype("Int64")


def serie_data(df: pd.DataFrame, norm_map: dict[str, str], candidates: list[str]) -> pd.Series:
    col = pick_column(df, norm_map, candidates)
    if col is None:
        return pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
    return pd.to_datetime(col, dayfirst=True, errors="coerce")


def gre_from_polo(polo_series: pd.Series) -> pd.Series:
    polo_norm = polo_series.astype("string").fillna("").apply(lambda x: remove_accents(x).upper().strip())
    return polo_norm.map(POLO_TO_GRE)


def process_base_estrutural(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_raw = pd.read_excel(path)
    norm_map = {normalizar_nome(c): c for c in df_raw.columns}

    df = pd.DataFrame(index=df_raw.index)
    df["UF"] = serie_texto(df_raw, norm_map, ["uf"])
    df["Polo"] = serie_texto(df_raw, norm_map, ["polo", "regional"])
    df["coEscolaCenso"] = serie_texto(df_raw, norm_map, ["coescolacenso", "codigoescola"])
    df["Escola"] = serie_texto(df_raw, norm_map, ["escola", "nomeescola"])
    df["Municipio"] = serie_texto(df_raw, norm_map, ["municipio", "cidade"]).apply(normalizar_municipio)
    df["Localizacao"] = serie_texto(df_raw, norm_map, ["localizacao", "localizacaoescola", "localidade"])
    df["Rede"] = serie_texto(df_raw, norm_map, ["rede", "tiporede"])
    df["Telefone1"] = serie_texto(df_raw, norm_map, ["telefone1", "telefone", "tel1"])
    df["Telefone2"] = serie_texto(df_raw, norm_map, ["telefone2", "tel2"])
    df["CoTurmaCenso"] = serie_texto(df_raw, norm_map, ["coturmacenso", "turmacenso", "codturma", "turma"])
    df["Turma"] = serie_texto(df_raw, norm_map, ["turma"])
    df["Serie"] = serie_texto(df_raw, norm_map, ["serie", "serieano"])
    df["Turno"] = serie_texto(df_raw, norm_map, ["turno"])
    df["ObservacoesDaEscola"] = serie_texto(df_raw, norm_map, ["observacoesdaescola", "observacoes", "observacao", "obsescola"])
    df["TemCiencias"] = serie_texto(df_raw, norm_map, ["temciencias", "ciencias"])
    df["QtdDiasAplicacao"] = serie_numero(df_raw, norm_map, ["qtddiasaplicacao", "diasaplicacao", "qtdiasaplicacao", "quantidadedias"])
    df["gRE"] = serie_texto(df_raw, norm_map, ["gre"])
    df["diaAplicacao"] = serie_texto(df_raw, norm_map, ["diaaplicacao", "diaplicacao", "dia", "dataaplicacao"])

    gre_fallback = gre_from_polo(df["Polo"])
    df["gRE"] = df["gRE"].where(~df["gRE"].isna(), gre_fallback)
    df["gRE"] = df["gRE"].apply(normalizar_gre)
    df["coEscolaCenso"] = df["coEscolaCenso"].astype("string").str.strip().replace({"": pd.NA})

    df = df.reindex(columns=ESTRUTURAL_SCHEMA)

    df_normalizado = df.copy()
    df_normalizado.columns = [normalize_col(c) for c in df_normalizado.columns]
    df_normalizado["municipio_norm"] = df_normalizado["municipio"].apply(normalizar_municipio)

    return df, df_normalizado


def process_base_agendamentos(path: Path) -> pd.DataFrame:
    df_raw = pd.read_excel(path)
    norm_map = {normalizar_nome(c): c for c in df_raw.columns}

    df = pd.DataFrame(index=df_raw.index)
    df["uf"] = serie_texto(df_raw, norm_map, ["uf"])
    df["polo"] = serie_texto(df_raw, norm_map, ["polo", "regional"])
    df["coEscolaCenso"] = serie_texto(df_raw, norm_map, ["coescolacenso", "codigoescola"])
    df["coTurmaCenso"] = serie_texto(df_raw, norm_map, ["coturmacenso", "turmacenso", "codturma", "turma"])
    df["escola"] = serie_texto(df_raw, norm_map, ["escola", "nomeescola"])
    df["municipio"] = serie_texto(df_raw, norm_map, ["municipio", "cidade"]).apply(normalizar_municipio)
    df["turma"] = serie_texto(df_raw, norm_map, ["turma"])
    df["serie"] = serie_texto(df_raw, norm_map, ["serie", "serieano"])
    df["turno"] = serie_texto(df_raw, norm_map, ["turno"])
    df["tipoAplic"] = serie_texto(df_raw, norm_map, ["tipoaplic", "tipoaplicacao", "aplicacao"])
    df["statusAplicacao"] = serie_texto(df_raw, norm_map, ["statusaplicacao", "status", "statusaplic"])
    df["localizacao"] = serie_texto(df_raw, norm_map, ["localizacao", "localizacaoescola", "localidade"])
    df["tipoRede"] = serie_texto(df_raw, norm_map, ["tiporede", "rede"])
    df["aplicador"] = serie_texto(df_raw, norm_map, ["aplicador", "aplicadora", "aplicadornome"])
    df["cpf"] = serie_texto(df_raw, norm_map, ["cpf", "aplicadorcpf"])
    df["qtdAlunosPrevistos"] = serie_numero(df_raw, norm_map, ["qtdalunosprevistos", "alocados", "qtdalunos"])
    df["diaAplicacao"] = serie_texto(df_raw, norm_map, ["diaaplicacao", "diaplicacao", "dia", "dataaplicacao"])
    df["dataAgendamento"] = serie_data(df_raw, norm_map, ["dataagendamento", "dataagendmento", "agendamento", "dataaplicacao"])
    df["gRE"] = serie_texto(df_raw, norm_map, ["gre"])
    df["aplicacaoId"] = serie_texto(df_raw, norm_map, ["aplicacaoid", "idaplicacao", "id"])

    gre_fallback = gre_from_polo(df["polo"])
    df["gRE"] = df["gRE"].where(~df["gRE"].isna(), gre_fallback)
    df["gRE"] = df["gRE"].apply(normalizar_gre)
    df["coEscolaCenso"] = df["coEscolaCenso"].astype("string").str.strip().replace({"": pd.NA})

    return df.reindex(columns=AGENDAMENTOS_SCHEMA)


def process_base_presenca(df_presence_raw: pd.DataFrame, df_estrutural_norm: pd.DataFrame) -> pd.DataFrame:
    presence_required_input = [
        "UF",
        "Polo",
        "CoEscolaCenso",
        "Escola",
        "MunicipioPolo",
        "MunicipioEscola",
        "TipoRede",
        "Localizacao",
        "Dia",
        "Serie",
        "TipoAplic",
        "Turno",
        "CoTurmaCenso",
        "Turma",
        "Agendamento",
        "QtdAlunosPrevistos",
        "QtdAlunosPresentes",
        "Percentual",
    ]

    df_raw = df_presence_raw.copy()
    df_raw.columns = [str(c).strip() for c in df_raw.columns]
    if "QtdAlunosPrevistos" not in df_raw.columns and "Alocados" in df_raw.columns:
        df_raw["QtdAlunosPrevistos"] = df_raw["Alocados"]
    if "QtdAlunosPresentes" not in df_raw.columns and "Presentes" in df_raw.columns:
        df_raw["QtdAlunosPresentes"] = df_raw["Presentes"]

    require_columns(df_raw, presence_required_input, "Presenca (entrada)")

    mapeamento = {
        "UF": "uf",
        "Polo": "polo",
        "CoEscolaCenso": "coEscolaCenso",
        "Escola": "escola",
        "TipoRede": "tipoRede",
        "Localizacao": "localizacao",
        "Dia": "diaAplicacao",
        "Serie": "serie",
        "TipoAplic": "tipoAplic",
        "Turno": "turno",
        "CoTurmaCenso": "coTurmaCenso",
        "Turma": "turma",
        "Agendamento": "dataReal",
        "QtdAlunosPrevistos": "qtdAlunosPrevistos",
        "QtdAlunosPresentes": "qtdAlunosPresentes",
        "Percentual": "percentual",
    }

    df_presence = df_raw.rename(columns=mapeamento)
    for col in mapeamento.values():
        if col not in df_presence.columns:
            df_presence[col] = pd.NA

    municipio_escola = df_raw.get("MunicipioEscola")
    municipio_polo = df_raw.get("MunicipioPolo")
    municipio_base = municipio_escola if municipio_escola is not None else pd.Series(pd.NA, index=df_raw.index)
    municipio_fallback = municipio_polo if municipio_polo is not None else pd.Series(pd.NA, index=df_raw.index)
    df_presence["municipio"] = (
        municipio_base.fillna(municipio_fallback)
        .astype("string")
        .str.strip()
        .str.upper()
    )

    df_presence["coEscolaCenso"] = df_presence["coEscolaCenso"].astype("string").str.strip().replace({"": pd.NA})
    df_presence["coTurmaCenso"] = df_presence["coTurmaCenso"].astype("string").str.strip().replace({"": pd.NA})
    df_presence["diaAplicacao"] = df_presence["diaAplicacao"].astype("string").str.strip()
    df_presence["turma"] = df_presence["turma"].astype("string").str.strip()
    df_presence["serie"] = df_presence["serie"].astype("string").str.strip()
    df_presence["tipoAplic"] = df_presence["tipoAplic"].astype("string").str.strip()
    df_presence["turno"] = df_presence["turno"].astype("string").str.strip()
    df_presence["polo"] = df_presence["polo"].astype("string").str.strip()
    df_presence["uf"] = df_presence["uf"].astype("string").str.strip()
    df_presence["tipoRede"] = df_presence["tipoRede"].astype("string").str.strip()
    df_presence["localizacao"] = df_presence["localizacao"].astype("string").str.strip()
    df_presence["escola"] = df_presence["escola"].astype("string").str.strip()

    df_presence["dataReal"] = pd.to_datetime(df_presence["dataReal"], dayfirst=True, errors="coerce")
    df_presence["qtdAlunosPrevistos"] = pd.to_numeric(df_presence["qtdAlunosPrevistos"], errors="coerce").fillna(0).astype(int)
    df_presence["qtdAlunosPresentes"] = pd.to_numeric(df_presence["qtdAlunosPresentes"], errors="coerce").fillna(0).astype(int)
    df_presence["percentual"] = pd.to_numeric(df_presence["percentual"], errors="coerce")

    df_merge = df_presence.merge(
        df_estrutural_norm[["coEscolaCenso", "municipio", "gRE"]],
        on="coEscolaCenso",
        how="left",
        suffixes=("", "_estrut"),
    )
    if "municipio_estrut" in df_merge.columns:
        df_merge["municipio"] = df_merge["municipio"].fillna(df_merge["municipio_estrut"])
        df_merge.drop(columns=["municipio_estrut"], inplace=True)
    df_merge["municipio"] = df_merge["municipio"].astype("string").str.strip().str.upper()

    if "gRE" in df_merge.columns:
        df_merge["gRE"] = df_merge["gRE"].astype("string").str.strip()

    df_final = df_merge[PRESENCA_SCHEMA].copy()
    return df_final


def process_base_pendentes(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path)
    new_cols = {col: remove_accents(str(col)).strip() for col in df.columns}
    df = df.rename(columns=new_cols)
    gre_cols = [c for c in df.columns if c.lower() == "gre"]
    if gre_cols:
        df = df.rename(columns={gre_cols[0]: "gRE"})
    if "gRE" in df.columns:
        df["gRE"] = df["gRE"].apply(normalizar_gre)
    mun_cols = [c for c in df.columns if normalizar_nome(c) == "municipio"]
    if mun_cols:
        df["municipio"] = df[mun_cols[0]].apply(normalizar_municipio)
    return df


def load_existing_outputs() -> None:
    if st.session_state.get("loader_ok"):
        return
    paths = {
        "estrutural": PROCESSADO_DIR / "base_estrutural.parquet",
        "agendamentos": PROCESSADO_DIR / "base_agendamentos.parquet",
        "presenca": PROCESSADO_DIR / "base_percentual_presenca.parquet",
        "pendentes": PROCESSADO_DIR / "base_registros_pendentes.parquet",
    }
    if all(p.exists() for p in paths.values()):
        st.session_state["arquivos_processados"] = {
            "estrutural": pd.read_parquet(paths["estrutural"]),
            "agendamentos": pd.read_parquet(paths["agendamentos"]),
            "presenca": pd.read_parquet(paths["presenca"]),
            "pendentes": pd.read_parquet(paths["pendentes"]),
        }
        st.session_state["loader_ok"] = True


def process_bases() -> None:
    ensure_folder(PROCESSADO_DIR)

    base_paths = {
        "estrutural": latest_file_with_prefix(Path("data/origem/Base_Estrutural"), "Base_Estrutural"),
        "agendamentos": latest_file_with_prefix(Path("data/origem/Alocacoes"), "Alocacoes"),
        "presenca": latest_file_with_prefix(Path("data/origem/Percentual_Presenca"), "Percentual_Presenca"),
        "pendentes": latest_file_with_prefix(Path("data/origem/Registros_Pendentes"), "Registros_Pendentes"),
    }

    faltantes = [nome for nome, caminho in base_paths.items() if caminho is None]
    if faltantes:
        st.warning("Nenhum arquivo encontrado para: " + ", ".join(faltantes) + ".")
        return

    df_estrutural, df_estrutural_normalizado = process_base_estrutural(base_paths["estrutural"])
    df_agendamentos = process_base_agendamentos(base_paths["agendamentos"])
    df_presenca_raw = pd.read_excel(base_paths["presenca"])
    df_presenca = process_base_presenca(df_presenca_raw, df_estrutural_normalizado)
    df_pendentes = process_base_pendentes(base_paths["pendentes"])

    # 1. Normalizar colunas (preserva presença para o schema final esperado)
    df_estrutural = normalize_columns(df_estrutural)
    df_agendamentos = normalize_columns(df_agendamentos)
    df_pendentes = normalize_columns(df_pendentes)

    # 2. Validar estrutura obrigatória
    validate_all_bases(df_estrutural, df_agendamentos, df_presenca)

    df_estrutural.to_parquet(PROCESSADO_DIR / "base_estrutural.parquet", index=False)
    df_estrutural_normalizado.to_parquet(
        PROCESSADO_DIR / "base_estrutural_normalizado.parquet", index=False
    )
    df_agendamentos.to_parquet(PROCESSADO_DIR / "base_agendamentos.parquet", index=False)
    df_presenca.to_parquet(PROCESSADO_DIR / "base_percentual_presenca.parquet", index=False)
    df_pendentes.to_parquet(PROCESSADO_DIR / "base_registros_pendentes.parquet", index=False)

    # Atualiza estado da sessão para os downloads
    st.session_state["loader_ok"] = True
    st.session_state["arquivos_processados"] = {
        "estrutural": df_estrutural,
        "agendamentos": df_agendamentos,
        "presenca": df_presenca,
        "pendentes": df_pendentes,
    }

    # Sincroniza com Firestore (não quebra a execução caso falhe)
    with st.spinner("Sincronizando dados com o Firestore..."):
        try:
            df_estrutural_firestore = normalize_columns(df_estrutural.copy())
            save_dataframe(df_estrutural_firestore, "siave_estrutural", "base_estrutural")
            df_agendamentos_firestore = normalize_columns(df_agendamentos.copy())
            save_dataframe(df_agendamentos_firestore, "siave_agendamentos", "base_agendamentos")
            df_presenca_firestore = normalize_columns(df_presenca.copy())
            save_dataframe(df_presenca_firestore, "siave_presenca", "base_percentual_presenca")
            df_pendentes_firestore = normalize_columns(df_pendentes.copy())
            save_dataframe(df_pendentes_firestore, "siave_pendencias", "base_pendencias")
            st.success("Sincronização com Firestore concluída.")
        except Exception as exc:
            st.warning(f"Não foi possível sincronizar com o Firestore: {exc}")


def render_upload_tab(title: str, prefix: str, folder: Path, folder_display: str) -> None:
    st.subheader(f"Upload de {title}")
    uploaded_file = st.file_uploader("Selecione o arquivo (.xlsx)", type=["xlsx"], accept_multiple_files=False, key=f"uploader_{prefix}")

    if uploaded_file is not None:
        saved_path = save_uploaded_file(uploaded_file, folder, prefix)
        st.success(f"Arquivo salvo em {folder_display} como {saved_path.name}")

    render_history(folder)
    render_delete_section(folder, prefix)


def render_loader_section() -> None:
    st.subheader("Executar Loader")
    if st.session_state.get("loader_ok"):
        st.success("Bases ja processadas. Utilize os botoes para download ou reprocese se necessario.")
    else:
        st.info("Clique em Executar Loader para processar os arquivos mais recentes.")

    if st.button("Executar Loader", type="primary"):
        with st.spinner("Processando bases..."):
            process_bases()

    if st.session_state.get("loader_ok") and st.session_state.get("arquivos_processados"):
        dados = st.session_state["arquivos_processados"]
        col1, col2 = st.columns(2)
        with col1:
            st.download_button(
                "Baixar base_estrutural.parquet",
                data=gerar_bytes_parquet(dados["estrutural"]),
                file_name="base_estrutural.parquet",
            )
            st.download_button(
                "Baixar base_agendamentos.parquet",
                data=gerar_bytes_parquet(dados["agendamentos"]),
                file_name="base_agendamentos.parquet",
            )
        with col2:
            st.download_button(
                "Baixar base_percentual_presenca.parquet",
                data=gerar_bytes_parquet(dados["presenca"]),
                file_name="base_percentual_presenca.parquet",
            )
            st.download_button(
                "Baixar base_registros_pendentes.parquet",
                data=gerar_bytes_parquet(dados["pendentes"]),
                file_name="base_registros_pendentes.parquet",
            )


def init_state() -> None:
    st.session_state.setdefault("loader_ok", False)
    st.session_state.setdefault("arquivos_processados", {})
    load_existing_outputs()


def main() -> None:
    init_state()

    if st.session_state.get("deleted_file"):
        st.session_state["deleted_file"] = False
        st.experimental_rerun()

    st.title("Base de Dados - Atualizacoes SIAVE 2025")
    st.caption("Area restrita para upload e processamento das bases utilizadas nos dashboards.")

    senha = st.text_input("Senha de acesso", type="password")
    if not senha:
        st.info("Digite a senha para liberar as acoes.")
        st.stop()
    if senha != PASSWORD_CORRECT:
        st.error("Senha incorreta")
        st.stop()

    abas = st.tabs(
        [
            "Base Estrutural",
            "Alocacoes",
            "Percentual de Presenca",
            "Registros Pendentes",
            "Executar Loader",
        ]
    )

    for tab, config in zip(abas, UPLOAD_CONFIGS):
        with tab:
            render_upload_tab(
                title=config["title"],
                prefix=config["prefix"],
                folder=config["folder"],
                folder_display=config["folder_display"],
            )

    with abas[-1]:
        render_loader_section()


if __name__ == "__main__":
    main()
