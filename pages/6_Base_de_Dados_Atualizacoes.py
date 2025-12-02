import io
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from src.base_estrutural_loader import normalize_col


PASSWORD_CORRECT = "A9C3B"

POLO_TO_GRE = {
    "JOAO PESSOA 01": "1a GRE",
    "JOAO PESSOA 02": "1a GRE",
    "JOAO PESSOA 03": "1a GRE",
    "JOAO PESSOA 04": "1a GRE",
    "JOAO PESSOA 05": "1a GRE",
    "JOAO PESSOA 06": "1a GRE",
    "JOAO PESSOA 07": "1a GRE",
    "GUARABIRA 01": "2a GRE",
    "GUARABIRA 02": "2a GRE",
    "GUARABIRA 03": "2a GRE",
    "GUARABIRA 04": "2a GRE",
    "CAMPINA GRANDE 01": "3a GRE",
    "CAMPINA GRANDE 02": "3a GRE",
    "CAMPINA GRANDE 03": "3a GRE",
    "CAMPINA GRANDE 04": "3a GRE",
    "CAMPINA GRANDE 05": "3a GRE",
    "CAMPINA GRANDE 06": "3a GRE",
    "CAMPINA GRANDE 07": "3a GRE",
    "CAMPINA GRANDE 08": "3a GRE",
    "CAMPINA GRANDE 09": "3a GRE",
    "CUITE 01": "4a GRE",
    "CUITE 02": "4a GRE",
    "MONTEIRO 01": "5a GRE",
    "MONTEIRO 02": "5a GRE",
    "PATOS 01": "6a GRE",
    "PATOS 02": "6a GRE",
    "PATOS 03": "6a GRE",
    "ITAPORANGA 01": "7a GRE",
    "ITAPORANGA 02": "7a GRE",
    "CATOLE 01": "8a GRE",
    "CATOLE 02": "8a GRE",
    "CAJAZEIRAS 01": "9a GRE",
    "CAJAZEIRAS 02": "9a GRE",
    "SOUSA 01": "10a GRE",
    "SOUSA 02": "10a GRE",
    "PRINCESA ISABEL": "11a GRE",
    "ITABAIANA 01": "12a GRE",
    "ITABAIANA 02": "12a GRE",
    "POMBAL": "13a GRE",
    "MAMANGUAPE 01": "14a GRE",
    "MAMANGUAPE 02": "14a GRE",
    "QUEIMADAS 01": "15a GRE",
    "QUEIMADAS 02": "15a GRE",
    "QUEMADAS 03": "15a GRE",
    "SANTA RITA 01": "16a GRE",
    "SANTA RITA 02": "16a GRE",
    "SANTA RITA 03": "16a GRE",
    "SANTA RITA 04": "16a GRE",
    "SANTA RITA 05": "16a GRE",
    "SANTA RITA 06": "16a GRE",
    "SANTA RITA 07": "16a GRE",
}

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

PROCESSADO_DIR = Path("data/processado")


def ensure_folder(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def format_timestamp_brazil(dt: datetime) -> str:
    brasil = dt - timedelta(hours=3)
    return brasil.strftime("%d/%m/%Y %H:%M:%S")


def build_timestamped_filename(prefix: str, now: datetime) -> str:
    timestamp = now.strftime("%Y-%m-%dT%H_%M_%S.%f")[:23]
    return f"{prefix}-{timestamp}Z.xlsx"


def save_uploaded_file(uploaded_file, folder: Path, prefix: str) -> tuple[Path, datetime]:
    ensure_folder(folder)
    now = datetime.utcnow()
    filename = build_timestamped_filename(prefix, now)
    destination = folder / filename
    destination.write_bytes(uploaded_file.getbuffer())
    return destination, now


def list_files(folder: Path) -> list[Path]:
    ensure_folder(folder)
    return sorted(folder.glob("*.xlsx"), key=lambda f: f.stat().st_mtime, reverse=True)


def render_history(folder: Path) -> None:
    st.markdown("**Historico dos ultimos 5 arquivos**")
    files = list_files(folder)[:5]
    if not files:
        st.info("Nenhum arquivo encontrado.")
        return
    data = []
    for file in files:
        created = datetime.fromtimestamp(file.stat().st_mtime)
        data.append(
            {
                "Arquivo": file.name,
                "Criado em (Brasil)": format_timestamp_brazil(created),
            }
        )
    st.table(data)


def render_delete_section(folder: Path, prefix: str) -> None:
    files = list_files(folder)
    if not files:
        st.info("Nenhum arquivo disponivel para exclusao.")
        return

    options = [file.name for file in files]
    selected = st.selectbox(
        "Selecione um arquivo para excluir",
        options,
        key=f"delete_select_{prefix}",
    )
    if st.button("Excluir arquivo selecionado", key=f"delete_button_{prefix}"):
        target = folder / selected
        if target.exists():
            target.unlink()
            st.success("Arquivo removido com sucesso.")
            st.experimental_rerun()


def remove_accents(text):
    if text is None:
        return text
    if not isinstance(text, str):
        return text
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join([c for c in nfkd if not unicodedata.combining(c)])


def normalize_upper(text):
    text = remove_accents(text)
    return str(text).upper().strip()


def latest_file_with_prefix(folder: Path, prefix: str) -> Path | None:
    ensure_folder(folder)
    pattern = f"{prefix}-*.xlsx"
    arquivos = sorted(folder.glob(pattern), key=lambda f: f.stat().st_mtime)
    return arquivos[-1] if arquivos else None


def normalizar_nome(nome):
    n = unicodedata.normalize("NFKD", str(nome))
    n = "".join(c for c in n if not unicodedata.combining(c))
    return "".join(ch.lower() for ch in n if ch.isalnum())


def gerar_bytes_parquet(df):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    return buffer.getvalue()


def process_bases():
    ensure_folder(PROCESSADO_DIR)

    base_estrutural = latest_file_with_prefix(Path("data/origem/Base_Estrutural"), "Base_Estrutural")
    base_alocacoes = latest_file_with_prefix(Path("data/origem/Alocacoes"), "Alocacoes")
    base_presenca = latest_file_with_prefix(Path("data/origem/Percentual_Presenca"), "Percentual_Presenca")
    base_pendentes = latest_file_with_prefix(Path("data/origem/Registros_Pendentes"), "Registros_Pendentes")

    faltantes = []
    if base_estrutural is None:
        faltantes.append("Base Estrutural")
    if base_alocacoes is None:
        faltantes.append("Base de Alocacoes")
    if base_presenca is None:
        faltantes.append("Base de Presenca")
    if base_pendentes is None:
        faltantes.append("Base de Registros Pendentes")

    if faltantes:
        st.warning("Nenhum arquivo encontrado para: " + ", ".join(faltantes) + ".")
        return

    # Base Estrutural
    df_estrutural = pd.read_excel(base_estrutural)
    if "Municipio" in df_estrutural.columns and "municipio" not in df_estrutural.columns:
        df_estrutural = df_estrutural.rename(columns={"Municipio": "municipio"})
    if "municipio" in df_estrutural.columns:
        df_estrutural["municipio"] = df_estrutural["municipio"].apply(normalize_upper)

    df_estrutural["Polo_normalizado"] = (
        df_estrutural["Polo"]
        .astype(str)
        .str.normalize("NFKD")
        .str.encode("ascii", "ignore")
        .str.decode("ascii")
        .str.upper()
        .str.strip()
    )
    df_estrutural["gRE"] = df_estrutural["Polo_normalizado"].map(POLO_TO_GRE)
    df_estrutural["gRE"] = df_estrutural["gRE"].fillna("GRE NAO IDENTIFICADA")
    df_estrutural["gRE"] = df_estrutural["gRE"].apply(normalize_upper)
    df_estrutural = df_estrutural.drop(columns=["Polo_normalizado"])
    df_estrutural.columns = [remove_accents(c) for c in df_estrutural.columns]
    for col in df_estrutural.select_dtypes(include=["object"]).columns:
        df_estrutural[col] = df_estrutural[col].apply(remove_accents)
    df_estrutural.to_parquet(PROCESSADO_DIR / "base_estrutural.parquet", index=False)
    st.download_button(
        label="⬇️ Baixar arquivo processado",
        data=gerar_bytes_parquet(df_estrutural),
        file_name="base_estrutural.parquet",
        mime="application/octet-stream",
    )

    df_estrutural_normalizado = df_estrutural.copy()
    df_estrutural_normalizado.columns = [normalize_col(c) for c in df_estrutural_normalizado.columns]
    df_estrutural_normalizado.to_parquet(
        PROCESSADO_DIR / "base_estrutural_normalizado.parquet", index=False
    )
    st.success("Base Estrutural processada com sucesso.")

    # Base de Agendamentos
    df_alocacoes = pd.read_excel(base_alocacoes)
    norm_map = {normalizar_nome(c): c for c in df_alocacoes.columns}

    if "dataagendmento" in norm_map:
        df_alocacoes = df_alocacoes.rename(columns={norm_map["dataagendmento"]: "dataAgendamento"})
    norm_map = {normalizar_nome(c): c for c in df_alocacoes.columns}

    if "coescolacenso" in norm_map:
        df_alocacoes = df_alocacoes.rename(columns={norm_map["coescolacenso"]: "coEscolaCenso"})
    elif "codigoescola" in norm_map:
        df_alocacoes = df_alocacoes.rename(columns={norm_map["codigoescola"]: "coEscolaCenso"})
    if "coEscolaCenso" not in df_alocacoes.columns:
        alt_col = norm_map.get("codigoescola") or norm_map.get("coescolacenso")
        df_alocacoes["coEscolaCenso"] = df_alocacoes[alt_col] if alt_col else pd.NA
    df_alocacoes["coEscolaCenso"] = df_alocacoes["coEscolaCenso"].astype(str).str.strip()

    municipio_col = norm_map.get("municipio") or norm_map.get("municipioescola") or norm_map.get("municipioescol")
    if municipio_col:
        df_alocacoes = df_alocacoes.rename(columns={municipio_col: "municipio"})
    if "municipio" not in df_alocacoes.columns:
        df_alocacoes["municipio"] = pd.NA
    df_alocacoes["municipio"] = df_alocacoes["municipio"].apply(normalize_upper)

    def pick_col(possiveis):
        norm_map_local = {normalizar_nome(c): c for c in df_alocacoes.columns}
        for key in possiveis:
            if key in norm_map_local:
                return norm_map_local[key]
        return None

    def serie_texto(possiveis):
        col = pick_col(possiveis)
        if col is None:
            return pd.Series(pd.NA, index=df_alocacoes.index, dtype="string")
        return df_alocacoes[col].astype("string").str.strip()

    def serie_numero(possiveis):
        col = pick_col(possiveis)
        if col is None:
            return pd.Series(pd.NA, index=df_alocacoes.index, dtype="Int64")
        return pd.to_numeric(df_alocacoes[col], errors="coerce").astype("Int64")

    def limpar_vazios(series: pd.Series) -> pd.Series:
        return series.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

    df_agendamentos_norm = pd.DataFrame(index=df_alocacoes.index)
    df_agendamentos_norm["escola"] = limpar_vazios(serie_texto(["escola", "nomeescola"]))
    df_agendamentos_norm["municipio"] = df_alocacoes["municipio"]
    df_agendamentos_norm["polo"] = limpar_vazios(serie_texto(["polo"]))
    df_agendamentos_norm["coEscolaCenso"] = df_alocacoes["coEscolaCenso"]
    df_agendamentos_norm["serie"] = limpar_vazios(serie_texto(["serie", "serieano"]))
    df_agendamentos_norm["turno"] = limpar_vazios(serie_texto(["turno"]))
    df_agendamentos_norm["turma"] = limpar_vazios(serie_texto(["turma"]))

    df_agendamentos_norm["qtdAlunosPrevistos"] = serie_numero(
        ["alocados", "qtdalunos", "qtdalunosprevistos"]
    )

    dia_aplicacao = serie_texto(
        [
            "diaaplicacao",
            "dia",
            "aplicacao",
            "dataaplicacao",
            "diaprova",
            "diateste",
        ]
    )
    df_agendamentos_norm["diaAplicacao"] = limpar_vazios(dia_aplicacao).astype(str).str.strip()

    data_agendamento = serie_texto(
        ["dataagendamento", "dataagendmento", "agendamento", "dataaplicacao"]
    )
    df_agendamentos_norm["dataAgendamento"] = pd.to_datetime(
        data_agendamento, dayfirst=True, errors="coerce"
    )
    if "dataAplicacaoReal" in df_alocacoes.columns:
        df_agendamentos_norm["dataAplicacaoReal"] = pd.to_datetime(
            df_alocacoes["dataAplicacaoReal"], dayfirst=True, errors="coerce"
        )
    else:
        df_agendamentos_norm["dataAplicacaoReal"] = df_agendamentos_norm["dataAgendamento"]

    polo_normalizado = (
        df_agendamentos_norm["polo"]
        .fillna("")
        .str.normalize("NFKD")
        .str.encode("ascii", "ignore")
        .str.decode("ascii")
        .str.upper()
        .str.strip()
    )
    df_agendamentos_norm["gRE"] = polo_normalizado.map(POLO_TO_GRE)
    df_agendamentos_norm["gRE"] = df_agendamentos_norm["gRE"].fillna("GRE NAO IDENTIFICADA")
    df_agendamentos_norm["gRE"] = df_agendamentos_norm["gRE"].str.upper().str.strip()
    df_agendamentos_norm["GRE"] = df_agendamentos_norm["gRE"]

    required_ag_cols = [
        "coEscolaCenso",
        "escola",
        "municipio",
        "polo",
        "gRE",
        "dataAgendamento",
        "dataAplicacaoReal",
        "qtdAlunosPrevistos",
        "diaAplicacao",
        "turno",
        "serie",
        "turma",
    ]
    for col in required_ag_cols:
        if col not in df_agendamentos_norm.columns:
            df_agendamentos_norm[col] = pd.NA
    df_agendamentos_norm = df_agendamentos_norm[required_ag_cols + ["GRE"]]

    df_agendamentos_norm.to_parquet(PROCESSADO_DIR / "base_agendamentos.parquet", index=False)
    st.download_button(
        label="⬇️ Baixar arquivo processado",
        data=gerar_bytes_parquet(df_agendamentos_norm),
        file_name="base_agendamentos.parquet",
        mime="application/octet-stream",
    )
    st.success("Base de Alocacoes processada com sucesso.")

    # Percentual de Presenca
    df_presenca = pd.read_excel(base_presenca)
    norm_map_presenca = {normalizar_nome(c): c for c in df_presenca.columns}

    if "codigoescola" in norm_map_presenca:
        df_presenca = df_presenca.rename(columns={norm_map_presenca["codigoescola"]: "coEscolaCenso"})
    if "coEscolaCenso" not in df_presenca.columns:
        df_presenca["coEscolaCenso"] = pd.NA
    df_presenca["coEscolaCenso"] = df_presenca["coEscolaCenso"].astype(str).str.strip()

    if "municipio" not in df_presenca.columns:
        df_presenca["municipio"] = None

    if "gre" in norm_map_presenca:
        df_presenca = df_presenca.rename(columns={norm_map_presenca["gre"]: "gRE"})
    if "gRE" not in df_presenca.columns:
        df_presenca["gRE"] = pd.NA
    df_presenca["gRE"] = df_presenca["gRE"].apply(normalize_upper)
    df_presenca["GRE"] = df_presenca["gRE"]

    if "datareal" in norm_map_presenca:
        df_presenca = df_presenca.rename(columns={norm_map_presenca["datareal"]: "dataAplicacaoReal"})
    if "dataAplicacaoReal" in df_presenca.columns:
        df_presenca["dataAplicacaoReal"] = pd.to_datetime(
            df_presenca["dataAplicacaoReal"], dayfirst=True, errors="coerce"
        )
    else:
        df_presenca["dataAplicacaoReal"] = pd.NaT

    if "diaaplicacao" in norm_map_presenca:
        df_presenca = df_presenca.rename(columns={norm_map_presenca["diaaplicacao"]: "diaAplicacao"})
    if "diaAplicacao" not in df_presenca.columns:
        df_presenca["diaAplicacao"] = pd.NA
    df_presenca["diaAplicacao"] = df_presenca["diaAplicacao"].astype(str).str.strip()

    if "percentual" not in df_presenca.columns:
        df_presenca["percentual"] = pd.NA
    df_presenca["percentual"] = pd.to_numeric(df_presenca["percentual"], errors="coerce")

    if "qtdAlunosPrevistos" not in df_presenca.columns:
        df_presenca["qtdAlunosPrevistos"] = pd.NA
    if "qtdAlunosPresentes" not in df_presenca.columns:
        df_presenca["qtdAlunosPresentes"] = pd.NA
    df_presenca["qtdAlunosPrevistos"] = pd.to_numeric(
        df_presenca["qtdAlunosPrevistos"], errors="coerce"
    ).astype("Int64")
    df_presenca["qtdAlunosPresentes"] = pd.to_numeric(
        df_presenca["qtdAlunosPresentes"], errors="coerce"
    ).astype("Int64")

    norm_map_estrutural = {normalizar_nome(c): c for c in df_estrutural.columns}

    if "coEscolaCenso" not in df_estrutural.columns:
        alt_co = norm_map_estrutural.get("coescolacenso")
        df_estrutural["coEscolaCenso"] = df_estrutural[alt_co] if alt_co else pd.NA
    if "escola" not in df_estrutural.columns:
        alt_escola = norm_map_estrutural.get("escola")
        df_estrutural["escola"] = df_estrutural[alt_escola] if alt_escola else pd.NA
    if "polo" not in df_estrutural.columns:
        alt_polo = norm_map_estrutural.get("polo")
        df_estrutural["polo"] = df_estrutural[alt_polo] if alt_polo else pd.NA

    df_estrutural["coEscolaCenso"] = df_estrutural["coEscolaCenso"].astype(str).str.strip()

    df_presenca = df_presenca.merge(
        df_estrutural[["coEscolaCenso", "escola", "polo"]],
        on="coEscolaCenso",
        how="left",
    )

    for col in ["escola", "polo", "municipio", "gRE"]:
        if col not in df_presenca.columns:
            df_presenca[col] = pd.NA

    df_presence_norm = df_presenca[
        [
            "coEscolaCenso",
            "escola",
            "municipio",
            "polo",
            "gRE",
            "qtdAlunosPrevistos",
            "qtdAlunosPresentes",
            "percentual",
            "diaAplicacao",
            "dataAplicacaoReal",
        ]
    ].copy()

    df_presence_norm.to_parquet(PROCESSADO_DIR / "base_percentual_presenca.parquet", index=False)
    st.download_button(
        label="⬇️ Baixar arquivo processado",
        data=gerar_bytes_parquet(df_presence_norm),
        file_name="base_percentual_presenca.parquet",
        mime="application/octet-stream",
    )
    st.success("Base de Presenca processada com sucesso.")

    # Registros Pendentes
    df_pendentes = pd.read_excel(base_pendentes)
    df_pendentes.to_parquet(PROCESSADO_DIR / "base_registros_pendentes.parquet", index=False)
    st.download_button(
        label="⬇️ Baixar arquivo processado",
        data=gerar_bytes_parquet(df_pendentes),
        file_name="base_registros_pendentes.parquet",
        mime="application/octet-stream",
    )
    st.success("Base de Registros Pendentes processada com sucesso.")

    st.success("Loader padronizado com sucesso (Estrutural, Agendamentos e Presenca).")


def render_upload_tab(title: str, prefix: str, folder: Path, folder_display: str) -> None:
    st.subheader(f"Upload de {title}")
    uploaded_file = st.file_uploader(
        "Selecione o arquivo (.xlsx)",
        type=["xlsx"],
        accept_multiple_files=False,
        key=f"uploader_{prefix}",
    )

    if uploaded_file is not None:
        saved_path, saved_time = save_uploaded_file(uploaded_file, folder, prefix)
        dt_br = format_timestamp_brazil(saved_time)
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
📂 Pasta: {folder_display}<br>
📄 Arquivo salvo: {saved_path.name}<br>
⏰ Enviado em: {dt_br}
</div>
""",
            unsafe_allow_html=True,
        )

    render_history(folder)
    render_delete_section(folder, prefix)


st.title("Base de Dados – Atualizacoes SIAVE 2025")
st.caption(
    "Pagina restrita a equipe tecnica. Use esta interface para atualizar as bases que alimentam os dashboards."
)

senha = st.text_input("Senha de acesso", type="password")

if not senha:
    st.info("Digite a senha para liberar os envios.")
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
    st.subheader("Executar Loader")
    if st.button("Processar Bases e Gerar Arquivos .parquet"):
        process_bases()
