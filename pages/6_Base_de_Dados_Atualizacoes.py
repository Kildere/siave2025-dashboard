from pathlib import Path
from datetime import datetime, timedelta
import unicodedata

import pandas as pd
import streamlit as st
from src.base_estrutural_loader import normalize_col


PASSWORD_CORRECT = "A9C3B"

UPLOAD_CONFIGS = [
    {
        "tab": "Base Estrutural",
        "title": "Base Estrutural",
        "prefix": "Base_Estrutural",
        "folder": Path("data/origem/Base_Estrutural"),
        "folder_display": "data/origem/Base_Estrutural",
    },
    {
        "tab": "Aloca\u00e7\u00f5es",
        "title": "Aloca\u00e7\u00f5es",
        "prefix": "Alocacoes",
        "folder": Path("data/origem/Alocacoes"),
        "folder_display": "data/origem/Alocacoes",
    },
    {
        "tab": "Percentual de Presen\u00e7a",
        "title": "Percentual de Presen\u00e7a",
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
    st.markdown("**Hist\u00f3rico dos \u00faltimos 5 arquivos**")
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
        st.info("Nenhum arquivo dispon\u00edvel para exclus\u00e3o.")
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


def latest_file_with_prefix(folder: Path, prefix: str) -> Path | None:
    ensure_folder(folder)
    pattern = f"{prefix}-*.xlsx"
    arquivos = sorted(folder.glob(pattern), key=lambda f: f.stat().st_mtime)
    return arquivos[-1] if arquivos else None


def normalizar_nome(nome):
    import unicodedata
    n = unicodedata.normalize("NFKD", str(nome))
    n = "".join(c for c in n if not unicodedata.combining(c))
    return "".join(ch.lower() for ch in n if ch.isalnum())


def process_bases():
    ensure_folder(PROCESSADO_DIR)

    def padronizar_colunas_obrigatorias(df: pd.DataFrame) -> pd.DataFrame:
        alvos = {
            "gre": "gRE",
            "coescolacenso": "coEscolaCenso",
            "diaaplicacao": "diaAplicacao",
        }
        normalizados = {}
        for coluna in df.columns:
            chave = normalizar_nome(coluna)
            if chave not in normalizados:
                normalizados[chave] = coluna

        renomear = {}
        for chave, nome_final in alvos.items():
            if nome_final in df.columns:
                continue
            origem = normalizados.get(chave)
            if origem:
                renomear[origem] = nome_final
        if renomear:
            df = df.rename(columns=renomear)

        for nome_final in alvos.values():
            if nome_final not in df.columns:
                df[nome_final] = None
        return df

    base_estrutural = latest_file_with_prefix(Path("data/origem/Base_Estrutural"), "Base_Estrutural")
    base_alocacoes = latest_file_with_prefix(Path("data/origem/Alocacoes"), "Alocacoes")
    base_presenca = latest_file_with_prefix(Path("data/origem/Percentual_Presenca"), "Percentual_Presenca")
    base_pendentes = latest_file_with_prefix(Path("data/origem/Registros_Pendentes"), "Registros_Pendentes")

    faltantes = []
    if base_estrutural is None:
        faltantes.append("Base Estrutural")
    if base_alocacoes is None:
        faltantes.append("Base de Aloca\u00e7\u00f5es")
    if base_presenca is None:
        faltantes.append("Base de Presen\u00e7a")
    if base_pendentes is None:
        faltantes.append("Base de Registros Pendentes")

    if faltantes:
        st.warning("Nenhum arquivo encontrado para: " + ", ".join(faltantes) + ".")
        return

    # Base Estrutural
    df_estrutural = pd.read_excel(base_estrutural)
    # -----------------------------------------------------------
    # MAPEAR coluna gRE na base estrutural
    # -----------------------------------------------------------
    norm_map = {normalizar_nome(c): c for c in df_estrutural.columns}

    possiveis_gre = [
        "gre",
        "regional",
        "gerenciaregional",
        "gerenciaderegional",
        "numgre",
        "nregional",
        "gerencia",
        "grecodigo",
        "codigogre",
    ]

    col_gre = None
    for key in possiveis_gre:
        if key in norm_map:
            col_gre = norm_map[key]
            break

    if col_gre is not None:
        df_estrutural["gRE"] = df_estrutural[col_gre]
    else:
        # fallback seguro sem quebrar o app
        df_estrutural["gRE"] = "Sem GRE mapeada"
    df_estrutural = padronizar_colunas_obrigatorias(df_estrutural)
    df_estrutural.columns = [remove_accents(c) for c in df_estrutural.columns]
    for col in df_estrutural.select_dtypes(include=["object"]).columns:
        df_estrutural[col] = df_estrutural[col].apply(remove_accents)
    df_estrutural.to_parquet(PROCESSADO_DIR / "base_estrutural.parquet", index=False)

    df_estrutural_normalizado = df_estrutural.copy()
    df_estrutural_normalizado.columns = [normalize_col(c) for c in df_estrutural_normalizado.columns]
    df_estrutural_normalizado.to_parquet(
        PROCESSADO_DIR / "base_estrutural_normalizado.parquet", index=False
    )
    st.success("Base Estrutural processada com sucesso.")

    # Base de Aloca\u00e7\u00f5es
    df_alocacoes = pd.read_excel(base_alocacoes)
    # MAPEAR col coEscolaCenso
    norm_map = {normalizar_nome(c): c for c in df_alocacoes.columns}

    possiveis_coesc = [
        "coescolacenso",
        "codigoescola",
        "codigocenso",
        "codescola",
        "escolacod",
        "escod",
        "id_escola",
        "id_escolacenso",
    ]

    col_codigo = None
    for key in possiveis_coesc:
        if key in norm_map:
            col_codigo = norm_map[key]
            break

    if col_codigo is not None:
        df_alocacoes["coEscolaCenso"] = df_alocacoes[col_codigo]
    else:
        df_alocacoes["coEscolaCenso"] = pd.NA

    # MAPEAR col diaAplicacao
    possiveis_dia = [
        "diaaplicacao",
        "dia",
        "aplicacao",
        "dataaplicacao",
        "diaprova",
        "diateste",
    ]

    col_dia = None
    for key in possiveis_dia:
        if key in norm_map:
            col_dia = norm_map[key]
            break

    if col_dia is not None:
        df_alocacoes["diaAplicacao"] = df_alocacoes[col_dia]
    else:
        df_alocacoes["diaAplicacao"] = pd.NA
    df_alocacoes = padronizar_colunas_obrigatorias(df_alocacoes)
    df_alocacoes.to_parquet(PROCESSADO_DIR / "base_agendamentos.parquet", index=False)
    st.success("Base de Aloca\u00e7\u00f5es processada com sucesso.")

    # Percentual de Presen\u00e7a
    df_presenca = pd.read_excel(base_presenca)
    # -----------------------------------------------------------
    # Garantir coEscolaCenso na base de aplicações
    # -----------------------------------------------------------
    norm_map = {normalizar_nome(c): c for c in df_presenca.columns}

    possiveis_coesc = [
        "coescolacenso",
        "codigoescola",
        "codigocenso",
        "codescola",
        "escolacod",
        "escod",
        "id_escola",
        "id_escolacenso",
    ]

    col_codigo = None
    for key in possiveis_coesc:
        if key in norm_map:
            col_codigo = norm_map[key]
            break

    if col_codigo is not None:
        df_presenca["coEscolaCenso"] = df_presenca[col_codigo]
    else:
        df_presenca["coEscolaCenso"] = pd.NA

    # -----------------------------------------------------------
    # Garantir diaAplicacao
    # -----------------------------------------------------------
    possiveis_dia = [
        "diaaplicacao",
        "dia",
        "aplicacao",
        "dataaplicacao",
        "diaprova",
        "diateste",
    ]

    col_dia = None
    for key in possiveis_dia:
        if key in norm_map:
            col_dia = norm_map[key]
            break

    if col_dia is not None:
        df_presenca["diaAplicacao"] = df_presenca[col_dia]
    else:
        df_presenca["diaAplicacao"] = pd.NA
    df_presenca = padronizar_colunas_obrigatorias(df_presenca)
    df_presenca.to_parquet(PROCESSADO_DIR / "base_aplicacoes.parquet", index=False)
    st.success("Base de Presen\u00e7a processada com sucesso.")

    # Registros Pendentes
    df_pendentes = pd.read_excel(base_pendentes)
    df_pendentes.to_parquet(PROCESSADO_DIR / "base_registros_pendentes.parquet", index=False)
    st.success("Base de Registros Pendentes processada com sucesso.")


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
\U0001F4C2 Pasta: {folder_display}<br>
\U0001F4C4 Arquivo salvo: {saved_path.name}<br>
\U0001F553 Enviado em: {dt_br}
</div>
""",
            unsafe_allow_html=True,
        )

    render_history(folder)
    render_delete_section(folder, prefix)


st.title("Base de Dados \u2013 Atualiza\u00e7\u00f5es SIAVE 2025")
st.caption(
    "P\u00e1gina restrita \u00e0 equipe t\u00e9cnica. Use esta interface para atualizar as bases que alimentam os dashboards."
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
        "Aloca\u00e7\u00f5es",
        "Percentual de Presen\u00e7a",
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
