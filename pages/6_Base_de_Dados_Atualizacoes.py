import io
from pathlib import Path
from datetime import datetime, timedelta
import unicodedata

import pandas as pd
import streamlit as st
from src.base_estrutural_loader import normalize_col


PASSWORD_CORRECT = "A9C3B"

POLO_TO_GRE = {
    "JOAO PESSOA 01": "1\u00aa GRE",
    "JOAO PESSOA 02": "1\u00aa GRE",
    "JOAO PESSOA 03": "1\u00aa GRE",
    "JOAO PESSOA 04": "1\u00aa GRE",
    "JOAO PESSOA 05": "1\u00aa GRE",
    "JOAO PESSOA 06": "1\u00aa GRE",
    "JOAO PESSOA 07": "1\u00aa GRE",

    "GUARABIRA 01": "2\u00aa GRE",
    "GUARABIRA 02": "2\u00aa GRE",
    "GUARABIRA 03": "2\u00aa GRE",
    "GUARABIRA 04": "2\u00aa GRE",

    "CAMPINA GRANDE 01": "3\u00aa GRE",
    "CAMPINA GRANDE 02": "3\u00aa GRE",
    "CAMPINA GRANDE 03": "3\u00aa GRE",
    "CAMPINA GRANDE 04": "3\u00aa GRE",
    "CAMPINA GRANDE 05": "3\u00aa GRE",
    "CAMPINA GRANDE 06": "3\u00aa GRE",
    "CAMPINA GRANDE 07": "3\u00aa GRE",
    "CAMPINA GRANDE 08": "3\u00aa GRE",
    "CAMPINA GRANDE 09": "3\u00aa GRE",

    "CUIT\u00c9 01": "4\u00aa GRE",
    "CUIT\u00c9 02": "4\u00aa GRE",

    "MONTEIRO 01": "5\u00aa GRE",
    "MONTEIRO 02": "5\u00aa GRE",

    "PATOS 01": "6\u00aa GRE",
    "PATOS 02": "6\u00aa GRE",
    "PATOS 03": "6\u00aa GRE",

    "ITAPORANGA 01": "7\u00aa GRE",
    "ITAPORANGA 02": "7\u00aa GRE",

    "CATOL\u00c9 01": "8\u00aa GRE",
    "CATOL\u00c9 02": "8\u00aa GRE",

    "CAJAZEIRAS 01": "9\u00aa GRE",
    "CAJAZEIRAS 02": "9\u00aa GRE",

    "SOUSA 01": "10\u00aa GRE",
    "SOUSA 02": "10\u00aa GRE",

    "PRINCESA ISABEL": "11\u00aa GRE",

    "ITABAIANA 01": "12\u00aa GRE",
    "ITABAIANA 02": "12\u00aa GRE",

    "POMBAL": "13\u00aa GRE",

    "MAMANGUAPE 01": "14\u00aa GRE",
    "MAMANGUAPE 02": "14\u00aa GRE",

    "QUEIMADAS 01": "15\u00aa GRE",
    "QUEIMADAS 02": "15\u00aa GRE",
    "QUEMADAS 03": "15\u00aa GRE",

    "SANTA RITA 01": "16\u00aa GRE",
    "SANTA RITA 02": "16\u00aa GRE",
    "SANTA RITA 03": "16\u00aa GRE",
    "SANTA RITA 04": "16\u00aa GRE",
    "SANTA RITA 05": "16\u00aa GRE",
    "SANTA RITA 06": "16\u00aa GRE",
    "SANTA RITA 07": "16\u00aa GRE",
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

def gerar_bytes_parquet(df):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    return buffer.getvalue()


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
    # Normalizar nomes para evitar diferencas de acentuacao
    df_estrutural["Polo_normalizado"] = (
        df_estrutural["Polo"]
        .astype(str)
        .str.normalize("NFKD")
        .str.encode("ascii", "ignore")
        .str.decode("ascii")
        .str.upper()
        .str.strip()
    )

    # Aplicar mapeamento POLO -> GRE
    df_estrutural["gRE"] = df_estrutural["Polo_normalizado"].map(POLO_TO_GRE)

    # Caso algum polo nao esteja no dicionario
    df_estrutural["gRE"] = df_estrutural["gRE"].fillna("GRE NAO IDENTIFICADA")

    # Remover coluna auxiliar
    df_estrutural = df_estrutural.drop(columns=["Polo_normalizado"])
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
    # -------------------------
    # Botão para baixar parquet
    # -------------------------
    st.download_button(
        label="⬇️ Baixar arquivo processado",
        data=gerar_bytes_parquet(df_estrutural),
        file_name="base_estrutural.parquet",
        mime="application/octet-stream"
    )

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

    def pick_col(possiveis):
        for key in possiveis:
            if key in norm_map:
                return norm_map[key]
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

    df_agendamentos_norm = pd.DataFrame(index=df_alocacoes.index)
    df_agendamentos_norm["uf"] = limpar_vazios(serie_texto(["uf", "estado"]))
    df_agendamentos_norm["polo"] = limpar_vazios(serie_texto(["polo"]))
    df_agendamentos_norm["coEscolaCenso"] = limpar_vazios(serie_texto(possiveis_coesc))
    df_agendamentos_norm["escola"] = limpar_vazios(serie_texto(["escola", "nomeescola"]))

    municipio_escola = serie_texto(["municipioescola", "municipio"])
    municipio_polo = serie_texto(["municipiopolo"])
    municipio_comb = municipio_escola.where(
        municipio_escola.notna() & (municipio_escola != ""),
        municipio_polo,
    )
    df_agendamentos_norm["municipio"] = limpar_vazios(municipio_comb)

    df_agendamentos_norm["serie"] = limpar_vazios(serie_texto(["serie", "serieano"]))
    df_agendamentos_norm["turno"] = limpar_vazios(serie_texto(["turno"]))
    df_agendamentos_norm["turma"] = limpar_vazios(serie_texto(["turma"]))
    df_agendamentos_norm["coTurmaCenso"] = limpar_vazios(
        serie_texto(["coturmacenso", "turmacenso", "codturmacenso", "idturma"])
    )
    df_agendamentos_norm["tipoAplic"] = limpar_vazios(
        serie_texto(["tipoaplic", "tipoaplicacao"])
    )
    df_agendamentos_norm["statusAplicacao"] = limpar_vazios(
        serie_texto(["statusaplicacao", "status"])
    )
    df_agendamentos_norm["localizacao"] = limpar_vazios(serie_texto(["localizacao"]))
    df_agendamentos_norm["tipoRede"] = limpar_vazios(serie_texto(["tiporede", "rede"]))
    df_agendamentos_norm["aplicador"] = limpar_vazios(serie_texto(["aplicador"]))
    df_agendamentos_norm["cpf"] = limpar_vazios(serie_texto(["cpf", "cpfaplicador"]))

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
    df_agendamentos_norm["diaAplicacao"] = limpar_vazios(dia_aplicacao)

    data_agendamento = serie_texto(
        ["dataagendamento", "dataagendmento", "agendamento", "dataaplicacao"]
    )
    df_agendamentos_norm["dataAgendmento"] = pd.to_datetime(
        data_agendamento, dayfirst=True, errors="coerce"
    )

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

    chave_aplicacao = df_agendamentos_norm["coTurmaCenso"].where(
        df_agendamentos_norm["coTurmaCenso"].notna() & (df_agendamentos_norm["coTurmaCenso"] != ""),
        df_agendamentos_norm["coEscolaCenso"],
    )
    df_agendamentos_norm["aplicacaoId"] = (
        chave_aplicacao.fillna("").astype(str).str.strip()
        + "_"
        + df_agendamentos_norm["diaAplicacao"].fillna("").astype(str).str.strip()
    ).str.strip("_")
    df_agendamentos_norm["aplicacaoId"] = df_agendamentos_norm["aplicacaoId"].replace("", pd.NA)

    df_agendamentos_norm.to_parquet(PROCESSADO_DIR / "base_agendamentos.parquet", index=False)
    # -------------------------
    # Botão para baixar parquet
    # -------------------------
    st.download_button(
        label="⬇️ Baixar arquivo processado",
        data=gerar_bytes_parquet(df_agendamentos_norm),
        file_name="base_agendamentos.parquet",
        mime="application/octet-stream"
    )
    st.success("Base de Aloca\u00e7\u00f5es processada com sucesso.")


    # Percentual de Presença
    df_presenca = pd.read_excel(base_presenca)
    norm_map_presenca = {normalizar_nome(c): c for c in df_presenca.columns}

    def pick_col_presenca(possiveis):
        for key in possiveis:
            if key in norm_map_presenca:
                return norm_map_presenca[key]
        return None

    def serie_texto_presenca(possiveis):
        col = pick_col_presenca(possiveis)
        if col is None:
            return pd.Series(pd.NA, index=df_presenca.index, dtype="string")
        return df_presenca[col].astype("string").str.strip()

    df_presence_norm = pd.DataFrame(index=df_presenca.index)
    df_presence_norm["uf"] = limpar_vazios(serie_texto_presenca(["uf", "estado"]))
    df_presence_norm["polo"] = limpar_vazios(serie_texto_presenca(["polo"]))
    df_presence_norm["tipoRede"] = limpar_vazios(serie_texto_presenca(["tiporede", "rede"]))
    df_presence_norm["localizacao"] = limpar_vazios(serie_texto_presenca(["localizacao"]))
    df_presence_norm["coEscolaCenso"] = limpar_vazios(serie_texto_presenca(
        [
            "coescolacenso",
            "codigoescola",
            "codigocenso",
            "codescola",
            "escolacod",
            "escod",
            "id_escola",
            "id_escolacenso",
        ]
    ))
    df_presence_norm["escola"] = limpar_vazios(serie_texto_presenca(["escola", "nomeescola"]))
    df_presence_norm["serie"] = limpar_vazios(serie_texto_presenca(["serie", "serieano"]))

    df_presence_norm["qtdAlunosPrevistos"] = pd.to_numeric(
        serie_texto_presenca(["qtdalunosprevistos", "previstos", "qtd_previstos"]),
        errors="coerce",
    ).astype("Int64")
    df_presence_norm["qtdAlunosPresentes"] = pd.to_numeric(
        serie_texto_presenca(["qtdalunospresentes", "presentes", "qtd_presentes"]),
        errors="coerce",
    ).astype("Int64")

    percentual_raw = serie_texto_presenca(["percentual", "percent"])
    percentual_clean = (
        percentual_raw.str.replace("%", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    df_presence_norm["percentual"] = pd.to_numeric(percentual_clean, errors="coerce")

    df_presence_norm["diaAplicacao"] = limpar_vazios(serie_texto_presenca(
        ["aplicacao", "diaaplicacao", "dia", "dataaplicacao", "diaprova", "diateste"]
    ))

    data_real_col = serie_texto_presenca(["datareal", "dataaplicacaoreal", "data"])
    df_presence_norm["dataReal"] = pd.to_datetime(data_real_col, dayfirst=True, errors="coerce")

    polo_normalizado_presenca = (
        df_presence_norm["polo"]
        .fillna("")
        .str.normalize("NFKD")
        .str.encode("ascii", "ignore")
        .str.decode("ascii")
        .str.upper()
        .str.strip()
    )
    df_presence_norm["gRE"] = polo_normalizado_presenca.map(POLO_TO_GRE)
    df_presence_norm["gRE"] = df_presence_norm["gRE"].fillna("GRE NAO IDENTIFICADA")

    df_presence_norm.to_parquet(PROCESSADO_DIR / "base_percentual_presenca.parquet", index=False)
    # -------------------------
    # Botão para baixar parquet
    # -------------------------
    st.download_button(
        label="⬇️ Baixar arquivo processado",
        data=gerar_bytes_parquet(df_presence_norm),
        file_name="base_percentual_presenca.parquet",
        mime="application/octet-stream"
    )
    st.success("Base de Presença processada com sucesso.")

    # Registros Pendentes
    df_pendentes = pd.read_excel(base_pendentes)
    df_pendentes.to_parquet(PROCESSADO_DIR / "base_registros_pendentes.parquet", index=False)
    # -------------------------
    # Botão para baixar parquet
    # -------------------------
    st.download_button(
        label="⬇️ Baixar arquivo processado",
        data=gerar_bytes_parquet(df_pendentes),
        file_name="base_registros_pendentes.parquet",
        mime="application/octet-stream"
    )
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
