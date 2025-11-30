from pathlib import Path
from datetime import datetime, timedelta

import streamlit as st


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
