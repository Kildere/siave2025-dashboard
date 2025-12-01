import re
import unicodedata
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from src.data_paths import ARQ_BASE_APLICACOES, ARQ_BASE_AGENDAMENTOS

SERIE_LABELS = {
    "3a": "3ª série",
    "4a": "4ª série",
    "em": "Ensino Médio",
    "2o": "2º Ano",
    "5o": "5º Ano",
    "9o": "9º Ano",
    "ef": "Ensino Fundamental",
}
SERIE_PATTERN = re.compile("|".join(SERIE_LABELS.keys()))
AGENDA_START = date(2025, 11, 24)
AGENDA_END = date(2025, 12, 5)
CALENDAR_EXCLUDED_DATES = {date(2025, 11, 29), date(2025, 11, 30)}
ARQ_BASE_PERCENTUAL = Path("data/processado/base_percentual_presenca.parquet")
TEAM_GRE_GROUPS = {
    "Equipe 1 - Iara e Sely": {"1", "12", "16"},
    "Equipe 2 - Rodrigo e Kildere": {"7", "9", "10", "11"},
    "Equipe 3 - Andrea e Juvaneide": {"8", "13", "6", "5"},
    "Equipe 4 - Angelica e Janaina": {"3", "4", "2", "14", "15"},
}
ARQ_PREVISTOS_PLANILHA = ARQ_BASE_PERCENTUAL
UPLOAD_LOG_KEY = "aplicacoes_upload_logs"
MONTH_ABBR_PT = [
    "jan",
    "fev",
    "mar",
    "abr",
    "mai",
    "jun",
    "jul",
    "ago",
    "set",
    "out",
    "nov",
    "dez",
]


def format_short_date_pt(day_value: date) -> str:
    return f"{day_value.day:02d}/{MONTH_ABBR_PT[day_value.month - 1]}"


def extract_gre_digits(label: str | None) -> str | None:
    if label is None:
        return None
    text = str(label)
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits or None


def format_upload_log_entry(filename: str) -> str:
    stem = Path(filename).stem
    match = re.search(r"(.+)-(\d{4}-\d{2}-\d{2})T(\d{2})_(\d{2})_(\d{2})", stem)
    if not match:
        return filename
    prefix, date_part, hh, mm, ss = match.groups()
    dt = datetime(
        int(date_part[0:4]),
        int(date_part[5:7]),
        int(date_part[8:10]),
        int(hh),
        int(mm),
        int(ss),
    )
    local_dt = dt - timedelta(hours=3)
    return f"{prefix} [{date_part}] [{local_dt.strftime('%H:%M')}]"


def register_upload_log(filename: str) -> None:
    entry = format_upload_log_entry(filename)
    logs = st.session_state.get(UPLOAD_LOG_KEY, [])
    logs = [entry] + logs
    st.session_state[UPLOAD_LOG_KEY] = logs[:10]


def get_calendar_dates_filtered() -> list[date]:
    return [
        ref_date
        for ref_date in get_agenda_dates()
        if ref_date not in CALENDAR_EXCLUDED_DATES
    ]


def gre_sort_key(label: str) -> tuple[int, str]:
    digits = extract_gre_digits(label)
    if digits:
        return (0, int(digits))
    return (1, str(label).strip())


def build_calendar_table(
    df_source: pd.DataFrame,
    group_col: str,
    labels: list[str],
    calendar_dates: list[date],
    date_col: str = "dataAgendmento",
) -> pd.DataFrame | None:
    if not labels or not calendar_dates:
        return None
    if group_col not in df_source.columns or date_col not in df_source.columns:
        return None

    calendar_df = df_source.dropna(subset=[group_col, date_col]).copy()
    calendar_df[group_col] = calendar_df[group_col].astype(str)
    calendar_df["diaCalendario"] = calendar_df[date_col].dt.date
    calendar_date_set = set(calendar_dates)
    calendar_df = calendar_df[calendar_df["diaCalendario"].isin(calendar_date_set)]

    calendar_index = pd.MultiIndex.from_product(
        [labels, calendar_dates], names=[group_col, "diaCalendario"]
    )
    if calendar_df.empty:
        base_summary = pd.DataFrame(calendar_index.tolist(), columns=[group_col, "diaCalendario"])
        base_summary["agendamentos"] = 0
    else:
        grouped = (
            calendar_df.groupby([group_col, "diaCalendario"])
            .agg(agendamentos=("aplicacaoId", "count"))
            .reset_index()
        )
        base_summary = (
            grouped.set_index([group_col, "diaCalendario"])
            .reindex(calendar_index, fill_value=0)
            .reset_index()
        )

    pivot_calendar = (
        base_summary.pivot(
            index=group_col,
            columns="diaCalendario",
            values="agendamentos",
        )
        .reindex(labels)
        .reindex(columns=calendar_dates)
        .fillna(0)
    )
    if not pivot_calendar.empty:
        pivot_calendar = pivot_calendar.astype(int)
    pivot_calendar.columns = [format_short_date_pt(day) for day in pivot_calendar.columns]
    pivot_calendar = pivot_calendar.reset_index()
    return pivot_calendar


def normalize_col(name: str) -> str:
    text = str(name).strip()
    nfkd = unicodedata.normalize("NFKD", text)
    no_accents = "".join(c for c in nfkd if not unicodedata.combining(c))
    clean = (
        no_accents.replace(" ", "")
        .replace("_", "")
        .replace("-", "")
    )
    return clean[0].lower() + clean[1:]


def normalize_serie_label(value) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    raw = str(value).strip()
    if not raw:
        return None
    norm = (
        raw.lower()
        .replace("º", "o")
        .replace("ª", "a")
    )
    matches = SERIE_PATTERN.findall(norm)
    if matches:
        seen = []
        for m in matches:
            if m not in seen:
                seen.append(m)
        return " / ".join(SERIE_LABELS[m] for m in seen)
    return SERIE_LABELS.get(norm, raw)


def normalize_previsto_column(name: str) -> str:
    text = str(name).strip()
    nfkd = unicodedata.normalize("NFKD", text)
    ascii_text = "".join(c for c in nfkd if not unicodedata.combining(c))
    return (
        ascii_text.lower()
        .replace(" ", "")
        .replace("_", "")
        .replace("-", "")
    )


@st.cache_data
def load_previstos_por_turma(path: Path = ARQ_PREVISTOS_PLANILHA) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["turma_norm", "previstos"])

    try:
        df_prev = pd.read_parquet(path)
    except Exception:
        return pd.DataFrame(columns=["turma_norm", "previstos"])
    df_prev.columns = [normalize_previsto_column(col) for col in df_prev.columns]
    rename_map = {
        "qtdalunosprevistos": "previstos",
        "qtdalunospreservistos": "previstos",
        "qtdalunospreservados": "previstos",
        "qtdalunospresvistos": "previstos",
        "qtdalunospresentes": "presentes",
    }
    df_prev = df_prev.rename(columns=rename_map)

    if "turma" not in df_prev.columns or "previstos" not in df_prev.columns:
        return pd.DataFrame(columns=["turma_norm", "previstos"])

    df_prev["turma_norm"] = df_prev["turma"].astype(str).str.lower().str.strip()
    df_prev["previstos"] = pd.to_numeric(df_prev["previstos"], errors="coerce").astype("Int64")
    aggregated = (
        df_prev.dropna(subset=["turma_norm"])
        .groupby("turma_norm", as_index=False)
        .agg(previstos=("previstos", "sum"))
    )
    return aggregated


@st.cache_data
def load_base_aplicacoes(path: Path = ARQ_BASE_APLICACOES) -> pd.DataFrame:
    if not path.exists():
        st.error(
            "Base de aplicações não encontrada. Execute o loader de agendamentos antes."
        )
        st.stop()
    df = pd.read_parquet(path)
    if "dataAgendmento" in df.columns:
        df["dataAgendmento"] = pd.to_datetime(df["dataAgendmento"])
    return df


def process_presence_file(uploaded_file) -> pd.DataFrame:
    df = pd.read_excel(uploaded_file)
    df.columns = [normalize_col(c) for c in df.columns]

    standard_required = {"codigoEscola", "aplicacao", "qtdAlunosPrevistos"}
    aloc_required = {"coEscolaCenso", "dia", "alocados"}

    if standard_required.issubset(df.columns):
        pass
    elif aloc_required.issubset(df.columns):
        df = df.rename(
            columns={
                "coEscolaCenso": "codigoEscola",
                "dia": "aplicacao",
                "alocados": "qtdAlunosPrevistos",
            }
        )
        if "qtdAlunosPresentes" not in df.columns:
            df["qtdAlunosPresentes"] = pd.NA
    else:
        missing = standard_required - set(df.columns)
        raise ValueError(
            "Arquivo enviado não possui as colunas necessárias para alunos previstos."
        )

    if "serie" in df.columns:
        df["serie"] = df["serie"].apply(normalize_serie_label)
    else:
        df["serie"] = None

    if "qtdAlunosPresentes" not in df.columns:
        df["qtdAlunosPresentes"] = pd.NA

    df["qtdAlunosPrevistos"] = pd.to_numeric(
        df["qtdAlunosPrevistos"], errors="coerce"
    ).astype("Int64")
    df["qtdAlunosPresentes"] = pd.to_numeric(
        df["qtdAlunosPresentes"], errors="coerce"
    ).astype("Int64")

    df["aplicacaoId"] = (
        df["codigoEscola"].astype(str).str.strip()
        + "_"
        + df["aplicacao"].astype(str).str.strip()
    )
    df = df.rename(
        columns={
            "qtdAlunosPrevistos": "qtdPrevistos",
            "qtdAlunosPresentes": "qtdPresentes",
        }
    )
    aggregated = (
        df.groupby("aplicacaoId", as_index=False)
        .agg(
            qtdPrevistos=("qtdPrevistos", "sum"),
            qtdPresentes=("qtdPresentes", "sum"),
        )
        .reset_index(drop=True)
    )
    aggregated["presenceKey"] = aggregated["aplicacaoId"]
    aggregated["qtdPrevistos"] = pd.to_numeric(
        aggregated["qtdPrevistos"], errors="coerce"
    ).astype("Int64")
    aggregated["qtdPresentes"] = pd.to_numeric(
        aggregated["qtdPresentes"], errors="coerce"
    ).astype("Int64")
    prev_numeric = aggregated["qtdPrevistos"].astype("float64")
    pres_numeric = aggregated["qtdPresentes"].astype("float64")
    aggregated["percentualPresenca"] = (
        (pres_numeric / prev_numeric) * 100
    ).where(prev_numeric > 0)
    aggregated["percentualPresencaStr"] = aggregated["percentualPresenca"].apply(
        lambda x: f"{x:.1f}%" if pd.notna(x) else None
    )
    cols = [
        "aplicacaoId",
        "presenceKey",
        "qtdPrevistos",
        "qtdPresentes",
        "percentualPresenca",
        "percentualPresencaStr",
    ]
    return aggregated[cols]


def merge_presence(df_aplicacoes: pd.DataFrame, df_presence: pd.DataFrame | None) -> pd.DataFrame:
    df = df_aplicacoes.copy()
    df["presenceKey"] = (
        df["coEscolaCenso"].astype(str).str.strip()
        + "_"
        + df["diaAplicacao"].astype(str).str.strip()
    )
    if df_presence is None:
        for col in [
            "qtdPrevistos",
            "qtdPresentes",
            "percentualPresenca",
            "percentualPresencaStr",
        ]:
            if col not in df.columns:
                df[col] = pd.NA
        return df.drop(columns=["presenceKey"])

    df_presence = df_presence.copy()
    if "presenceKey" not in df_presence.columns:
        df_presence["presenceKey"] = df_presence["aplicacaoId"]
    merged = df.merge(
        df_presence.drop(columns=["aplicacaoId"], errors="ignore"),
        on="presenceKey",
        how="left",
        suffixes=("", "_presence"),
    )
    merged = merged.drop(columns=["presenceKey"])
    return merged


def format_percent(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:.1f}%"


def get_agenda_dates() -> list[date]:
    return [
        AGENDA_START + timedelta(days=offset)
        for offset in range((AGENDA_END - AGENDA_START).days + 1)
    ]


@st.cache_data
def load_presence_from_parquet(path: Path = ARQ_BASE_PERCENTUAL) -> tuple[pd.DataFrame | None, str | None]:
    """Carrega o parquet consolidado de percentual de presen��a."""
    if not path.exists():
        return None, None
    try:
        df = pd.read_parquet(path)
    except Exception:
        return None, None
    return (df, path.name) if not df.empty else (None, path.name)


st.title("Aplicações - SIAVE 2025")

from src.utils import format_timestamp_brazil

# Caminho da pasta onde os arquivos de alocações são enviados
arquivo_agend = ARQ_BASE_AGENDAMENTOS
nome_arq = arquivo_agend.name if arquivo_agend.exists() else "Nenhum arquivo encontrado"
dt_br = (
    format_timestamp_brazil(datetime.fromtimestamp(arquivo_agend.stat().st_mtime))
    if arquivo_agend.exists()
    else "Data nao identificada"
)

st.markdown(f"""
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
📂 Pasta: {arquivo_agend.parent}<br>
📄 Arquivo carregado: {nome_arq}<br>
🕒 Atualizado em: {dt_br}
</div>
""", unsafe_allow_html=True)

base_df = load_base_aplicacoes()

default_presence_df, default_presence_name = load_presence_from_parquet()
presence_df = default_presence_df
presence_source = default_presence_name

uploaded_file = st.file_uploader(
    "Envie o arquivo de alunos previstos (QtdAlunosPrevistos em .xlsx)",
    type=["xlsx"],
    help="Arquivos no formato Percentual_Presenca-*.xlsx ou similares são suportados.",
)
if uploaded_file is not None:
    try:
        presence_df = process_presence_file(uploaded_file)
        presence_source = uploaded_file.name
        register_upload_log(uploaded_file.name)
        st.success("Arquivo de presença processado com sucesso!")
    except Exception as exc:
        st.error(f"Falha ao processar arquivo: {exc}")

if presence_df is None:
    st.info(
        "Nenhum arquivo com a quantidade prevista de alunos foi encontrado automaticamente. Faça o upload para habilitar os indicadores."
    )
elif presence_source:
    st.caption(f"Fonte(s) dos dados de alunos previstos: {presence_source}")

upload_logs = st.session_state.get(UPLOAD_LOG_KEY, [])
if upload_logs:
    st.caption("Log de uploads recentes:")
    for entry in upload_logs:
        st.write(f"- {entry}")

df = merge_presence(base_df, presence_df)

previstos_por_turma = load_previstos_por_turma()
if "turma" in df.columns:
    df["turma_norm"] = df["turma"].astype(str).str.lower().str.strip()
    if not previstos_por_turma.empty:
        df = df.merge(previstos_por_turma, on="turma_norm", how="left")
    if "previstos" not in df.columns:
        df["previstos"] = pd.NA
    df["previstos"] = pd.to_numeric(df["previstos"], errors="coerce").astype("Int64")
    df = df.drop(columns=["turma_norm"])
else:
    df["previstos"] = pd.Series(pd.NA, index=df.index, dtype="Int64")

st.subheader("Visão Agenda - Resumo")
current_day = date.today()
operacional_df = df.copy()
if "dataAgendmento" in operacional_df.columns:
    day_mask = operacional_df["dataAgendmento"].dt.date == current_day
    operacional_df = operacional_df[day_mask]
st.caption(f"Dia corrente: {format_short_date_pt(current_day)}")
col_op1, col_op2, col_op3, col_op4 = st.columns(4)
col_op1.metric("Aplicações do dia", len(operacional_df))
col_op2.metric(
    "Escolas atendidas",
    operacional_df["coEscolaCenso"].nunique() if "coEscolaCenso" in operacional_df else 0,
)
col_op3.metric(
    "Turmas",
    operacional_df["coTurmaCenso"].nunique() if "coTurmaCenso" in operacional_df else 0,
)
if presence_df is not None:
    total_prev_oper = pd.to_numeric(operacional_df["qtdPrevistos"], errors="coerce").fillna(0).sum()
    total_pres_oper = pd.to_numeric(operacional_df["qtdPresentes"], errors="coerce").fillna(0).sum()
    perc_operacional = format_percent(
        (total_pres_oper / total_prev_oper) * 100 if total_prev_oper else None
    )
else:
    perc_operacional = "-"
col_op4.metric("% média presença", perc_operacional)

st.divider()
st.header("Calendário Agendamento / Alocações por GRE")

calendar_dates = get_calendar_dates_filtered()

if not calendar_dates:
    st.info("Nenhuma data configurada para montar o calendario.")
elif "dataAgendmento" not in df.columns:
    st.info("Base sem coluna de data de agendamento para montar o calendario.")
else:

    def render_calendar(
        source_df: pd.DataFrame,
        column_name: str,
        labels: list[str],
        display_label: str,
        empty_msg: str,
    ):
        if not labels:
            st.info(empty_msg)
            return False
        pivot_calendar = build_calendar_table(
            source_df,
            column_name,
            labels,
            calendar_dates,
        )
        if pivot_calendar is None or pivot_calendar.empty:
            st.info(empty_msg)
            return False
        pivot_calendar = pivot_calendar.rename(columns={column_name: display_label})
        st.dataframe(pivot_calendar, use_container_width=True)
        return True

    gre_labels = sorted(
        df["gRE"].dropna().astype(str).unique(), key=gre_sort_key
    )
    rendered_gre = render_calendar(
        df,
        "gRE",
        gre_labels,
        "GRE",
        "Nenhuma GRE encontrada na base para montar o calendario.",
    )
    if rendered_gre:
        st.caption(
            "Quantidade de agendamentos por GRE (dias entre 24/11 e 05/12, sem 29 e 30/11)."
        )

    st.subheader("Calendário Agendamento / Alocações por Polo")
    polo_source_df = df.copy()
    team_options = ["(Todas)"] + list(TEAM_GRE_GROUPS.keys())
    team_selected = st.selectbox(
        "Equipe (agrupamento de GREs)",
        team_options,
        index=0,
        key="calendar_team_filter",
    )
    if team_selected != "(Todas)":
        if "gRE" not in df.columns:
            st.info("Base sem coluna de GRE para aplicar o filtro por equipe.")
        else:
            allowed_digits = TEAM_GRE_GROUPS[team_selected]
            mask = (
                df["gRE"]
                .apply(lambda value: extract_gre_digits(value) in allowed_digits)
                .fillna(False)
            )
            polo_source_df = df[mask]
    if "polo" not in polo_source_df.columns:
        st.info("Base sem coluna de polo para montar o calendario.")
    else:
        polo_labels = sorted(polo_source_df["polo"].dropna().astype(str).unique())
        rendered_polo = render_calendar(
            polo_source_df,
            "polo",
            polo_labels,
            "Polo",
            "Nenhum polo encontrado na base para montar o calendario.",
        )
        if rendered_polo:
            st.caption(
                "Quantidade de agendamentos por Polo (dias entre 24/11 e 05/12, sem 29 e 30/11)."
            )

st.divider()
st.subheader("Visão Estrutural")

filtered = df.copy()

st.subheader("Calendario Agendamento / Alocações por Municípios")
calendar_mun_dates = get_calendar_dates_filtered()
if not calendar_mun_dates:
    st.info("Sem configuracao de datas para o calendario por municipio.")
elif (
    "dataAgendmento" not in filtered.columns
    or "municipio" not in filtered.columns
    or "polo" not in filtered.columns
):
    st.info("Base sem colunas necessarias para o calendario por municipio.")
else:
    cal_col_polo, cal_col_mun = st.columns(2)
    calendar_polo_options = ["(Todos)"] + sorted(
        filtered["polo"].dropna().astype(str).unique()
    )
    calendar_polo_selected = cal_col_polo.selectbox(
        "Polo (Calendario)",
        calendar_polo_options,
        index=0,
        key="struct_calendar_polo",
    )
    calendar_df = filtered.copy()
    if calendar_polo_selected != "(Todos)":
        calendar_df = calendar_df[
            calendar_df["polo"].astype(str) == calendar_polo_selected
        ]
    calendar_mun_options = ["(Todos)"] + sorted(
        calendar_df["municipio"].dropna().astype(str).unique()
    )
    calendar_mun_selected = cal_col_mun.selectbox(
        "Municipio",
        calendar_mun_options,
        index=0,
        key="struct_calendar_municipio",
    )
    if calendar_mun_selected != "(Todos)":
        calendar_df = calendar_df[
            calendar_df["municipio"].astype(str) == calendar_mun_selected
        ]
    municipio_labels = sorted(calendar_df["municipio"].dropna().astype(str).unique())
    if not municipio_labels:
        st.info("Nenhum municipio encontrado para os filtros selecionados.")
    else:
        calendario_municipio = build_calendar_table(
            calendar_df,
            "municipio",
            municipio_labels,
            calendar_mun_dates,
        )
        if calendario_municipio is None or calendario_municipio.empty:
            st.info("Nao foi possivel gerar o calendario por municipio.")
        else:
            calendario_municipio = calendario_municipio.rename(
                columns={"municipio": "Municipio"}
            )
            st.dataframe(calendario_municipio, use_container_width=True)
            st.caption(
                "Agendamentos por municipio (dias entre 24/11 e 05/12, sem 29 e 30/11)."
            )

    detail_df = calendar_df.copy()
    if detail_df.empty:
        st.info("Nenhum registro para detalhar com os filtros do calendario.")
    else:
        detail_df["Percentual"] = detail_df["percentualPresencaStr"].fillna("-")
        if "dataAgendmento" in detail_df.columns:
            detail_df["dataAgendamentoStr"] = (
                detail_df["dataAgendmento"].dt.strftime("%d/%m").fillna("-")
            )
        else:
            detail_df["dataAgendamentoStr"] = "-"
        if "previstos" in detail_df.columns:
            detail_df["previstos"] = pd.to_numeric(
                detail_df["previstos"], errors="coerce"
            ).astype("Int64")
        else:
            detail_df["previstos"] = pd.Series(pd.NA, index=detail_df.index, dtype="Int64")
        order_cols = [col for col in ["municipio", "escola", "turma"] if col in detail_df.columns]
        if order_cols:
            detail_df = detail_df.sort_values(order_cols)
        table_cols = {
            "gRE": "GRE",
            "polo": "Polo",
            "municipio": "Municipio",
            "escola": "Escola",
            "turma": "Turma",
            "dataAgendamentoStr": "Data",
            "turno": "Turno",
            "previstos": "Previstos",
            "qtdPresentes": "Presentes",
            "Percentual": "%",
            "statusAplicacao": "Status",
        }
        display_cols = [c for c in table_cols.keys() if c in detail_df.columns]
        if display_cols:
            detail_display = detail_df[display_cols].rename(columns=table_cols)
            st.dataframe(detail_display, use_container_width=True)
        else:
            st.info("Colunas necessarias para detalhar nao encontradas.")


