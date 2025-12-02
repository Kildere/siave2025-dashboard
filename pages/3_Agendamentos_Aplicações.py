from datetime import date, datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from src.data_paths import ARQ_BASE_AGENDAMENTOS
from src.utils import format_timestamp_brazil

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

TEAM_GRE_GROUPS = {
    "Equipe 1 - Iara e Sely": {"1", "12", "16"},
    "Equipe 2 - Rodrigo e Kildere": {"7", "9", "10", "11"},
    "Equipe 3 - Andrea e Juvaneide": {"8", "13", "6", "5"},
    "Equipe 4 - Angelica e Janaina": {"3", "4", "2", "14", "15"},
}


def prep(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza colunas para o padrão do Loader, aceitando tanto esquemas novos
    (parquet) quanto planilhas antigas.
    """
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    rename_map = {
        # chaves básicas
        "coescolacenso": "coEscolaCenso",
        "municipio": "municipio",
        "gre": "gRE",
        "polo": "polo",

        # previstos / presentes podem vir com nomes antigos ou novos
        "previstos": "previstos",
        "qtdalunosprevistos": "previstos",
        "presentes": "presentes",
        "qtdalunospresentes": "presentes",

        # percentual
        "percentual": "percentual",

        # datas
        "dataagendamento": "dataAgendamento",
        "dataagendmento": "dataAgendamento",
        "dataaplicacaoreal": "dataAplicacaoReal",
        "datareal": "dataAplicacaoReal",

        # eventual suporte
        "qtdiasaplicacao": "qtdDiasAplicacao",
    }

    df.rename(columns=rename_map, inplace=True)
    return df


def format_short_date_pt(day_value: date) -> str:
    return f"{day_value.day:02d}/{MONTH_ABBR_PT[day_value.month - 1]}"


def extract_gre_digits(label: str | None) -> str | None:
    if label is None:
        return None
    digits = "".join(ch for ch in str(label) if ch.isdigit())
    return digits or None


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
    date_col: str = "dataAgendamento",
) -> pd.DataFrame | None:
    if not labels or not calendar_dates:
        return None
    if group_col not in df_source.columns or date_col not in df_source.columns:
        return None

    calendar_df = df_source.dropna(subset=[group_col, date_col]).copy()
    calendar_df[group_col] = calendar_df[group_col].astype(str)
    calendar_df[date_col] = pd.to_datetime(calendar_df[date_col], errors="coerce")
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
            .size()
            .reset_index(name="agendamentos")
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


@st.cache_data
def load_base_agendamentos(path: Path = ARQ_BASE_AGENDAMENTOS) -> pd.DataFrame:
    if not path.exists():
        st.error("Base de agendamentos nao encontrada. Execute o loader antes.")
        st.stop()
    df = pd.read_parquet(path)
    df.columns = df.columns.str.strip()
    return df


def get_calendar_dates(df: pd.DataFrame) -> list[date]:
    if "dataAgendamento" not in df.columns:
        return []
    return sorted(pd.to_datetime(df["dataAgendamento"], errors="coerce").dropna().dt.date.unique().tolist())


def format_percent(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value:.1f}%"


def merge_presence(base: pd.DataFrame, presence: pd.DataFrame) -> pd.DataFrame:
    """
    Junta agendamentos (base) com presença (presence) usando o padrão do Loader.
    Chaves do merge: coEscolaCenso + municipio + gRE + polo.
    """
    if base is None or base.empty:
        return base

    if presence is None or presence.empty:
        df = base.copy()
        for col in ["previstos", "presentes", "percentual", "dataAplicacaoReal"]:
            if col not in df.columns:
                df[col] = pd.NA
        return df

    df = base.merge(
        presence,
        on=["coEscolaCenso", "municipio", "gRE", "polo"],
        how="left",
        suffixes=("", "_pres"),
    )

    for col in ["previstos", "presentes", "percentual", "dataAplicacaoReal"]:
        pres_col = f"{col}_pres"
        if pres_col in df.columns:
            df[col] = df[pres_col].where(df[pres_col].notna(), df.get(col))
            df.drop(columns=[pres_col], inplace=True)
        elif col not in df.columns:
            df[col] = pd.NA

    return df


st.title("Aplicacoes - SIAVE 2025")

arquivo_agend = ARQ_BASE_AGENDAMENTOS
nome_arq = arquivo_agend.name if arquivo_agend.exists() else "Nenhum arquivo encontrado"
dt_br = (
    format_timestamp_brazil(datetime.fromtimestamp(arquivo_agend.stat().st_mtime))
    if arquivo_agend.exists()
    else "Data nao identificada"
)

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
📂 Pasta: {arquivo_agend.parent}<br>
📄 Arquivo carregado: {nome_arq}<br>
⏰ Atualizado em: {dt_br}
</div>
""",
    unsafe_allow_html=True,
)

base_df = prep(load_base_agendamentos())
presence_path = Path("data/processado/base_percentual_presenca.parquet")
presence_df = prep(pd.read_parquet(presence_path)) if presence_path.exists() else pd.DataFrame()

df = merge_presence(base_df, presence_df)

if "dataAgendamento" in df.columns:
    df["dataAgendamento"] = pd.to_datetime(df["dataAgendamento"], errors="coerce")
if "dataAplicacaoReal" in df.columns:
    df["dataAplicacaoReal"] = pd.to_datetime(df["dataAplicacaoReal"], errors="coerce")
for num_col in ["previstos", "presentes"]:
    if num_col in df.columns:
        df[num_col] = pd.to_numeric(df[num_col], errors="coerce").fillna(0)

st.subheader("Visao Agenda - Resumo")
current_day = date.today()
st.caption(f"Dia corrente: {format_short_date_pt(current_day)}")
col_op1, col_op2, col_op3, col_op4 = st.columns(4)
col_op1.metric(
    "Agendamentos do dia",
    df[df["dataAgendamento"].dt.date == current_day]["coEscolaCenso"].nunique()
    if "dataAgendamento" in df.columns
    else 0,
)
col_op2.metric("Escolas atendidas", df["coEscolaCenso"].nunique() if "coEscolaCenso" in df.columns else 0)
col_op3.metric("Turmas", df["coTurmaCenso"].nunique() if "coTurmaCenso" in df.columns else 0)
total_prev = df["previstos"].sum() if "previstos" in df.columns else 0
total_pres = df["presentes"].sum() if "presentes" in df.columns else 0
perc_operacional = format_percent((total_pres / max(total_prev, 1)) * 100)
col_op4.metric("% media presenca", perc_operacional)

st.divider()
st.header("Calendario Agendamento / Alocacoes por GRE")

calendar_dates = get_calendar_dates(df)

if "dataAgendamento" not in df.columns:
    st.warning("Base sem coluna dataAgendamento.")
elif not calendar_dates:
    st.info("Nenhuma data encontrada para montar o calendario.")
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

    gre_labels = sorted(df["gRE"].dropna().astype(str).unique(), key=gre_sort_key) if "gRE" in df.columns else []
    rendered_gre = render_calendar(
        df,
        "gRE",
        gre_labels,
        "GRE",
        "Nenhuma GRE encontrada na base para montar o calendario.",
    )
    if rendered_gre:
        st.caption("Quantidade de agendamentos por GRE (datas encontradas na base).")

    st.subheader("Calendario Agendamento / Alocacoes por Polo")
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
            st.caption("Quantidade de agendamentos por Polo (datas encontradas na base).")

st.divider()
st.subheader("Visao Estrutural")

filtered = df.copy()

st.subheader("Calendario Agendamento / Alocacoes por Municipios")
calendar_mun_dates = calendar_dates
if not calendar_mun_dates:
    st.info("Sem datas na base para o calendario por municipio.")
elif (
    "dataAgendamento" not in filtered.columns
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
        calendar_df = calendar_df[calendar_df["polo"].astype(str) == calendar_polo_selected]
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
        calendar_df = calendar_df[calendar_df["municipio"].astype(str) == calendar_mun_selected]
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
            calendario_municipio = calendario_municipio.rename(columns={"municipio": "Municipio"})
            st.dataframe(calendario_municipio, use_container_width=True)
            st.caption("Agendamentos por municipio (datas encontradas na base).")

    detail_df = calendar_df.copy()
    if detail_df.empty:
        st.info("Nenhum registro para detalhar com os filtros do calendario.")
    else:
        if "dataAgendamento" in detail_df.columns:
            detail_df["dataAgendamento"] = pd.to_datetime(detail_df["dataAgendamento"], errors="coerce")
            detail_df["dataAgendamentoStr"] = detail_df["dataAgendamento"].dt.strftime("%d/%m").fillna("-")
        else:
            detail_df["dataAgendamentoStr"] = "-"
        detail_df["Percentual"] = detail_df.get("percentual", pd.Series()).fillna(0).apply(format_percent)
        if "previstos" in detail_df.columns:
            detail_df["previstos"] = pd.to_numeric(detail_df["previstos"], errors="coerce").fillna(0).astype(int)
        else:
            detail_df["previstos"] = pd.Series(0, index=detail_df.index, dtype=int)
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
            "presentes": "Presentes",
            "Percentual": "%",
            "qtdDiasAplicacao": "Qtd Dias",
        }
        display_cols = [c for c in table_cols.keys() if c in detail_df.columns]
        if display_cols:
            detail_display = detail_df[display_cols].rename(columns=table_cols)
            st.dataframe(detail_display, use_container_width=True)
        else:
            st.info("Colunas necessarias para detalhar nao encontradas.")
