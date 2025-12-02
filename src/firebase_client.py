from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
import streamlit as st


@st.cache_resource(show_spinner=False)
def get_db() -> firestore.Client | None:
    """
    Inicializa e retorna o cliente do Firestore.

    Ordem de busca das credenciais:
    1) st.secrets["firestore"]["credentials"] (Streamlit Cloud)
    2) arquivo local secrets/firestore_key.json

    Se não encontrar credenciais ou ocorrer erro, retorna None e
    a aplicação deve fazer fallback para leitura via parquet.
    """
    try:
        # Se já existe app inicializado, reaproveita
        if firebase_admin._apps:
            return firestore.client()

        cred: credentials.Certificate | None = None

        # 1) Credenciais via st.secrets (ambiente Streamlit Cloud)
        if "firestore" in st.secrets:
            secrets_section = st.secrets["firestore"]
            if "credentials" in secrets_section:
                key_dict = json.loads(secrets_section["credentials"])
                cred = credentials.Certificate(key_dict)

        # 2) Credenciais via arquivo local secrets/firestore_key.json
        if cred is None:
            cred_path = Path("secrets") / "firestore_key.json"
            if cred_path.exists():
                cred = credentials.Certificate(str(cred_path))

        if cred is None:
            # Não quebra a aplicação – apenas devolve None
            st.warning("Firestore não configurado: nenhuma credencial encontrada.")
            return None

        firebase_admin.initialize_app(cred)
        return firestore.client()
    except Exception as exc:
        st.warning(f"Não foi possível inicializar o Firestore: {exc}")
        return None


def _safe_get_db() -> firestore.Client | None:
    """Wrapper que tenta obter o db e devolve None em caso de erro."""
    try:
        return get_db()
    except Exception as exc:
        st.warning(f"Firestore indisponível: {exc}")
        return None


def save_dataframe(collection: str, df: pd.DataFrame, chunk_size: int = 400) -> None:
    """
    Envia um DataFrame para o Firestore, criando documentos com IDs automáticos.

    - collection: nome da coleção (ex.: 'siave_estrutural')
    - df: DataFrame a ser persistido
    - chunk_size: quantidade de documentos por batch
    """
    db = _safe_get_db()
    if db is None:
        return
    if df is None or df.empty:
        return

    records: List[Dict[str, Any]] = df.to_dict(orient="records")
    for i in range(0, len(records), chunk_size):
        batch = db.batch()
        for row in records[i : i + chunk_size]:
            doc_ref = db.collection(collection).document()
            batch.set(doc_ref, row)
        batch.commit()


@st.cache_data(show_spinner=False)
def load_collection_df(collection: str) -> pd.DataFrame:
    """
    Lê todos os documentos de uma coleção do Firestore e devolve um DataFrame.

    - Se Firestore não estiver configurado ou a coleção estiver vazia,
      devolve DataFrame vazio (sem quebrar a aplicação).
    """
    db = _safe_get_db()
    if db is None:
        return pd.DataFrame()

    try:
        docs = list(db.collection(collection).stream())
        if not docs:
            return pd.DataFrame()
        data = [doc.to_dict() for doc in docs]
        return pd.DataFrame(data)
    except Exception as exc:
        st.warning(f"Erro ao ler coleção '{collection}' no Firestore: {exc}")
        return pd.DataFrame()
