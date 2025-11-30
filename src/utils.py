
def log(msg: str):
    print(f"[SIAVE2025] {msg}")

from pathlib import Path
from datetime import datetime, timedelta
import re

def get_latest_file(base_dir: str | Path, prefix: str) -> Path | None:
    """
    Retorna o arquivo mais recente dentro da pasta base_dir,
    cujo nome começa com prefix + '-'.
    Ex: prefix="Alocacoes" → Alocacoes-2025-11-29T14_48_04.082Z.xlsx
    """
    base = Path(base_dir)
    if not base.exists():
        return None

    pattern = f"{prefix}-*.xlsx"
    arquivos = sorted(base.glob(pattern))
    if not arquivos:
        return None

    return arquivos[-1]  # Arquivo mais recente

def parse_timestamp_from_filename(filename: str) -> datetime | None:
    """
    Extrai o timestamp ISO-8601 do nome do arquivo.
    Ex: 'Alocacoes-2025-11-29T14_48_04.082Z.xlsx'
    """
    match = re.search(r"(\d{4}-\d{2}-\d{2}T\d{2}[_:]\d{2}[_:]\d{2}\.\d{3})Z", filename)
    if not match:
        return None
    raw = match.group(1).replace("_", ":")
    try:
        return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S.%f")
    except:
        return None

def format_timestamp_brazil(dt: datetime | None) -> str:
    """
    Converte UTC → horário do Brasil (UTC-3) e devolve string formatada.
    """
    if dt is None:
        return "Data não identificada"

    brasil = dt - timedelta(hours=3)
    return brasil.strftime("%d/%m/%Y %H:%M")
