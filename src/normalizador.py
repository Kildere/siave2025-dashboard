import pandas as pd
import unicodedata
from pathlib import Path

BASE = Path("data/processado/base_estrutural.parquet")
DESTINO = Path("data/processado/base_estrutural_normalizado.parquet")

def normalize(name: str) -> str:
    # tira acentos
    nfkd = unicodedata.normalize("NFKD", name)
    no_accents = "".join([c for c in nfkd if not unicodedata.combining(c)])
    # tira espaços e especiais
    clean = (
        no_accents.replace(" ", "")
                  .replace("_", "")
                  .replace("-", "")
    )
    # camelCase
    clean = clean[0].lower() + clean[1:]
    return clean

def normalizar_parquet():
    df = pd.read_parquet(BASE)
    df.columns = [normalize(c) for c in df.columns]
    DESTINO.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(DESTINO, index=False)
    print("\n==== CONCLUÍDO ====")
    print("Colunas finais:", df.columns.tolist())
    print(f"Arquivo salvo em: {DESTINO}")

if __name__ == "__main__":
    normalizar_parquet()
