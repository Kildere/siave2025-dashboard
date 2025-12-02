import pandas as pd
from firebase_client import save_batch

def parquet_to_firestore(path, collection, id_field):
    df = pd.read_parquet(path)
    records = df.to_dict(orient="records")
    save_batch(collection, records, id_field)
