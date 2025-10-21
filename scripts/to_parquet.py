import pandas as pd
from pathlib import Path

# Définition des chemins
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
BRONZE_DIR = Path(__file__).resolve().parent.parent / "data" / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

# Conversion de chaque CSV
for csv_file in RAW_DIR.glob("*.csv"):
    df = pd.read_csv(csv_file)
    parquet_file = BRONZE_DIR / f"{csv_file.stem}.parquet"
    df.to_parquet(parquet_file, compression="snappy")
    print(f"✅ {csv_file.name} → {parquet_file.name}")
