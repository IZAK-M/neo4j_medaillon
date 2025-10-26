import pandas as pd
import numpy as np
from pathlib import Path

import sys

# Ajoute le dossier racine au chemin d'import
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from quality.gx_checkpoint import validate_graph_quality

# Chemins
BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
NUM_SHARDS = 8

# Chargement des données
edges_df = pd.read_parquet(BRONZE_DIR / "edges.parquet")
nodes_df = pd.read_parquet(BRONZE_DIR / "nodes.parquet")

# Validation
results = validate_graph_quality(nodes_df, edges_df)

# Vérification des résultats
if not all(r["success"] for r in results.values()):
    print("❌ Données invalides, nettoyage en cours...")
    edges_df = edges_df.dropna(subset=["src", "dst"])
    nodes_df = nodes_df.drop_duplicates(subset=["id"])
    nodes_df = nodes_df.dropna()
    print("✅ Données valides, partitionnement en cours...")
    print("📂 Partitionnement : ")
else:
    print("✅ Données valides, partitionnement en cours...")
    print("📂 Partitionnement : ")

# Création d'une copie explicite pour éviter le warning ou un comportement inattendus
edges_df = edges_df.copy()

# Attribution aléatoire d'un shard à chaque edge
edges_df["shard"] = np.random.randint(0, NUM_SHARDS, size=len(edges_df))

# Écritures des nodes
SILVER_DIR.mkdir(parents=True, exist_ok=True)
nodes_df.to_parquet(SILVER_DIR / "nodes.parquet", compression="snappy")

# Partitionnement et écriture
for shard_id in range(NUM_SHARDS):
    shard_dir = SILVER_DIR / f"shard={shard_id}"
    shard_dir.mkdir(parents=True, exist_ok=True)

    # Écriture des edges
    shard_edges = edges_df[edges_df["shard"] == shard_id].drop(columns=["shard"])
    shard_edges.to_parquet(shard_dir / "edges.parquet", compression="snappy")

    print(f"✅ Shard {shard_id} → {len(shard_edges)} edges")