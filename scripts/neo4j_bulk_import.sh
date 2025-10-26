#!/bin/bash

set -e  # Stop on error

# Répertoires
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SILVER_DIR="$BASE_DIR/data/silver"
GOLD_DIR="$BASE_DIR/data/gold"

# Création du dossier gold
mkdir -p "$GOLD_DIR"

echo "📦 Export des données vers CSV pour Neo4j..."

# Export des nodes (unique)
echo "🔄 Export des nodes..."
python3 - <<EOF
import pandas as pd
from pathlib import Path

df = pd.read_parquet(Path("$SILVER_DIR") / "nodes.parquet")
df = df[["id", "name", "label"]]
df.columns = ["id:ID", "name", "label"]
df.to_csv(Path("$GOLD_DIR") / "nodes.csv", index=False)
EOF
echo "✅ nodes.csv exporté"

# Concaténation des edges
echo "🔄 Concaténation des edges..."
python3 - <<EOF
import pandas as pd
from pathlib import Path
import glob

shard_paths = glob.glob(str(Path("$SILVER_DIR") / "shard=*/edges.parquet"))
dfs = [pd.read_parquet(Path(p)) for p in shard_paths]
edges_df = pd.concat(dfs, ignore_index=True)
edges_df = edges_df[["src", "dst"]]
edges_df.columns = [":START_ID", ":END_ID"]
edges_df.to_csv(Path("$GOLD_DIR") / "edges.csv", index=False)
EOF
echo "✅ edges.csv exporté"

echo "🎯 Tous les fichiers CSV sont prêts dans $GOLD_DIR"

