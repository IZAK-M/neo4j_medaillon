#!/bin/bash

set -e  # Stop on error

# Chemins locaux
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_DIR="$PROJECT_DIR/data/gold"
IMPORT_DIR="/tmp/neo4j_import"
DB_DIR="$HOME/neo4j_data/databases/graph.db"

echo "📁 Copie des fichiers CSV vers $IMPORT_DIR..."
mkdir -p "$IMPORT_DIR"
cp "$SOURCE_DIR"/*.csv "$IMPORT_DIR"

echo "📦 Préparation du dossier de base Neo4j : $DB_DIR"
mkdir -p "$DB_DIR"

echo "🐳 Lancement du conteneur Neo4j pour import avec persistance..."
docker run -it --rm \
  --user=neo4j \
  --volume="$IMPORT_DIR:/var/lib/neo4j/import" \
  --volume="$DB_DIR:/data/databases/graph.db" \
  neo4j:2025.09.0 bash -c "
    echo '📥 Import Neo4j en cours...';
    neo4j-admin database import full \
      --nodes=Person='/var/lib/neo4j/import/nodes_*.csv' \
      --relationships=REL='/var/lib/neo4j/import/edges_*.csv' \
      --database=graph.db;
    echo '✅ Import terminé';
"
