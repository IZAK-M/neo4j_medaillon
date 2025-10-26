import csv
import random
from pathlib import Path

# Configuration
NUM_NODES = 1_000_000
NUM_EDGES = 5_000_000
NODE_LABELS = ['Person', 'Org', 'Paper']

# Dossier parent du script
BASE_DIR = Path(__file__).resolve().parent.parent / "data" 
OUTPUT_DIR = BASE_DIR / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Génération des nœuds
with open(OUTPUT_DIR / "nodes.csv", mode="w", newline="") as nodes_file:
    writer = csv.writer(nodes_file)
    writer.writerow(["id", "label", "name"])
    for node_id in range(NUM_NODES):
        label = random.choice(NODE_LABELS)
        name = f"name_{node_id}"
        writer.writerow([node_id, label, name])

# Génération des relations
with open(OUTPUT_DIR / "edges.csv", mode="w", newline="") as edges_file:
    writer = csv.writer(edges_file)
    writer.writerow(["src", "dst", "type"])
    for _ in range(NUM_EDGES):
        src = random.randint(0, NUM_NODES - 1)
        dst = random.randint(0, NUM_NODES - 1)
        writer.writerow([src, dst, "REL"])
