import great_expectations as gx
from pathlib import Path
import pandas as pd

# Chemins
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"

# Chargement des données
nodes_df = pd.read_csv(RAW_DIR / "nodes.csv")
edges_df = pd.read_csv(RAW_DIR / "edges.csv")

# Initialisation du contexte GE (ephemeral)
context = gx.get_context()

# Création des Validators
nodes_validator = context.data_sources.add_pandas(nodes_df)
edges_validator = context.sources.pandas_default.read_dataframe(edges_df)

# Expectations
nodes_validator.expect_column_values_to_be_unique("id")
edges_validator.expect_column_values_to_not_be_null("src")
edges_validator.expect_column_values_to_not_be_null("dst")

# Résultats
results = nodes_validator.validate()
results.merge(edges_validator.validate())

# Affichage
for res in results["results"]:
    name = res["expectation_config"]["expectation_type"]
    status = "✅ OK" if res["success"] else "❌ Échec"
    print(f"{name}: {status}")

