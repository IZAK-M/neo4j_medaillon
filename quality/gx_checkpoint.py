import great_expectations as gx
from pathlib import Path
import pandas as pd

# Chemins
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"

# Chargement des données
nodes_df = pd.read_csv(RAW_DIR / "nodes.csv")
edges_df = pd.read_csv(RAW_DIR / "edges.csv")

def validate_graph_quality(nodes_df, edges_df):
    # Initialisation du contexte GE (ephemeral)
    context = gx.get_context()

    # Connection aux données et créations des lots
    data_source = context.data_sources.add_pandas("bronze_source")

    # Création des Data Assets
    nodes_asset = data_source.add_dataframe_asset(name="nodes_asset")
    edges_asset = data_source.add_dataframe_asset(name="edges_asset")

    # Batch definitions
    nodes_batch_def = nodes_asset.add_batch_definition_whole_dataframe("nodes_batch_def")
    edges_batch_def = edges_asset.add_batch_definition_whole_dataframe("edges_batch_def")

    # Batch
    nodes_batch = nodes_batch_def.get_batch(batch_parameters={"dataframe": nodes_df})
    edges_batch = edges_batch_def.get_batch(batch_parameters={"dataframe": edges_df})

    # Expectations
    # id unique et non nul
    expect_unique_id = gx.expectations.ExpectColumnValuesToBeUnique(column="id", severity="critical")
    expect_not_null_id = gx.expectations.ExpectColumnValuesToNotBeNull(column="id", severity="critical")

    # src et dst non nuls
    expect_not_null_src = gx.expectations.ExpectColumnValuesToNotBeNull(column="src", severity="critical")
    expect_not_null_dst = gx.expectations.ExpectColumnValuesToNotBeNull(column="dst", severity="critical")

    # Validation des données
    results = {
        "nodes_id_unique": nodes_batch.validate(expect_unique_id),
        "nodes_id_not_null": nodes_batch.validate(expect_not_null_id),
        "edges_src_not_null": edges_batch.validate(expect_not_null_src),
        "edges_dst_not_null": edges_batch.validate(expect_not_null_dst),
    }

    # Fonction d'affichage
    def print_validation(label, result):
        status = "✅ OK" if result["success"] else "❌ Échec"
        print(f"{label}: {status}")
    
    # Affichage des résultats
    print_validation("Unicité des IDs (nodes)", results["nodes_id_unique"])
    print_validation("Non-null ID (nodes)", results["nodes_id_not_null"])
    print_validation("Non-null src (edges)", results["edges_src_not_null"])
    print_validation("Non-null dst (edges)", results["edges_dst_not_null"])

    return results

if __name__== "__main__":
    validate_graph_quality(nodes_df, edges_df)