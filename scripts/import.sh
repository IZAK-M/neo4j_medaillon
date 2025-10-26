echo supp volumes containers
docker compose down -v

docker compose run --rm -e NEO4J_AUTH=none neo4j \
  neo4j-admin database import full \
  --overwrite-destination=true \
  --id-type=integer \
  --nodes=Entity=/import/nodes_neo4j.csv \
  --relationships=REL=/import/edges_neo4j.csv \
  -- \
  neo4j