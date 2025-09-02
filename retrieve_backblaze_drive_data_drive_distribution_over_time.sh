#!/usr/bin/bash

DOCKER="/usr/bin/docker"
DOCKER_CONTAINER="backblaze-drive-stats-trino-iceberg-trino-1"
TRINO_CATALOG="drivestats_b2"
TRINO_DRIVESTATS_SCHEMA="ds_schema"

${DOCKER} exec -it ${DOCKER_CONTAINER} trino --catalog ${TRINO_CATALOG} --schema ${TRINO_DRIVESTATS_SCHEMA} \
--execute "SELECT date, model, count(model) AS model_count FROM drivestats GROUP by date, model HAVING model like '%HUH72%' OR model like '%WUH72%' OR model like 'ST????NM%' or model like '%MG??ACA%';" --output-format CSV_HEADER_UNQUOTED
