#!/usr/bin/bash

DOCKER="/usr/bin/docker"
DOCKER_CONTAINER="backblaze-drive-stats-trino-iceberg-trino-1"
TRINO_CATALOG="drivestats_b2"
TRINO_DRIVESTATS_SCHEMA="ds_schema"

${DOCKER} exec -it ${DOCKER_CONTAINER} trino --catalog ${TRINO_CATALOG} --schema ${TRINO_DRIVESTATS_SCHEMA} \
--execute "SELECT date, model, count(model) AS model_count FROM drivestats GROUP by date, model HAVING model LIKE '%HUH72%' OR model LIKE '%WUH72%' OR model LIKE '%ST__000NM%' OR model LIKE '%MG__ACA__T%';" --output-format CSV_HEADER_UNQUOTED
