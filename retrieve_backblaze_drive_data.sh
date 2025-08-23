#!/usr/bin/bash

DOCKER="/usr/bin/docker"
DOCKER_CONTAINER="backblaze-drive-stats-trino-iceberg-trino-1"
TRINO_CATALOG="drivestats_b2"
TRINO_DRIVESTATS_SCHEMA="ds_schema"

${DOCKER} exec -it ${DOCKER_CONTAINER} trino --catalog ${TRINO_CATALOG} --schema ${TRINO_DRIVESTATS_SCHEMA} --execute "SELECT model, date, COUNT(serial_number) AS drive_count, SUM(failure) AS failure_count FROM drivestats GROUP BY model, date ORDER BY model, date;" --output-format CSV_HEADER_UNQUOTED
