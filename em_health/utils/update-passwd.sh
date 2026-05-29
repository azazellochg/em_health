#!/bin/sh
# Update database passwords from docker/.env
# Usage:
#   sh em_health/utils/update-passwd.sh

set -eu

ENV_FILE="docker/.env"
PG_CONTAINER="emhealth-db"
GRAFANA_CONTAINER="emhealth-grafana"

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: $ENV_FILE not found" >&2
    exit 1
fi

manager=$(grep "^MANAGER_TYPE=" $ENV_FILE | cut -d'=' -f2)

if [ "$($manager inspect -f '{{.State.Running}}' $PG_CONTAINER 2>/dev/null)" != "true" ]; then
    echo "Error: container $PG_CONTAINER is not running" >&2
    exit 1
fi

pg_user=$(grep "^POSTGRES_USER=" $ENV_FILE | cut -d'=' -f2)
pg_admin_pw=$(grep "^POSTGRES_PASSWORD=" $ENV_FILE | cut -d'=' -f2)
pg_grafana_pw=$(grep "^POSTGRES_GRAFANA_PASSWORD=" $ENV_FILE | cut -d'=' -f2)
pg_emhealth_pw=$(grep "^POSTGRES_EMHEALTH_PASSWORD=" $ENV_FILE | cut -d'=' -f2)
pg_pganalyze_pw=$(grep "^POSTGRES_PGANALYZE_PASSWORD=" $ENV_FILE | cut -d'=' -f2)
grafana_admin_pw=$(grep "^GRAFANA_ADMIN_PASSWORD=" $ENV_FILE | cut -d'=' -f2)


printf "Update database and Grafana passwords using values from $ENV_FILE? (y/N) "
read -r reply

case "$reply" in
    [Yy]|[Yy][Ee][Ss])
        ;;
    *)
        echo "Aborted."
        exit 0
        ;;
esac

$manager exec -i $PG_CONTAINER psql -U $pg_user -v ON_ERROR_STOP=1 <<EOF
  ALTER USER "$pg_user" WITH PASSWORD '${pg_admin_pw}';
  ALTER USER grafana WITH PASSWORD '${pg_grafana_pw}';
  ALTER USER emhealth WITH PASSWORD '${pg_emhealth_pw}';
  ALTER USER pganalyze WITH PASSWORD '${pg_pganalyze_pw}';
EOF
echo "PostgreSQL passwords updated!"

if [ "$($manager inspect -f '{{.State.Running}}' $GRAFANA_CONTAINER 2>/dev/null)" != "true" ]; then
    echo "Error: container $GRAFANA_CONTAINER is not running" >&2
    exit 1
fi

$manager exec -it $GRAFANA_CONTAINER grafana cli admin reset-admin-password ${grafana_admin_pw}
echo "Grafana admin password updated!"

# recreate containers since compose only injects environment variables at container creation time
$manager compose -f docker/compose.yaml up -d --force-recreate
