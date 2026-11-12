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

manager=$(grep "^MANAGER_TYPE=" "$ENV_FILE" | cut -d'=' -f2-)
case "$manager" in
    docker|podman)
        ;;
    *)
        echo "Error: MANAGER_TYPE in $ENV_FILE must be 'docker' or 'podman'" >&2
        exit 1
        ;;
esac

if [ "$("$manager" inspect -f '{{.State.Running}}' "$PG_CONTAINER" 2>/dev/null)" != "true" ]; then
    echo "Error: container $PG_CONTAINER is not running" >&2
    exit 1
fi

pg_user=$(grep "^POSTGRES_USER=" "$ENV_FILE" | cut -d'=' -f2-)
pg_admin_pw=$(grep "^POSTGRES_PASSWORD=" "$ENV_FILE" | cut -d'=' -f2-)
pg_grafana_pw=$(grep "^POSTGRES_GRAFANA_PASSWORD=" "$ENV_FILE" | cut -d'=' -f2-)
pg_emhealth_pw=$(grep "^POSTGRES_EMHEALTH_PASSWORD=" "$ENV_FILE" | cut -d'=' -f2-)
pg_pganalyze_pw=$(grep "^POSTGRES_PGANALYZE_PASSWORD=" "$ENV_FILE" | cut -d'=' -f2-)
grafana_admin_pw=$(grep "^GRAFANA_ADMIN_PASSWORD=" "$ENV_FILE" | cut -d'=' -f2-)


printf 'Update database and Grafana passwords using values from %s? (y/N) ' "$ENV_FILE"
read -r reply

case "$reply" in
    [Yy]|[Yy][Ee][Ss])
        ;;
    *)
        echo "Aborted."
        exit 0
        ;;
esac

# Passwords go to the container as environment variables, not as command-line arguments
# (those are visible to other users in 'ps'). psql quotes them (:'var'), so any character is safe.
PW_ADMIN="$pg_admin_pw" PW_GRAFANA="$pg_grafana_pw" PW_EMHEALTH="$pg_emhealth_pw" PW_PGANALYZE="$pg_pganalyze_pw" \
    "$manager" exec -i -e PW_ADMIN -e PW_GRAFANA -e PW_EMHEALTH -e PW_PGANALYZE "$PG_CONTAINER" \
    psql -U "$pg_user" -v ON_ERROR_STOP=1 -v pg_user="$pg_user" <<'EOF'
\getenv pw_admin PW_ADMIN
\getenv pw_grafana PW_GRAFANA
\getenv pw_emhealth PW_EMHEALTH
\getenv pw_pganalyze PW_PGANALYZE
ALTER USER :"pg_user" WITH PASSWORD :'pw_admin';
ALTER USER grafana WITH PASSWORD :'pw_grafana';
ALTER USER emhealth WITH PASSWORD :'pw_emhealth';
ALTER USER pganalyze WITH PASSWORD :'pw_pganalyze';
EOF
echo "PostgreSQL passwords updated!"

if [ "$("$manager" inspect -f '{{.State.Running}}' "$GRAFANA_CONTAINER" 2>/dev/null)" != "true" ]; then
    echo "Error: container $GRAFANA_CONTAINER is not running" >&2
    exit 1
fi

# The password is passed on stdin, not as a command-line argument
printf '%s' "$grafana_admin_pw" | "$manager" exec -i "$GRAFANA_CONTAINER" \
    grafana cli admin reset-admin-password --password-from-stdin
echo "Grafana admin password updated!"

# recreate containers since compose only injects environment variables at container creation time
if [ "$manager" = "podman" ]; then
  podman-compose -f docker/compose.yaml up -d --force-recreate
else
  docker compose -f docker/compose.yaml up -d --force-recreate
fi
