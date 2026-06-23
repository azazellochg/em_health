#!/bin/sh
# Generate passwords and optionally update docker/.env
# Usage:
#   sh em_health/utils/generate-keys.sh

set -eu

ENV_FILE="docker/.env"

gen_hex() {
    openssl rand -hex 16
}

if ! command -v openssl >/dev/null 2>&1; then
    echo "Error: openssl is required but not found."
    exit 1
fi

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: $ENV_FILE not found" >&2
    exit 1
fi

pg_admin_pw=$(gen_hex)
pg_grafana_pw=$(gen_hex)
pg_emhealth_pw=$(gen_hex)
pg_pganalyze_pw=$(gen_hex)
grafana_admin_pw=$(gen_hex)
grafana_rnd_key=$(gen_hex)

echo ""
echo "POSTGRES_PASSWORD=${pg_admin_pw}"
echo "POSTGRES_GRAFANA_PASSWORD=${pg_grafana_pw}"
echo "POSTGRES_EMHEALTH_PASSWORD=${pg_emhealth_pw}"
echo "POSTGRES_PGANALYZE_PASSWORD=${pg_pganalyze_pw}"
echo "GRAFANA_ADMIN_PASSWORD=${grafana_admin_pw}"
echo "GRAFANA_RENDER_TOKEN=${grafana_rnd_key}"
echo ""

printf "Update %s? (y/N) " "$ENV_FILE"
read -r reply

case "$reply" in
    [Yy]|[Yy][Ee][Ss])
        ;;
    *)
        echo "Aborted."
        exit 0
        ;;
esac

sed \
    -i.old \
    -e "s|^POSTGRES_PASSWORD=.*$|POSTGRES_PASSWORD=${pg_admin_pw}|" \
    -e "s|^POSTGRES_GRAFANA_PASSWORD=.*$|POSTGRES_GRAFANA_PASSWORD=${pg_grafana_pw}|" \
    -e "s|^POSTGRES_EMHEALTH_PASSWORD=.*$|POSTGRES_EMHEALTH_PASSWORD=${pg_emhealth_pw}|" \
    -e "s|^POSTGRES_PGANALYZE_PASSWORD=.*$|POSTGRES_PGANALYZE_PASSWORD=${pg_pganalyze_pw}|" \
    -e "s|^GRAFANA_ADMIN_PASSWORD=.*$|GRAFANA_ADMIN_PASSWORD=${grafana_admin_pw}|" \
    -e "s|^GRAFANA_RENDER_TOKEN=.*$|GRAFANA_RENDER_TOKEN=${grafana_rnd_key}|" \
    $ENV_FILE

echo "Done. Don't forget to run 'sh em_health/utils/update-passwd.sh'!"
