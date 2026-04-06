#!/bin/sh
set -e

until vault status > /dev/null 2>&1; do
  echo "Waiting for Vault to be ready..."
  sleep 2
done

echo "Vault is ready. Writing sample Airflow secrets..."

vault kv put secret/connections/postgres_default \
  conn_uri="postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

vault kv put secret/connections/redis_default \
  conn_uri="redis://:@redis:6379/0"

vault kv put secret/variables/airflow_env \
  value=development

echo "Vault initialization complete."
echo ""
echo "Connections written:"
echo "  secret/connections/postgres_default"
echo "  secret/connections/redis_default"
echo ""
echo "Variables written:"
echo "  secret/variables/airflow_env"
echo ""
echo "NOTE: Vault-managed connections and variables are resolved at task runtime only."
echo "They do not appear in the Airflow UI. Use the Vault UI at http://localhost:8200 to manage them."
echo "To verify a connection resolves: docker exec <scheduler-container> airflow connections get <conn_id>"
echo "To add a new connection: vault kv put secret/connections/<conn_id> conn_uri=\"<uri>\""
echo "To add a new variable:   vault kv put secret/variables/<key> value=<value>"
