#!/bin/sh

KEYS_DIR="/vault/data"

echo "Waiting for Vault to start..."
while true; do
  vault status > /dev/null 2>&1
  CODE=$?
  [ $CODE -eq 0 ] || [ $CODE -eq 2 ] && break
  sleep 2
done

while true; do
  vault status > /dev/null 2>&1
  VAULT_CODE=$?

  if [ $VAULT_CODE -eq 2 ]; then
    IS_INIT=$(vault status 2>/dev/null | grep '^Initialized' | awk '{print $2}')

    if [ "$IS_INIT" = "false" ]; then
      echo "Initializing Vault..."
      if INIT_OUT=$(vault operator init -key-shares=1 -key-threshold=1 2>&1); then
        UNSEAL_KEY=$(echo "$INIT_OUT" | grep 'Unseal Key 1:' | awk '{print $NF}')
        ROOT_TOKEN=$(echo "$INIT_OUT" | grep 'Initial Root Token:' | awk '{print $NF}')
        if [ -n "$UNSEAL_KEY" ] && [ -n "$ROOT_TOKEN" ]; then
          echo "$UNSEAL_KEY" > "$KEYS_DIR/.unseal_key"
          echo "$ROOT_TOKEN" > "$KEYS_DIR/.root_token"
          echo "Initialized."
        else
          echo "Vault init did not return expected credentials."
        fi
      else
        echo "$INIT_OUT"
      fi
    fi

    UNSEAL_KEY=$(cat "$KEYS_DIR/.unseal_key" 2>/dev/null)
    if [ -n "$UNSEAL_KEY" ]; then
      echo "Unsealing Vault..."
      vault operator unseal "$UNSEAL_KEY" > /dev/null
      echo "Unsealed."
    fi
  elif [ $VAULT_CODE -eq 0 ]; then
    ROOT_TOKEN=$(cat "$KEYS_DIR/.root_token" 2>/dev/null)
    if [ -n "$ROOT_TOKEN" ]; then
      export VAULT_TOKEN="$ROOT_TOKEN"

      vault secrets list 2>/dev/null | grep -q '^secret/' || \
        vault secrets enable -path=secret kv-v2 > /dev/null

      vault token lookup airflow > /dev/null 2>&1 || \
        vault token create -id=airflow -policy=root -no-default-policy -orphan -ttl=87600h > /dev/null

      vault kv get secret/connections/postgres_default > /dev/null 2>&1 || \
        vault kv put secret/connections/postgres_default \
          conn_uri="postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

      vault kv get secret/connections/redis_default > /dev/null 2>&1 || \
        vault kv put secret/connections/redis_default \
          conn_uri="redis://:@redis:6379/0"

      vault kv get secret/connections/clickhouse_default > /dev/null 2>&1 || \
        vault kv put secret/connections/clickhouse_default \
          conn_uri="clickhouse://default:test123@clickhouse:8123/default"

      vault kv get secret/variables/airflow_env > /dev/null 2>&1 || \
        vault kv put secret/variables/airflow_env value=development

      echo "Vault ready."
    fi
  fi

  sleep 10
done
