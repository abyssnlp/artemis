#!/bin/sh
set -e

echo "Waiting for Kibana to be available..."
until curl -sf "http://kibana:5601/api/status" | grep -q '"level":"available"'; do
  sleep 5
done

echo "Creating Data View 'airflow-logs-*'..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "http://kibana:5601/api/data_views/data_view" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -d '{
    "data_view": {
      "title": "airflow-logs-*",
      "name": "Airflow Logs",
      "timeFieldName": "@timestamp"
    }
  }')

if [ "$HTTP_CODE" = "200" ]; then
  echo "Data View created."
elif [ "$HTTP_CODE" = "409" ]; then
  echo "Data View already exists."
else
  echo "Data View creation returned HTTP $HTTP_CODE" >&2
  exit 1
fi
