#!/bin/bash
set -e

POOL="${WORK_POOL_NAME:-local-pool}"

echo "→ Waiting for Prefect API at ${PREFECT_API_URL}..."
until python -c "import urllib.request; urllib.request.urlopen('${PREFECT_API_URL}/health')" 2>/dev/null; do
  sleep 3
done
echo "✓ Prefect API is ready"

echo "→ Creating work pool '${POOL}' (process type)..."
prefect work-pool create "${POOL}" --type docker 2>/dev/null \
  || echo "  Work pool '${POOL}' already exists, continuing."

echo "→ Deploying flows from prefect.yaml..."
prefect --no-prompt deploy --all 2>/dev/null \
  || echo "  No deployments configured yet, skipping."

echo "→ Starting worker on pool '${POOL}'..."
exec prefect worker start --pool "${POOL}" --work-queue default
