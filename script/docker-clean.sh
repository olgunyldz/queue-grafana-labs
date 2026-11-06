#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

echo "Stopping Compose services..."
docker compose down -v --remove-orphans

VOLUME_DIR="$PROJECT_DIR/volumes"

if [[ ! -d "$VOLUME_DIR" ]]; then
  echo "No project volumes directory found: $VOLUME_DIR"
  exit 0
fi

echo "Removing project data under $VOLUME_DIR..."
for volume_name in \
  kafka \
  flink-checkpoints \
  prometheus \
  loki \
  alloy \
  grafana
do
  volume_path="$VOLUME_DIR/$volume_name"
  if [[ -d "$volume_path" ]]; then
    rm -rf -- "$volume_path"
    echo "Removed $volume_path"
  fi
done

echo "Recreating empty project volume directories..."
mkdir -p \
  "$VOLUME_DIR/kafka" \
  "$VOLUME_DIR/flink-checkpoints" \
  "$VOLUME_DIR/prometheus" \
  "$VOLUME_DIR/loki" \
  "$VOLUME_DIR/alloy" \
  "$VOLUME_DIR/grafana"

echo "Docker services and project volumes cleaned."
