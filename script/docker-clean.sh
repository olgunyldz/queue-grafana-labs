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
for volume_name in kafka flink-checkpoints prometheus loki alloy grafana; do
  mkdir -p "$VOLUME_DIR/$volume_name"
  # Tracked placeholder that keeps the directory in Git (see .gitignore); rm -rf above removed it.
  touch "$VOLUME_DIR/$volume_name/.gitkeep"
done

echo "Docker services and project volumes cleaned."
