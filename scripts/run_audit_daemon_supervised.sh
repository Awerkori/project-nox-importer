#!/bin/bash
set -euo pipefail

PIDFILE="/home/awerkori/.Projects/project-nox-importer/scratch/audit_daemon.pid"
LOGFILE="/home/awerkori/.Projects/project-nox-importer/scratch/audit_daemon.log"

cd /home/awerkori/.Projects/project-nox-importer

# Check if already running
if [ -f "$PIDFILE" ]; then
  OLD_PID=$(cat "$PIDFILE" 2>/dev/null || echo "")
  if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
    echo "Audit daemon already running with PID $OLD_PID"
    exit 0
  fi
fi

echo "$$" > "$PIDFILE"

trap 'rm -f "$PIDFILE"; exit 0' SIGINT SIGTERM EXIT

echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Starting supervised audit daemon..." >> "$LOGFILE"

while true; do
  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Launching audit daemon cycle..." >> "$LOGFILE"
  npx tsx scripts/audit_full_catalog_daemon.ts >> "$LOGFILE" 2>&1 || true
  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] Audit daemon process exited. Sleeping 5s before restart..." >> "$LOGFILE"
  sleep 5
done
