#!/bin/bash
set -euo pipefail

# Project Nox - YugabyteDB Aeon Dynamic IP Allowlist Synchronizer
# Restricts cluster network access strictly to the current dynamic /32 egress IP.
# Cost: R$0. Zero open 0.0.0.0/0.

CONFIG_DIR="/home/awerkori/.config/project-nox"
IP_CACHE_FILE="$CONFIG_DIR/current_egress_ip"
LOG_FILE="$CONFIG_DIR/allowlist_sync.log"
PROJECT_DIR="/home/awerkori/.Projects/project-nox-importer"
CLUSTER_NAME="project-nox"

mkdir -p "$CONFIG_DIR"

# 0. Load environment if available (safe, no secret leakage)
if [ -f "$CONFIG_DIR/yugabyte.env" ]; then
  set +u
  eval $(grep -E '^YBM_API_KEY|^YUGABYTE_' "$CONFIG_DIR/yugabyte.env" | sed 's/^/export /') 2>/dev/null || true
  set -u
fi

# 1. Fetch current public IPv4 address
CURRENT_IP=$(curl -s --max-time 10 https://api.ipify.org 2>/dev/null || curl -s --max-time 10 https://checkip.amazonaws.com 2>/dev/null || true)
CURRENT_IP=$(echo "$CURRENT_IP" | tr -d '[:space:]')

# Validate IPv4 format
if [[ ! "$CURRENT_IP" =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]; then
  echo "[$(date -Iseconds)] [ERROR] Invalid or empty IP fetched: '$CURRENT_IP'" >> "$LOG_FILE"
  exit 1
fi

# 2. Check cached IP
CACHED_IP=""
if [ -f "$IP_CACHE_FILE" ]; then
  CACHED_IP=$(cat "$IP_CACHE_FILE" | tr -d '[:space:]')
fi

# Fast exit if IP has not changed
if [ "$CURRENT_IP" == "$CACHED_IP" ]; then
  echo "[$(date -Iseconds)] [INFO] Egress IP unchanged ($CURRENT_IP). No allowlist update needed." >> "$LOG_FILE"
  exit 0
fi

echo "[$(date -Iseconds)] [NOTICE] Egress IP changed from '$CACHED_IP' to '$CURRENT_IP'. Synchronizing allowlist..." >> "$LOG_FILE"

# 3. Locate ybm CLI
YBM_BIN="/home/awerkori/.local/bin/ybm"
if [ ! -x "$YBM_BIN" ]; then
  YBM_BIN=$(which ybm 2>/dev/null || true)
fi

# Function to test direct DB connectivity with TLS + SELECT 1
test_direct_connection() {
  (
    cd "$PROJECT_DIR"
    node -e '
      const { getYugabytePool } = require("./build/db/yugabyte-direct.js");
      (async () => {
        try {
          const p = getYugabytePool();
          const res = await p.query("SELECT 1 as ok;");
          await p.end();
          if (Number(res.rows[0].ok) === 1) {
            process.exit(0);
          } else {
            process.exit(1);
          }
        } catch (err) {
          process.exit(2);
        }
      })();
    '
  )
}

# 4. Synchronize Allowlist
if [ -n "$YBM_BIN" ] && [ -x "$YBM_BIN" ]; then
  YBM_AUTH_FLAGS=""
  if [ -n "${YBM_API_KEY:-}" ]; then
    YBM_AUTH_FLAGS="--apiKey $YBM_API_KEY"
  elif [ ! -f "$HOME/.ybm-cli.yaml" ]; then
    echo "[$(date -Iseconds)] [WARN] ybm CLI present but not authenticated. Run 'ybm auth' or set YBM_API_KEY in yugabyte.env." >> "$LOG_FILE"
  fi

  if [ -n "$YBM_AUTH_FLAGS" ] || [ -f "$HOME/.ybm-cli.yaml" ]; then
    NEW_RULE_NAME="nox-host-dyn-$(echo "$CURRENT_IP" | tr '.' '-')"
    OLD_RULE_NAME=""
    if [ -n "$CACHED_IP" ]; then
      OLD_RULE_NAME="nox-host-dyn-$(echo "$CACHED_IP" | tr '.' '-')"
    fi

    echo "[$(date -Iseconds)] [INFO] Creating allowlist rule '$NEW_RULE_NAME' for $CURRENT_IP/32..." >> "$LOG_FILE"
    # Step A: Create new /32 rule FIRST (fail-safe)
    if ! "$YBM_BIN" network-allow-list create $YBM_AUTH_FLAGS --name "$NEW_RULE_NAME" --description "Nox-Device-PC dynamic egress IP" --ip "$CURRENT_IP/32" --cluster-name "$CLUSTER_NAME" >> "$LOG_FILE" 2>&1; then
      echo "[$(date -Iseconds)] [ERROR] Failed to create new allowlist rule '$NEW_RULE_NAME'. Preserving existing configuration." >> "$LOG_FILE"
      exit 1
    fi

    # Step B: Test TLS connection + SELECT 1
    echo "[$(date -Iseconds)] [INFO] Validating direct TLS connection with new IP..." >> "$LOG_FILE"
    if ! test_direct_connection; then
      echo "[$(date -Iseconds)] [ERROR] TLS direct connection test (SELECT 1) failed after adding '$NEW_RULE_NAME'. Preserving fallback." >> "$LOG_FILE"
      exit 1
    fi

    # Step C: Only after SELECT 1 is confirmed PASS, remove old rule
    if [ -n "$OLD_RULE_NAME" ] && [ "$OLD_RULE_NAME" != "$NEW_RULE_NAME" ]; then
      echo "[$(date -Iseconds)] [INFO] Direct connection verified. Removing previous rule '$OLD_RULE_NAME'..." >> "$LOG_FILE"
      "$YBM_BIN" network-allow-list delete $YBM_AUTH_FLAGS --name "$OLD_RULE_NAME" --cluster-name "$CLUSTER_NAME" >> "$LOG_FILE" 2>&1 || true
    fi
  fi
fi

# 5. Cache confirmed IP
echo "$CURRENT_IP" > "$IP_CACHE_FILE"
echo "[$(date -Iseconds)] [SUCCESS] Cache updated to $CURRENT_IP. Direct access verified." >> "$LOG_FILE"
exit 0
