#!/usr/bin/env bash
# getoken netcup daily backup — deduplicated + incremental + extreme compression.
#
# Captures the three production data stores into ONE borg archive/day:
#   - getoken  DB  (relay-db-postgres-1)  pg_dump plain SQL
#   - sub2api  DB  (relay-db-postgres-1)  pg_dump plain SQL  (authoritative pool)
#   - panel.db     (getoken-panel)        sqlite online .backup + integrity_check
#
# borg gives incremental storage (only changed chunks persisted across days) and
# zstd-19 compression ("极致" ratio at sane CPU). Retention pruned automatically.
# Mirrors the retired gcp/do-new-1 job: consistent online snapshot, integrity
# gate, metadata-tagged archive names, bounded retention.
set -euo pipefail
umask 077

export BORG_REPO=/opt/getoken-consolidated/backups/borg-repo
export BORG_PASSPHRASE=""          # local repo, encryption=none (restorable w/o key mgmt)
LOG=/var/log/getoken-backup.log
STAMP="$(date +%Y%m%d-%H%M%S)"
# Debian /tmp may be tmpfs; database dumps must not consume service RAM.
STAGE_ROOT=/opt/getoken-consolidated/backups/staging
install -d -m 0700 "$STAGE_ROOT"
exec 9>"$STAGE_ROOT/.backup.lock"
flock -n 9 || { echo 'Another Getoken backup is running' >&2; exit 1; }
case "$(findmnt -n -o FSTYPE --target "$STAGE_ROOT")" in
  ''|tmpfs|ramfs) echo 'Backup staging requires a disk filesystem' >&2; exit 1 ;;
esac
STAGE="$(mktemp -d "$STAGE_ROOT/gk-bk.XXXXXX")"
trap 'rm -rf "$STAGE"' EXIT
exec >>"$LOG" 2>&1
echo "==================== backup $STAMP START ===================="

# 1) Postgres logical dumps (portable, restore into any PG 16). --no-owner/-acl
#    keeps them role-agnostic for cross-host restore.
docker exec -i relay-db-postgres-1 pg_dump -U postgres -d getoken --no-owner --no-privileges > "$STAGE/getoken.sql"
docker exec -i relay-db-postgres-1 pg_dump -U postgres -d sub2api  --no-owner --no-privileges > "$STAGE/sub2api.sql"

# 2) panel.db consistent online snapshot + integrity gate (fail-closed).
docker exec -i getoken-panel python3 - <<'PY'
import sqlite3
src = sqlite3.connect('/app/data/panel.db')
dst = sqlite3.connect('/app/data/.panel.bak.db')
with dst:
    src.backup(dst)
ic = src.execute('PRAGMA integrity_check').fetchone()[0]
src.close(); dst.close()
if ic != 'ok':
    raise SystemExit('panel.db integrity_check=%r' % ic)
print('panel.db integrity ok')
PY
cp /opt/getoken-consolidated/getoken-panel/data/.panel.bak.db "$STAGE/panel.db"
rm -f /opt/getoken-consolidated/getoken-panel/data/.panel.bak.db

echo "-- staged sizes --"; ls -la "$STAGE"

# 3) One archive/day; auto skips incompressible chunks, else zstd,19.
borg create --stats --compression auto,zstd,19 "::getoken-$STAMP" "$STAGE"

# 4) Bounded retention + reclaim.
borg prune --stats --glob-archives 'getoken-*' --keep-daily=14 --keep-weekly=8 --keep-monthly=6
borg compact

echo "-- repo archives (tail) --"; borg list --glob-archives 'getoken-*' | tail -5
echo "==================== backup $STAMP DONE ===================="
