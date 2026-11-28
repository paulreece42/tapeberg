#!/usr/bin/env bash
# Tapeberg end-to-end test against the mhvtl dev rig.
#
# Exercises the full basic loop:
#   ingest -> worker archive (incl. a tape swap + EOM retry)
#   -> restore from tape 1 (filemark seek) and tape 2
#   -> byte-identical get
#   -> pin expiry sweep back to GLACIER
#   -> manual-changer mode with a scripted "human" ack
#
# Prereqs: mhvtl.target up, PostgreSQL catalog migrated+seeded, .env
# with TAPEBERG_DB_URL, lsscsi/mtx/mt in PATH, go toolchain.

set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d /tmp/tapeberg-e2e.XXXXXX)
BIN="$WORK/tapeberg"
trap 'rc=$?; rm -rf "$WORK"; scratch_catalog_exit $rc' EXIT

say() { printf '\n=== %s ===\n' "$*"; }
fail() { printf '\nE2E FAIL: %s\n' "$*" >&2; exit 1; }
. "$ROOT/test/scratch-catalog.sh"

say "building tapeberg"
cd "$ROOT"
go build -o "$BIN" . || fail "build failed"

say "discovering VTL devices"
CHANGER_DEV=$(lsscsi -g | awk '$2=="mediumx"{print $NF}')
ST=$(lsscsi -g | awk '$2=="tape" && $3=="IBM"{print $(NF-1); exit}')
DRIVE=${ST/\/st/\/nst}
[ -n "$CHANGER_DEV" ] || fail "no mediumx device — is mhvtl up?"
[ -n "$DRIVE" ] || fail "no VTL tape drive found"
echo "changer=$CHANGER_DEV drive=$DRIVE"

set -a; . "$ROOT/.env"; set +a
# Hermetic: this test's "human" is a script, not a phone. Never read the
# host's /etc/tapeberg.conf, and strip notification credentials so the
# manual-mode section cannot ping a real human.
export TAPEBERG_CONF=/dev/null
unset TAPEBERG_TELEGRAM_TOKEN TAPEBERG_TELEGRAM_CHAT TAPEBERG_WEBHOOK_URL TAPEBERG_WEBHOOK_TOKEN
export TAPEBERG_DRIVE="$DRIVE"
export TAPEBERG_CHANGER=robot
export TAPEBERG_CHANGER_DEV="$CHANGER_DEV"
export TAPEBERG_POOL=vtl
export TAPEBERG_BUFFER="$WORK/buffer"
mkdir -p "$TAPEBERG_BUFFER"

say "isolating the catalog (scratch schema — the real catalog's rows are never touched)"
scratch_catalog_up

say "schema.sql matches db/migrations"
command -v pg_dump >/dev/null || fail "pg_dump not installed"
CHK_SCHEMA=tapeberg_e2e_schemachk
PGOPTIONS= psql "$TAPEBERG_DB_URL" -Xq -c "DROP SCHEMA IF EXISTS $CHK_SCHEMA CASCADE" >/dev/null
PGOPTIONS= psql "$TAPEBERG_DB_URL" -Xq -c "CREATE SCHEMA $CHK_SCHEMA" >/dev/null \
  || fail "schema-check: cannot create $CHK_SCHEMA"
PGOPTIONS="-c search_path=$CHK_SCHEMA" psql "$TAPEBERG_DB_URL" -Xq -v ON_ERROR_STOP=1 -f "$ROOT/schema.sql" >/dev/null \
  || fail "schema.sql does not apply"
norm_dump() {
  pg_dump "$TAPEBERG_DB_URL" --schema-only --no-owner --no-privileges -n "$1" \
    | sed -e "s/$1\.//g" -e "s/; Schema: $1;/; Schema: -;/g" \
    | grep -v "^SELECT pg_catalog.set_config" | grep -v "^-- Dumped" \
    | grep -v "^-- Name: $1; Type: SCHEMA" | grep -v "^CREATE SCHEMA $1;" \
    | grep -v 'connect' | grep -v 'restrict'
}
# The scratch schema was just built from migrations+seed; a schema-only
# dump is therefore exactly the migrations' DDL. If schema.sql drifted,
# this fails after anyone edits a migration without regenerating
# (scripts/gen-schema.sh).
norm_dump "$SCRATCH_SCHEMA" | diff - <(norm_dump "$CHK_SCHEMA") > "$WORK/schema-diff" \
  || fail "schema.sql has drifted from db/migrations — run scripts/gen-schema.sh (diff in $WORK/schema-diff)"
PGOPTIONS= psql "$TAPEBERG_DB_URL" -Xq -c "DROP SCHEMA IF EXISTS $CHK_SCHEMA CASCADE" >/dev/null
echo "schema.sql matches the migrations"
PSQL="psql $TAPEBERG_DB_URL -tAq"

say "blanking VTL media (erase prior test data — capacity persists otherwise)"
mtx -f "$CHANGER_DEV" unload >/dev/null 2>&1 || true  # tolerate a leftover load
for bc in TB0001L5 TB0002L5 TB0003L5; do
  slot=$(mtx -f "$CHANGER_DEV" status | grep "VolumeTag=$bc" | sed 's/.*Storage Element \([0-9]*\):Full.*/\1/')
  mtx -f "$CHANGER_DEV" load "$slot" >/dev/null
  sleep 1
  mt -f "$DRIVE" rewind
  mt -f "$DRIVE" erase
  mt -f "$DRIVE" rewind
  mtx -f "$CHANGER_DEV" unload >/dev/null
done

say "resetting catalog to a clean slate (vtl pool + e2e keys only — never the real rig's rows)"
$PSQL -c "DELETE FROM retrieve_requests WHERE object_id IN (SELECT id FROM objects WHERE key LIKE 'e2e-%');
DELETE FROM archive_requests WHERE object_id IN (SELECT id FROM objects WHERE key LIKE 'e2e-%');
DELETE FROM tape_files WHERE tape_id IN (SELECT id FROM tapes WHERE pool_id=(SELECT id FROM tape_pools WHERE name='vtl'));
UPDATE tapes SET state='active', written_bytes=0 WHERE pool_id=(SELECT id FROM tape_pools WHERE name='vtl');
UPDATE tape_pools SET ec_k=NULL, ec_m=NULL;
DELETE FROM objects WHERE key LIKE 'e2e-%';" >/dev/null

say "generating 5 x 20MiB incompressible files (forces EOM + tape swap)"
for i in 1 2 3 4 5; do
  dd if=/dev/urandom of="$WORK/src$i" bs=1M count=20 status=none
done

say "CLI ingest follows the bucket's pool"
# The scratch catalog's 'tapeberg' bucket is seeded into 'default'; this
# rig's worker serves 'vtl'. Ingesting without --bucket must refuse the
# mismatch rather than file a vtl archive under a default-bound bucket.
if "$BIN" ingest "$WORK/src1" e2e-mismatch 2>"$WORK/mismatch"; then fail "ingest into a bucket bound to another pool must be refused"; fi
grep -- '--bucket' "$WORK/mismatch" >/dev/null || fail "mismatch refusal should point at --bucket: $(cat "$WORK/mismatch")"
echo "  refused: $(cat "$WORK/mismatch")"
"$BIN" bucket set-pool tapeberg vtl >/dev/null || fail "rebind tapeberg -> vtl (scratch catalog)"

say "ingesting 5 objects"
for i in 1 2 3 4 5; do "$BIN" ingest "$WORK/src$i" "e2e-file$i"; done
p=$($PSQL -c "SELECT count(*) FROM archive_requests r JOIN objects o ON o.id=r.object_id WHERE r.state='pending' AND o.key LIKE 'e2e-%'")
[ "$p" = 5 ] || fail "expected 5 pending archive requests, got $p"

say "worker pass 1: drain the archive queue"
"$BIN" worker

n1=$($PSQL -c "SELECT count(*) FROM tape_files tf JOIN tapes t ON t.id=tf.tape_id WHERE t.barcode='TB0001L5'")
n2=$($PSQL -c "SELECT count(*) FROM tape_files tf JOIN tapes t ON t.id=tf.tape_id WHERE t.barcode='TB0002L5'")
[ "$n1" = 4 ] || fail "expected 4 objects on TB0001L5, got $n1"
[ "$n2" = 1 ] || fail "expected 1 object on TB0002L5, got $n2"
s=$($PSQL -c "SELECT state FROM tapes WHERE barcode='TB0001L5'")
[ "$s" = full ] || fail "TB0001L5 should be state=full after EOM, got '$s'"
w=$($PSQL -c "SELECT count(*) FROM archive_requests r JOIN objects o ON o.id=r.object_id WHERE r.state='written' AND o.key LIKE 'e2e-%'")
[ "$w" = 5 ] || fail "expected 5 written e2e requests, got $w"
g=$($PSQL -c "SELECT count(*) FROM objects WHERE storage_class='GLACIER' AND key LIKE 'e2e-%'")
[ "$g" = 5 ] || fail "expected 5 GLACIER objects, got $g"
[ ! -e "$TAPEBERG_BUFFER/e2e-file1" ] || fail "disk copies should be dropped after archive"
echo "4 objects on TB0001L5 (EOM-retried), 1 on TB0002L5 — swap verified"

say "worker pass 2: restore from tape 1 (fsf seek) and tape 2"
"$BIN" restore e2e-file1 3
"$BIN" restore e2e-file4 3   # archive_id 4: exercises mt fsf positioning
"$BIN" restore e2e-file5 3   # lives on TB0002L5
"$BIN" worker

for i in 1 4 5; do
  s=$($PSQL -c "SELECT state FROM retrieve_requests r JOIN objects o ON o.id=r.object_id WHERE o.key='e2e-file$i'")
  [ "$s" = restored ] || fail "e2e-file$i restore state=$s"
  "$BIN" get e2e-file$i > "$WORK/out$i"
  cmp "$WORK/src$i" "$WORK/out$i" || fail "e2e-file$i differs from source bytes"
done
echo "3 restores byte-identical to sources"

say "restore pool follows tape placement, not the caller's TAPEBERG_POOL"
# A pool name no worker serves anywhere: even if this regresses, the
# request can never reach a real rig's worker (which would ask a human
# to load a virtual tape into a physical drive — the 0004/0005 incident).
MIS=e2e-misroute
TAPEBERG_POOL=$MIS "$BIN" restore e2e-file3 1
rp=$($PSQL -c "SELECT r.pool FROM retrieve_requests r JOIN objects o ON o.id=r.object_id WHERE o.key='e2e-file3' AND r.state='pending'")
[ "$rp" = vtl ] || fail "restore with TAPEBERG_POOL=$MIS queued in pool '$rp', want vtl (where e2e-file3's tape is)"
"$BIN" worker
s=$($PSQL -c "SELECT r.state FROM retrieve_requests r JOIN objects o ON o.id=r.object_id WHERE o.key='e2e-file3' ORDER BY r.id DESC LIMIT 1")
[ "$s" = restored ] || fail "placement-routed restore of e2e-file3 state=$s"
"$BIN" get e2e-file3 > "$WORK/out3"
cmp "$WORK/src3" "$WORK/out3" || fail "e2e-file3 differs from source bytes"
echo "restore queued by placement (vtl) despite TAPEBERG_POOL=$MIS; byte-identical"

say "worker re-routes a restore already queued in the wrong pool"
$PSQL -c "INSERT INTO retrieve_requests (object_id, pin_days, pool) SELECT id, 1, '$MIS' FROM objects WHERE key='e2e-file3'" >/dev/null
out=$(TAPEBERG_POOL=$MIS "$BIN" worker)
echo "$out"
echo "$out" | grep "re-routed $MIS -> vtl" >/dev/null || fail "the $MIS worker did not re-route the misqueued restore"
rp=$($PSQL -c "SELECT r.pool FROM retrieve_requests r JOIN objects o ON o.id=r.object_id WHERE o.key='e2e-file3' AND r.state='pending'")
[ "$rp" = vtl ] || fail "re-routed restore sits in pool '$rp', want vtl"
# Leave nothing pending for later passes.
$PSQL -c "UPDATE retrieve_requests SET state='cancelled' WHERE state='pending' AND object_id=(SELECT id FROM objects WHERE key='e2e-file3')" >/dev/null
echo "misqueued restore handed to the pool that holds its tape"

say "crash recovery: stale in-flight jobs are reaped at startup (issue #33)"
# A worker killed mid-job leaves rows in-flight forever: the archive
# stays 'assigned' (object never reaches tape), the restore 'restoring'
# (clients retry and get RestoreAlreadyInProgress for eternity).
# Startup — holding the exclusive buffer lock — reaps stale in-flight
# rows back to pending, and the same pass redoes them. Both fixtures
# get assigned_at/updated_at 2-3h in the past, like a worker that died
# before noon. (This section must not touch e2e-file1/2/3/4/5: the
# manual-changer section below drives them.)
dd if=/dev/urandom of="$WORK/src6" bs=1M count=5 status=none
"$BIN" ingest "$WORK/src6" e2e-file6 >/dev/null
$PSQL -q -c "UPDATE archive_requests SET state='assigned', tape_id=NULL, assigned_at=now() - interval '2 hours', updated_at=now() - interval '3 hours' WHERE object_id=(SELECT id FROM objects WHERE key='e2e-file6')" \
  || fail "stale-archive fixture failed"
out=$("$BIN" worker)
echo "$out"
echo "$out" | grep -q "recovered 1 stale archive" || fail "worker did not reap the stale archive job"
wa=$($PSQL -c "SELECT r.state FROM archive_requests r JOIN objects o ON o.id=r.object_id WHERE o.key='e2e-file6'")
[ "$wa" = written ] || fail "reaped stale archive was not written, state=$wa"
[ "$($PSQL -c "SELECT storage_class FROM objects WHERE key='e2e-file6'")" = GLACIER ] || fail "recovered archive should be GLACIER on tape"
# Now the same drill for a restore: e2e-file6 is on tape.
"$BIN" restore e2e-file6 3   # pin 3 days: survives the expiry sweep below
$PSQL -q -c "UPDATE retrieve_requests SET state='restoring', assigned_at=now() - interval '2 hours', updated_at=now() - interval '3 hours' WHERE object_id=(SELECT id FROM objects WHERE key='e2e-file6')" \
  || fail "stale-restore fixture failed"
out=$("$BIN" worker)
echo "$out"
echo "$out" | grep -q "1 stale restore" || fail "worker did not reap the stale restore job"
s=$($PSQL -c "SELECT r.state FROM retrieve_requests r JOIN objects o ON o.id=r.object_id WHERE o.key='e2e-file6' ORDER BY r.id DESC LIMIT 1")
[ "$s" = restored ] || fail "reaped stale restore was not redone, state=$s"
"$BIN" get e2e-file6 > "$WORK/out6recovered"
cmp "$WORK/src6" "$WORK/out6recovered" || fail "recovered restore reads wrong bytes"
echo "stale archive and stale restore both reaped and redone in one pass; bytes verified"

say "worker pass 3: pin expiry sweep"
$PSQL -c "UPDATE retrieve_requests SET expires_at = now() - interval '1 hour' WHERE object_id=(SELECT id FROM objects WHERE key='e2e-file1')" >/dev/null
"$BIN" worker
s=$($PSQL -c "SELECT storage_class FROM objects WHERE key='e2e-file1'")
[ "$s" = GLACIER ] || fail "e2e-file1 should return to GLACIER after pin expiry, got $s"
[ ! -e "$TAPEBERG_BUFFER/e2e-file1" ] || fail "expired disk copy should be removed"
rs=$($PSQL -c "SELECT state FROM retrieve_requests r JOIN objects o ON o.id=r.object_id WHERE o.key='e2e-file1'")
[ "$rs" = expired ] || fail "request state expected 'expired', got '$rs'"
echo "pin expired: file unpinned, object back to GLACIER"

say "manual-changer mode: scripted human plays the robot arm"
export TAPEBERG_CHANGER=manual
export TAPEBERG_ACK_DIR="$WORK/ack"
mkdir -p "$TAPEBERG_ACK_DIR"
"$BIN" restore e2e-file2 2

"$BIN" worker &
WPID=$!
for _ in $(seq 1 60); do
  [ -e "$TAPEBERG_ACK_DIR/prompt" ] && break
  sleep 1
done
[ -e "$TAPEBERG_ACK_DIR/prompt" ] || { kill $WPID 2>/dev/null || true; fail "no manual prompt appeared"; }
ACTION=$(awk -F': ' '/^ACTION/{print $2}' "$TAPEBERG_ACK_DIR/prompt")
TAPE=$(awk -F': ' '/^TAPE/{print $2}' "$TAPEBERG_ACK_DIR/prompt")
echo "prompt received: $ACTION $TAPE — human (script) acting"
if [ "$ACTION" = "load" ]; then
  slot=$(mtx -f "$CHANGER_DEV" status | grep "VolumeTag=$TAPE" | sed 's/.*Storage Element \([0-9]*\):Full.*/\1/')
  mtx -f "$CHANGER_DEV" load "$slot"
  "$BIN" ack "$TAPE"
fi
# Manual mode no longer prompts to unload — tapes stay in the drive
# between jobs. The worker should finish the restore and exit cleanly;
# a stale unload prompt would mean a regression.
wait $WPID || fail "manual-mode worker failed"
[ ! -e "$TAPEBERG_ACK_DIR/prompt" ] || fail "manual mode unexpectedly prompted to unload (tapes should stay put)"
"$BIN" get e2e-file2 > "$WORK/out2"
cmp "$WORK/src2" "$WORK/out2" || fail "e2e-file2 differs from source bytes"
echo "manual-changer restore verified (prompt -> ack -> restore byte-identical)"
unset TAPEBERG_CHANGER TAPEBERG_ACK_DIR

say "tapeberg status"
TAPEBERG_CHANGER=robot "$BIN" status

say "rm: soft delete from the CLI"
head -c 1048576 /dev/urandom > "$WORK/rm-test"
"$BIN" ingest "$WORK/rm-test" e2e-rm-test >/dev/null || fail "ingest e2e-rm-test failed"
"$BIN" rm e2e-rm-test >/dev/null || fail "rm failed"
if "$BIN" get e2e-rm-test >/dev/null 2>"$WORK/rmerr" 2>&1; then
  fail "get on a deleted key must fail"
fi
[ "$($PSQL -c "SELECT count(*) FROM archive_requests WHERE state='pending' AND object_id IN (SELECT id FROM objects WHERE key='e2e-rm-test')")" = 0 ] || fail "rm must cancel pending archive requests"

say "pool + bucket admin CLI"
# refuses() runs a command that must FAIL; its stderr is shown for the log.
refuses() { if "$@" 2>"$WORK/refusal"; then fail "should have been refused: $*"; fi; echo "  refused: $(cat "$WORK/refusal")"; }
poolrow() { "$BIN" pool ls | awk -F'\t' -v p="$1" '$1==p'; }
bucketrow() { "$BIN" bucket ls | awk -F'\t' -v b="$1" '$1==b'; }

"$BIN" pool add e2e-pool --description "e2e scratch pool" >/dev/null || fail "pool add failed"
poolrow e2e-pool | grep '0 tape(s).*0 bucket(s).*single copy' >/dev/null || fail "pool ls row for e2e-pool: '$(poolrow e2e-pool)'"
refuses "$BIN" pool add e2e-pool
refuses "$BIN" pool add Bad_Name
refuses "$BIN" pool set e2e-nonexistent --no-ec      # used to report success
refuses "$BIN" bucket create e2e-bkt --pool e2e-nonexistent
refuses "$BIN" bucket create e2e-bkt                 # --pool is mandatory
refuses "$BIN" bucket create E2E_BAD --pool e2e-pool
"$BIN" bucket create e2e-bkt --pool e2e-pool >/dev/null || fail "bucket create failed"
[ "$($PSQL -c "SELECT pool FROM buckets WHERE name='e2e-bkt'")" = e2e-pool ] || fail "e2e-bkt not bound to e2e-pool"
refuses "$BIN" bucket create e2e-bkt --pool e2e-pool
poolrow e2e-pool | grep '1 bucket(s)' >/dev/null || fail "pool ls should count e2e-bkt: '$(poolrow e2e-pool)'"
refuses "$BIN" pool rm e2e-pool                      # a bucket is bound to it
refuses "$BIN" pool rm vtl                           # it has tapes
refuses "$BIN" pool rm default
refuses "$BIN" bucket set-pool e2e-bkt e2e-nonexistent
"$BIN" bucket set-pool e2e-bkt vtl >/dev/null || fail "bucket set-pool failed"
bucketrow e2e-bkt | grep 'pool vtl' >/dev/null || fail "bucket ls after set-pool: '$(bucketrow e2e-bkt)'"
"$BIN" pool rm e2e-pool >/dev/null || fail "pool rm of the now-unused pool failed"
[ "$($PSQL -c "SELECT count(*) FROM tape_pools WHERE name='e2e-pool'")" = 0 ] || fail "e2e-pool still exists"
# A bucket bound to a pool name that doesn't exist (free-text column) is flagged.
$PSQL -c "UPDATE buckets SET pool='e2e-ghost' WHERE name='e2e-bkt'" >/dev/null
bucketrow e2e-bkt | grep 'NO SUCH POOL' >/dev/null || fail "dangling pool binding not flagged: '$(bucketrow e2e-bkt)'"
$PSQL -c "UPDATE buckets SET pool='vtl' WHERE name='e2e-bkt'" >/dev/null
# bucket rm refuses live objects AND tombstones (rows are kept forever).
$PSQL -c "INSERT INTO objects (bucket_id, key, size_bytes) SELECT id, 'e2e-bkt-obj', 1 FROM buckets WHERE name='e2e-bkt'" >/dev/null
refuses "$BIN" bucket rm e2e-bkt
$PSQL -c "UPDATE objects SET deleted_at = now() WHERE key='e2e-bkt-obj'" >/dev/null
refuses "$BIN" bucket rm e2e-bkt
$PSQL -c "DELETE FROM objects WHERE key='e2e-bkt-obj'" >/dev/null
refuses "$BIN" bucket rm tapeberg                    # the CLI ingest bucket
"$BIN" bucket rm e2e-bkt >/dev/null || fail "bucket rm of an empty bucket failed"
[ "$($PSQL -c "SELECT count(*) FROM buckets WHERE name='e2e-bkt'")" = 0 ] || fail "e2e-bkt still exists"
"$BIN" pool ls
"$BIN" bucket ls
echo "pool/bucket admin: create, bind, rebind, refuse-when-in-use, remove"

say "tape admin CLI"
taperow() { "$BIN" tape ls | awk -F'\t' -v t="$1" '$1==t'; }
tapeid() { $PSQL -c "SELECT id FROM tapes WHERE barcode='$1'"; }
# A vtl tape holding live data by now, and an empty active one.
LIVE=$($PSQL -c "SELECT t.barcode FROM tapes t JOIN tape_files tf ON tf.tape_id=t.id JOIN objects o ON o.id=tf.object_id AND o.deleted_at IS NULL ORDER BY t.barcode LIMIT 1")
EMPTY=$($PSQL -c "SELECT barcode FROM tapes t WHERE state='active' AND NOT is_cleaning AND barcode <> '$LIVE' AND NOT EXISTS (SELECT 1 FROM tape_files tf WHERE tf.tape_id=t.id) ORDER BY barcode DESC LIMIT 1")
[ -n "$LIVE" ] && [ -n "$EMPTY" ] || fail "fixture: need a live and an empty vtl tape (live='$LIVE' empty='$EMPTY')"
echo "  live tape $LIVE, empty tape $EMPTY"

[ "$("$BIN" tape ls --pool vtl | wc -l)" = 17 ] || fail "tape ls --pool vtl should list the 17 seeded tapes"
taperow "$LIVE" | grep 'pool vtl' | grep -v '\b0 live file' >/dev/null || fail "tape ls row for $LIVE: '$(taperow "$LIVE")'"

refuses "$BIN" tape add E2E901L5 100GB --pool e2e-tp            # no such pool
"$BIN" pool add e2e-tp >/dev/null
out=$("$BIN" tape add E2E901L5 100GB --pool e2e-tp) || fail "tape add --pool failed"
echo "$out" | grep 'pool e2e-tp' >/dev/null || fail "tape add must report the pool it used: $out"
[ "$($PSQL -c "SELECT p.name FROM tapes t JOIN tape_pools p ON p.id=t.pool_id WHERE barcode='E2E901L5'")" = e2e-tp ] || fail "E2E901L5 not in e2e-tp"
refuses "$BIN" tape add E2E901L5 --pool e2e-tp

# set-state: active <-> full; retire only when empty; retired comes back via reuse.
"$BIN" tape set-state E2E901L5 full >/dev/null && "$BIN" tape set-state E2E901L5 active >/dev/null || fail "active<->full"
refuses "$BIN" tape set-state E2E901L5 active                     # already
refuses "$BIN" tape set-state E2E901L5 lost                       # not offered
refuses "$BIN" tape set-state "$LIVE" retired                     # live data
"$BIN" tape set-state E2E901L5 retired >/dev/null || fail "retire an empty tape"
refuses "$BIN" tape set-state E2E901L5 active                     # retired: use reuse
refuses "$BIN" tape add E2E901L5 --pool e2e-tp                    # message points at reuse
grep 'tape reuse' "$WORK/refusal" >/dev/null || fail "re-adding a retired tape should point at 'tape reuse'"

# reuse: retired + fully dead only; drops the dead placement rows.
refuses "$BIN" tape reuse "$EMPTY"                                # not retired
$PSQL -c "INSERT INTO objects (bucket_id, key, size_bytes, deleted_at) SELECT id, 'e2e-tombstone', 7, now() FROM buckets WHERE name='tapeberg'" >/dev/null
$PSQL -c "INSERT INTO tape_files (tape_id, object_id, archive_id, physical_file, size_bytes) SELECT $(tapeid E2E901L5), id, 1, 1, 7 FROM objects WHERE key='e2e-tombstone'" >/dev/null
$PSQL -c "UPDATE tapes SET written_bytes=7 WHERE barcode='E2E901L5'" >/dev/null
out=$("$BIN" tape reuse E2E901L5) || fail "tape reuse failed"
echo "$out" | grep '1 stale placement' >/dev/null || fail "reuse should drop the tombstone's placement: $out"
[ "$($PSQL -c "SELECT state||':'||written_bytes FROM tapes WHERE barcode='E2E901L5'")" = active:0 ] || fail "reused tape should be active and empty"
[ "$($PSQL -c "SELECT count(*) FROM tape_files WHERE tape_id=$(tapeid E2E901L5)")" = 0 ] || fail "reuse left placement rows"
$PSQL -c "UPDATE tapes SET state='retired' WHERE barcode='$LIVE'" >/dev/null   # fabricate: retired yet live
refuses "$BIN" tape reuse "$LIVE"
$PSQL -c "UPDATE tapes SET state='active' WHERE barcode='$LIVE'" >/dev/null

# mv: never splits an object across pools; moves sets atomically; refuses in-flight tapes.
OBJ=$($PSQL -c "SELECT tf.object_id FROM tape_files tf JOIN objects o ON o.id=tf.object_id AND o.deleted_at IS NULL WHERE tf.tape_id=$(tapeid "$LIVE") LIMIT 1")
$PSQL -c "INSERT INTO tape_files (tape_id, object_id, part_number, archive_id, physical_file, size_bytes) VALUES ($(tapeid "$EMPTY"), $OBJ, 99, 1, 1, 1)" >/dev/null
refuses "$BIN" tape mv "$LIVE" --to e2e-tp                        # its object also lives on $EMPTY
grep 'split objects across pools' "$WORK/refusal" >/dev/null || fail "mv should explain the split"
[ "$($PSQL -c "SELECT p.name FROM tapes t JOIN tape_pools p ON p.id=t.pool_id WHERE barcode='$LIVE'")" = vtl ] || fail "refused mv must not move anything"
"$BIN" tape mv "$LIVE" "$EMPTY" --to e2e-tp >/dev/null || fail "moving the whole set should work"
[ "$($PSQL -c "SELECT count(*) FROM tapes t JOIN tape_pools p ON p.id=t.pool_id WHERE p.name='e2e-tp' AND barcode IN ('$LIVE','$EMPTY')")" = 2 ] || fail "set not moved"
"$BIN" tape mv "$LIVE" "$EMPTY" --to vtl >/dev/null || fail "move the set back"
$PSQL -c "DELETE FROM tape_files WHERE object_id=$OBJ AND part_number=99" >/dev/null
$PSQL -c "INSERT INTO archive_requests (object_id, state, tape_id, pool) VALUES ($OBJ, 'assigned', $(tapeid "$EMPTY"), 'vtl')" >/dev/null
refuses "$BIN" tape mv "$EMPTY" --to e2e-tp                       # an archive is being written to it
refuses "$BIN" tape set-state "$EMPTY" full
$PSQL -c "DELETE FROM archive_requests WHERE tape_id=$(tapeid "$EMPTY") AND state='assigned'" >/dev/null
refuses "$BIN" tape mv "$EMPTY" --to vtl                          # already there
refuses "$BIN" tape mv NOSUCH01 --to vtl

# Leave the catalog as the test found it.
$PSQL -c "DELETE FROM tapes WHERE barcode='E2E901L5'" >/dev/null
"$BIN" pool rm e2e-tp >/dev/null || fail "e2e-tp should be removable once empty"
"$BIN" tape ls --pool vtl
echo "tape admin: ls, add --pool, set-state, reuse, mv (atomic sets, no pool splits, no in-flight moves)"

say "--bucket: ingest/get/restore/rm in another bucket"
"$BIN" bucket create e2e-alt --pool vtl >/dev/null || fail "bucket create e2e-alt"
refuses "$BIN" ingest "$WORK/src1" e2e-x --bucket e2e-nope
printf 'alt-bytes' > "$WORK/alt"
out=$("$BIN" ingest "$WORK/alt" e2e-file1 --bucket e2e-alt) || fail "ingest --bucket e2e-alt failed"
echo "$out" | grep 'into bucket e2e-alt.*queued for pool vtl' >/dev/null || fail "ingest --bucket output: $out"
# Same key in two buckets: independent objects, independent staged bytes.
[ "$("$BIN" get e2e-file1 --bucket e2e-alt)" = alt-bytes ] || fail "get --bucket e2e-alt e2e-file1 returned the wrong bytes"
[ "$($PSQL -c "SELECT count(*) FROM objects WHERE key='e2e-file1' AND deleted_at IS NULL")" = 2 ] || fail "e2e-file1 should exist in both buckets"
# An explicit --bucket follows the bucket's pool, whatever TAPEBERG_POOL says.
TAPEBERG_POOL=e2e-misroute "$BIN" ingest "$WORK/alt" e2e-alt2 --bucket e2e-alt >/dev/null || fail "ingest --bucket with a different TAPEBERG_POOL"
[ "$($PSQL -c "SELECT r.pool FROM archive_requests r JOIN objects o ON o.id=r.object_id WHERE o.key='e2e-alt2' AND r.state='pending'")" = vtl ] \
  || fail "ingest --bucket e2e-alt should queue for the bucket's pool (vtl)"
# restore/get hints carry the bucket.
$PSQL -c "UPDATE objects SET storage_class='GLACIER', disk_path=NULL WHERE key='e2e-alt2'" >/dev/null   # fabricate: archived
refuses "$BIN" get e2e-alt2 --bucket e2e-alt
grep -- "restore e2e-alt2 --bucket e2e-alt" "$WORK/refusal" >/dev/null || fail "get hint should include --bucket: $(cat "$WORK/refusal")"
refuses "$BIN" get e2e-alt2                                        # not in the default bucket
# rm hits only the named bucket.
"$BIN" rm e2e-file1 --bucket e2e-alt >/dev/null || fail "rm --bucket"
[ "$($PSQL -c "SELECT b.name FROM objects o JOIN buckets b ON b.id=o.bucket_id WHERE o.key='e2e-file1' AND o.deleted_at IS NULL")" = tapeberg ] \
  || fail "rm --bucket e2e-alt must leave tapeberg/e2e-file1 alone"
"$BIN" rm e2e-alt2 --bucket e2e-alt >/dev/null || fail "rm e2e-alt2"
echo "--bucket: per-bucket keys, bucket's pool decides, hints and rm scoped to the bucket"

printf '\nE2E PASS — all basic functions verified\n'
