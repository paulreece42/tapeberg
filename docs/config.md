# Configuration reference

Every setting is a `TAPEBERG_*` environment variable. Any of them can
instead live in a conf file as `key = value`, where the key is the
variable name minus the `TAPEBERG_` prefix (case-insensitive; `-` and
`_` are interchangeable):

```ini
# /etc/tapeberg.conf   (chmod 600 — it carries credentials)
db_url  = postgres://tapeberg:***@127.0.0.1:5432/tapeberg
drive   = /dev/nst0
buffer  = /var/lib/tapeberg
changer = manual
```

**Lookup:** `/etc/tapeberg.conf`, then `~/.config/tapeberg.conf`; all
files that exist are read, in that order. Setting `TAPEBERG_CONF=<path>`
**replaces** that search list with the one file — that is how one
machine runs several workers with different hardware (see
[model.md](model.md#two-libraries-on-one-node)). A missing default file
is skipped, but the explicit one **must exist**: a typo in
`TAPEBERG_CONF` is an error (`TAPEBERG_CONF=…: no such file or
directory`), not a silent fallback to the environment and built-in
defaults.

**Comments** are whole lines starting with `#`. There are no trailing
comments: in `s3_listen = 127.0.0.1:7333  # temp`, everything after
`=` (the `# temp` included) is the value. `#` is not special inside a
value, so a password containing one needs no quoting.

**Precedence:** environment > conf file > built-in default. A conf file
never overrides a variable already set in the environment, and when
two files set the same key the first one read wins.

Everything here is **per node**: device paths, the buffer, and which
pool a process serves. Logical settings (pools, buckets, EC policy)
live in the catalog — see [model.md](model.md).

## Catalog, hardware, buffer

| Key | Default | Meaning |
|---|---|---|
| `DB_URL` | — **required** | PostgreSQL URL of the catalog ([database.md](database.md)) |
| `DRIVE` | — **required** | Non-rewinding tape device used for every single-stream read/write. Robot mode: must be the library's **DTE 0** drive. Required by *every* command (even `s3` and `user`), so set it on endpoint-only nodes too. Use the stable `/dev/tape/by-id/scsi-<serial>-nst` name, not `/dev/nstN` (see *Stable device names* below). |
| `DRIVES` | unset (= `DRIVE` only) | All of the library's drives, comma-separated, **in DTE order**. Used for parallel RAIT shard writes; the first entry should equal `DRIVE`. |
| `BUFFER` | — **required** | Disk staging directory. Also holds the worker lock (`.worker.lock`): workers sharing a buffer run one at a time. |
| `CHANGER` | `robot` | `robot` (autochanger via `mtx`) or `manual` (a human loads tapes when pinged, then runs `tapeberg ack`). |
| `CHANGER_DEV` | auto-discovered | Robot mode: the changer for `mtx -f`. Unset = the first `mediumx` device `lsscsi -g` lists — fine with one library, ambiguous with two. Set it to the changer's `/dev/tape/by-id/…-changer` name. **Not** `/dev/schN`: `mtx` fails on it (`READ ELEMENT STATUS Command Failed`). |
| `ACK_DIR` | `/run/tapeberg` | Manual mode: where the worker writes its prompt and `tapeberg ack` writes the acknowledgement. |
| `POOL` | `default` | The tape pool this process serves. A **worker** drains only this pool's queues and writes only to this pool's tapes. The **S3 endpoint** binds *new* buckets to it. The **CLI**'s `tape add` uses it unless `--pool` is given; `ingest` archives into the target bucket's pool and, without `--bucket`, refuses if the `tapeberg` bucket's pool differs from this one. |

## Stable device names

Kernel numbering is not stable: `/dev/nstN` and `/dev/sgN` are handed
out in probe order and reshuffle across reboots and driver reloads (the
reference rig's VTL drives at SCSI targets 1/2/3 have come up as
nst1/nst2/nst3 one day and nst3/nst1/nst2 the next). A config naming
`/dev/nst1` then silently points at a *different drive* — fatal for
`DRIVES`, where position N must be DTE N.

udev keeps serial-number links that never move, under `/dev/tape/by-id/`
(`ls -l /dev/tape/by-id/` lists them):

| For | Use | Example (reference rig) |
|---|---|---|
| a drive (`DRIVE`, `DRIVES`) | `scsi-<serial>-nst` — the **`-nst`** (non-rewinding) link | `/dev/tape/by-id/scsi-TB-DRIVE01-nst` |
| the changer (`CHANGER_DEV`) | `scsi-<vendor_model_serial>-changer` | `/dev/tape/by-id/scsi-SSPECTRA_PYTHON_TAPEBERG1-changer` |

Drive links need a binary with #21 (resolves the link before reading
the loaded tape's barcode); older binaries read no barcode through a
link, which blinds the manual changer's wrong-tape check.

To find which drive is which DTE, load a tape into each element in turn
and see which device comes online:

```sh
CH=/dev/tape/by-id/scsi-SSPECTRA_PYTHON_TAPEBERG1-changer
mtx -f $CH load <slot> 0     # DTE 0
for d in /dev/tape/by-id/*-nst; do mt -f $d status 2>/dev/null | grep -q ONLINE && echo "DTE 0 = $d"; done
mtx -f $CH unload <slot> 0   # repeat for 1, 2, …
```

## Policy

| Key | Default | Meaning |
|---|---|---|
| `REPACK_THRESHOLD` | `50` | Percent of a tape's written bytes that must be dead (deleted objects) before the worker repacks it. |
| `UPLOAD_TTL_DAYS` | `7` | Multipart uploads neither completed nor aborted after this many days are tombstoned. `0` disables the sweep. |
| `PART_MIN_BYTES` | `5242880` (5 MiB) | Smallest accepted multipart part (all but the last), enforced at CompleteMultipartUpload. Raise it on real tape (64–128 MiB keeps LTO streaming). |
| `PART_MAX_BYTES` | `5368709120` (5 GiB) | Largest accepted part, rejected per UploadPart. |
| `STALE_JOB_MINUTES` | `30` | In-flight jobs (archives `assigned`, restores `restoring`) whose `assigned_at` is older than this are reaped back to `pending` at worker startup, which then redoes them in the same pass. Recovers a worker killed mid-job without manual SQL; `0` reaps any in-flight row (single-worker rigs). |

## S3 endpoint

| Key | Default | Meaning |
|---|---|---|
| `S3_LISTEN` | `:7333` | Listen address for `tapeberg s3` (a positional argument overrides it). |
| `AUTH` | `builtin` | Auth backend. `builtin` = users and keys in the catalog. |
| `S3_KEY`, `S3_SECRET` | unset | Legacy static key pair, used only while the catalog has **no** users. With neither set and no users, the endpoint runs unsigned (dev mode). |
| `S3_DEBUG` | unset | Any value: log SigV4 canonical requests when signature checks fail. |

## Notifications (manual changer)

Any mix; all optional. `tapeberg doctor --notify-test` sends a test message.

| Key | Meaning |
|---|---|
| `TELEGRAM_TOKEN`, `TELEGRAM_CHAT` | Telegram bot token and chat id |
| `WEBHOOK_URL` | POSTs `{"text": ...}` (Slack incoming-webhook shape) |
| `WEBHOOK_TOKEN` | Optional bearer token for the webhook |

## Meta

| Key | Meaning |
|---|---|
| `CONF` | Path of the one conf file to read, replacing the default search list. Environment only — it can't be set from inside a conf file. |
