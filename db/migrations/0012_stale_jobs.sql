-- 0012_stale_jobs.sql
--
-- Crash recovery for in-flight jobs (issue #33): the worker marked
-- archive requests 'assigned' and retrieves 'restoring' with no way
-- back — a worker killed mid-job left the row, and everything queued
-- behind it, in limbo until manual SQL. RunWorker now reaps stale
-- in-flight rows at startup (it holds the exclusive buffer lock); the
-- reaper keys off assigned_at. updated_at already exists and backfills
-- rows stranded before this migration, so recovery is immediate — no
-- data backfill needed. The column is nullable: writers that never set
-- it (pre-0012 binaries) simply fall back to updated_at.
ALTER TABLE archive_requests ADD COLUMN assigned_at timestamptz;
ALTER TABLE retrieve_requests ADD COLUMN assigned_at timestamptz;
