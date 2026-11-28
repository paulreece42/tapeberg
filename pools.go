package main

// Pool and bucket administration — the logical half of docs/model.md.
// Pools group tapes; buckets bind to a pool (new archives in a bucket go
// to its pool's tapes). Hardware never appears here: which library serves
// a pool is node config (TAPEBERG_POOL / TAPEBERG_DRIVE...), not catalog.

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Pool names are queue keys and config values; keep them boring.
var poolNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

// S3 bucket naming (the subset every client accepts): 3-63 chars of
// lowercase letters, digits, '.', '-', starting and ending alphanumeric.
var bucketNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

// defaultPool is the pool TAPEBERG_POOL and buckets.pool default to.
const defaultPool = "default"

// cliBucket is the bucket CLI ingests land in (seeded).
const cliBucket = "tapeberg"

type PoolInfo struct {
	Name, Description           string
	ECK, ECM                    int
	Tapes, Active, Full, Gone   int // Gone = retired + lost
	Buckets                     int
	WrittenBytes, CapacityBytes int64 // capacity of active+full tapes
}

func listPools(db *sql.DB) ([]PoolInfo, error) {
	rows, err := db.Query(`
		SELECT p.name, COALESCE(p.description, ''), COALESCE(p.ec_k, 0), COALESCE(p.ec_m, 0),
		       count(t.id),
		       count(t.id) FILTER (WHERE t.state = 'active'),
		       count(t.id) FILTER (WHERE t.state = 'full'),
		       count(t.id) FILTER (WHERE t.state IN ('retired', 'lost')),
		       (SELECT count(*) FROM buckets b WHERE b.pool = p.name),
		       COALESCE(sum(t.written_bytes), 0),
		       COALESCE(sum(t.capacity_bytes) FILTER (WHERE t.state IN ('active', 'full')), 0)
		FROM tape_pools p LEFT JOIN tapes t ON t.pool_id = p.id
		GROUP BY p.id ORDER BY p.name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PoolInfo
	for rows.Next() {
		var p PoolInfo
		if err := rows.Scan(&p.Name, &p.Description, &p.ECK, &p.ECM, &p.Tapes, &p.Active, &p.Full, &p.Gone,
			&p.Buckets, &p.WrittenBytes, &p.CapacityBytes); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func poolExists(db *sql.DB, name string) (bool, error) {
	var ok bool
	err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM tape_pools WHERE name = $1)`, name).Scan(&ok)
	return ok, err
}

func addPool(db *sql.DB, name, description string) error {
	if !poolNameRe.MatchString(name) {
		return fmt.Errorf("pool name %q: use lowercase letters, digits, '-' or '_' (max 63)", name)
	}
	res, err := db.Exec(`INSERT INTO tape_pools (name, description) VALUES ($1, NULLIF($2, ''))
		ON CONFLICT (name) DO NOTHING`, name, description)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("pool %q already exists", name)
	}
	return nil
}

// removePool drops an unused pool. Anything that names it — tapes,
// buckets (whose new archives would queue for a pool no worker serves),
// or unfinished queue rows — blocks the removal.
func removePool(db *sql.DB, name string) error {
	if name == defaultPool {
		return fmt.Errorf("pool %q is the built-in default (TAPEBERG_POOL and new buckets fall back to it); it can't be removed", name)
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var id int64
	err = tx.QueryRow(`SELECT id FROM tape_pools WHERE name = $1 FOR UPDATE`, name).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("pool %q: %w", name, ErrNotFound)
	}
	if err != nil {
		return err
	}
	var tapes, buckets, archives, restores int
	if err := tx.QueryRow(`
		SELECT (SELECT count(*) FROM tapes WHERE pool_id = $1),
		       (SELECT count(*) FROM buckets WHERE pool = $2),
		       (SELECT count(*) FROM archive_requests WHERE pool = $2 AND state IN ('pending', 'assigned')),
		       (SELECT count(*) FROM retrieve_requests WHERE pool = $2 AND state IN ('pending', 'waiting_for_media', 'restoring'))`,
		id, name).Scan(&tapes, &buckets, &archives, &restores); err != nil {
		return err
	}
	var blockers []string
	if tapes > 0 {
		blockers = append(blockers, fmt.Sprintf("%d tape(s) — 'tapeberg tape ls' (move or remove them first)", tapes))
	}
	if buckets > 0 {
		blockers = append(blockers, fmt.Sprintf("%d bucket(s) bound to it — 'tapeberg bucket set-pool <bucket> <pool>'", buckets))
	}
	if archives > 0 {
		blockers = append(blockers, fmt.Sprintf("%d queued archive(s)", archives))
	}
	if restores > 0 {
		blockers = append(blockers, fmt.Sprintf("%d queued restore(s)", restores))
	}
	if len(blockers) > 0 {
		return fmt.Errorf("pool %q is in use: %s", name, strings.Join(blockers, "; "))
	}
	if _, err := tx.Exec(`DELETE FROM tape_pools WHERE id = $1`, id); err != nil {
		return err
	}
	return tx.Commit()
}

type BucketInfo struct {
	Name, Pool, Owner string // Owner "" = system-owned
	PoolMissing       bool   // bound to a pool name that doesn't exist
	Objects, Deleted  int    // live / tombstoned
	Bytes             int64  // live bytes
}

func listAllBuckets(db *sql.DB) ([]BucketInfo, error) {
	rows, err := db.Query(`
		SELECT b.name, b.pool, COALESCE(u.user_id, ''),
		       NOT EXISTS (SELECT 1 FROM tape_pools p WHERE p.name = b.pool),
		       count(o.id) FILTER (WHERE o.deleted_at IS NULL),
		       count(o.id) FILTER (WHERE o.deleted_at IS NOT NULL),
		       COALESCE(sum(o.size_bytes) FILTER (WHERE o.deleted_at IS NULL), 0)
		FROM buckets b
		LEFT JOIN s3_users u ON u.id = b.owner_id
		LEFT JOIN objects o ON o.bucket_id = b.id
		GROUP BY b.id, u.user_id ORDER BY b.name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BucketInfo
	for rows.Next() {
		var b BucketInfo
		if err := rows.Scan(&b.Name, &b.Pool, &b.Owner, &b.PoolMissing, &b.Objects, &b.Deleted, &b.Bytes); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// createBucketCLI makes a bucket bound to an explicit, existing pool.
// (S3 CreateBucket binds to the endpoint's TAPEBERG_POOL instead.)
func createBucketCLI(db *sql.DB, name, pool, ownerUID string) error {
	if !bucketNameRe.MatchString(name) || strings.Contains(name, "..") {
		return fmt.Errorf("bucket name %q: 3-63 chars of lowercase letters, digits, '.', '-', starting and ending alphanumeric", name)
	}
	if ok, err := poolExists(db, pool); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("pool %q does not exist ('tapeberg pool ls'; create it with 'tapeberg pool add')", pool)
	}
	if ownerUID != "" {
		if ok, err := dbUserExists(db, ownerUID); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("user %q does not exist ('tapeberg user ls')", ownerUID)
		}
	}
	res, err := db.Exec(`INSERT INTO buckets (name, pool, owner_id)
		VALUES ($1, $2, (SELECT id FROM s3_users WHERE user_id = NULLIF($3, '')))
		ON CONFLICT (name) DO NOTHING`, name, pool, ownerUID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("bucket %q already exists", name)
	}
	return nil
}

// setBucketPool rebinds a bucket. Only new archives follow the binding:
// objects already on tape stay put (restores follow their tapes), and
// archives already queued keep the pool they were queued for. It returns
// how many queued archives that is, so the caller can say so.
func setBucketPool(db *sql.DB, name, pool string) (queuedElsewhere, openUploads int, err error) {
	if ok, err := poolExists(db, pool); err != nil {
		return 0, 0, err
	} else if !ok {
		return 0, 0, fmt.Errorf("pool %q does not exist ('tapeberg pool ls'; create it with 'tapeberg pool add')", pool)
	}
	res, err := db.Exec(`UPDATE buckets SET pool = $2 WHERE name = $1`, name, pool)
	if err != nil {
		return 0, 0, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return 0, 0, fmt.Errorf("bucket %q: %w", name, ErrNotFound)
	}
	// Queued archives keep their original pool (the note in main.go).
	err = db.QueryRow(`
		SELECT count(*) FROM archive_requests a
		JOIN objects o ON o.id = a.object_id
		JOIN buckets b ON b.id = o.bucket_id
		WHERE b.name = $1 AND a.pool <> $2 AND a.state IN ('pending', 'assigned')`,
		name, pool).Scan(&queuedElsewhere)
	if err != nil {
		return 0, 0, err
	}
	// Open multipart uploads were PINNED to the old pool at initiation
	// (issue #39): their parts keep queueing there no matter what this
	// rebind says — the CLI reports them instead of silently stranding
	// the operator's mental model.
	err = db.QueryRow(`SELECT count(*) FROM objects o
		JOIN buckets b ON b.id = o.bucket_id
		WHERE b.name = $1 AND o.upload_id IS NOT NULL`, name).Scan(&openUploads)
	return queuedElsewhere, openUploads, err
}

// removeBucket drops a bucket that holds no object rows at all. Deleted
// objects are tombstones that keep their row (and, until repack reclaims
// it, their tape space), so a bucket that ever held objects can't be
// dropped yet.
func removeBucket(db *sql.DB, name string) error {
	if name == cliBucket {
		return fmt.Errorf("bucket %q is where CLI ingests land; it can't be removed", name)
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var id int64
	err = tx.QueryRow(`SELECT id FROM buckets WHERE name = $1 FOR UPDATE`, name).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("bucket %q: %w", name, ErrNotFound)
	}
	if err != nil {
		return err
	}
	var live, deleted int
	if err := tx.QueryRow(`SELECT count(*) FILTER (WHERE deleted_at IS NULL), count(*) FILTER (WHERE deleted_at IS NOT NULL)
		FROM objects WHERE bucket_id = $1`, id).Scan(&live, &deleted); err != nil {
		return err
	}
	if live > 0 {
		return fmt.Errorf("bucket %q holds %d object(s); delete them first", name, live)
	}
	if deleted > 0 {
		return fmt.Errorf("bucket %q has %d deleted object(s) whose catalog rows are kept as tombstones; buckets that ever held objects can't be removed yet", name, deleted)
	}
	if _, err := tx.Exec(`DELETE FROM buckets WHERE id = $1`, id); err != nil {
		return err
	}
	return tx.Commit()
}

func humanBytes(n int64) string {
	const unit = 1000 // tape vendors quote decimal units (LTO-5 = 1.5 TB)
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "kMGTPE"[exp])
}
