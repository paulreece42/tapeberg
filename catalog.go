// Catalog: the PostgreSQL layer. The catalog owns STATE; the tape owns BYTES.
// The worker owns all state transitions; the CLI only INSERTs and SELECTs.
package main

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// ErrNotFound is returned when a catalog row does not exist.
var ErrNotFound = errors.New("not found")

func openDB(url string) (*sql.DB, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("catalog unreachable: %w", err)
	}
	return db, nil
}

type Object struct {
	ID           int64
	Key          string
	Size         int64
	SHA256       string
	Etag         string // S3 ETag: md5 (single put) or md5-of-md5s-N (multipart); "" = present sha256
	StorageClass string
	DiskPath     string // "" when tape-only
	OwnerID      int64  // 0 = system (CLI ingests, legacy rows)
	CreatedAt    time.Time
}

// etagOf is what object-serving code presents as the S3 ETag: the
// proper md5 etag when known, else the stored sha256 (legacy rows).
func (o *Object) etagOf() string {
	if o.Etag != "" {
		return o.Etag
	}
	return o.SHA256
}

func bucketID(db *sql.DB) (int64, error) { return bucketIDByName(db, "tapeberg") }

func bucketIDByName(db *sql.DB, name string) (int64, error) {
	var id int64
	err := db.QueryRow(`SELECT id FROM buckets WHERE name = $1`, name).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("bucket %q: %w", name, ErrNotFound)
	}
	return id, err
}

func createBucketIfAbsent(db *sql.DB, name, pool string, owner int64) error {
	return insertBucket(db, name, pool, owner)
}

func insertBucket(db *sql.DB, name, pool string, owner int64) error {
	_, err := db.Exec(`INSERT INTO buckets (name, pool, owner_id) VALUES ($1, $2, NULLIF($3,0))
		ON CONFLICT (name) DO NOTHING`, name, pool, owner)
	return err
}

// bucketOwner reports the bucket's owner (0 = system-owned).
func bucketOwner(db *sql.DB, name string) (int64, error) {
	var o sql.NullInt64
	err := db.QueryRow(`SELECT owner_id FROM buckets WHERE name = $1`, name).Scan(&o)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("bucket %q: %w", name, ErrNotFound)
	}
	return o.Int64, err
}

// bucketPool reports the tape pool a bucket archives into. The binding
// is what makes pools visible in the S3 API: bucket = pool = tape set.
func bucketPool(db *sql.DB, name string) (string, error) {
	var pool string
	err := db.QueryRow(`SELECT pool FROM buckets WHERE name = $1`, name).Scan(&pool)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("bucket %q: %w", name, ErrNotFound)
	}
	return pool, err
}

type BucketRow struct {
	Name    string
	Created time.Time
}

// listBucketsFor scopes bucket visibility: admins see all buckets,
// users only their own (system-owned buckets are admin-only, like
// CLI-ingested objects).
func listBucketsFor(db *sql.DB, u AuthUser) ([]BucketRow, error) {
	q := `SELECT name, created_at FROM buckets`
	args := []interface{}{}
	if !u.IsAdmin {
		q += ` WHERE owner_id = $1`
		args = append(args, u.ID)
	}
	q += ` ORDER BY name`
	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BucketRow
	for rows.Next() {
		var b BucketRow
		if err := rows.Scan(&b.Name, &b.Created); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// listObjectsFor scopes object listing the same way.
func listObjectsFor(db *sql.DB, bucket, prefix string, u AuthUser) ([]Object, error) {
	q := `
		SELECT o.id, o.key, o.size_bytes, COALESCE(o.sha256,''), COALESCE(o.etag,''), o.storage_class,
		       COALESCE(o.disk_path,''), o.created_at
		FROM objects o JOIN buckets b ON b.id = o.bucket_id
		WHERE b.name = $1 AND ($2 = '' OR o.key LIKE $2 || '%')
		  AND o.upload_id IS NULL AND o.deleted_at IS NULL`
	args := []interface{}{bucket, prefix}
	if !u.IsAdmin {
		q += ` AND o.owner_id = $3`
		args = append(args, u.ID)
	}
	q += ` ORDER BY o.key`
	rows, err := db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Object
	for rows.Next() {
		var o Object
		if err := rows.Scan(&o.ID, &o.Key, &o.Size, &o.SHA256, &o.Etag, &o.StorageClass, &o.DiskPath, &o.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func listObjects(db *sql.DB, bucket, prefix string) ([]Object, error) {
	rows, err := db.Query(`
		SELECT o.id, o.key, o.size_bytes, COALESCE(o.sha256,''), COALESCE(o.etag,''), o.storage_class, COALESCE(o.disk_path,''), o.created_at
		FROM objects o JOIN buckets b ON b.id = o.bucket_id
		WHERE b.name = $1 AND ($2 = '' OR o.key LIKE $2 || '%') AND o.upload_id IS NULL
		  AND o.deleted_at IS NULL
		ORDER BY o.key`, bucket, prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Object
	for rows.Next() {
		var o Object
		if err := rows.Scan(&o.ID, &o.Key, &o.Size, &o.SHA256, &o.Etag, &o.StorageClass, &o.DiskPath, &o.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func getObjectIn(db *sql.DB, bucket, key string) (*Object, error) {
	bid, err := bucketIDByName(db, bucket)
	if err != nil {
		return nil, err
	}
	o := &Object{Key: key}
	var sha, path sql.NullString
	var owner sql.NullInt64
	err = db.QueryRow(`
		SELECT id, size_bytes, sha256, COALESCE(etag,''), storage_class, disk_path, created_at, owner_id
		FROM objects WHERE bucket_id = $1 AND key = $2 AND upload_id IS NULL
		  AND deleted_at IS NULL`, bid, key).
		Scan(&o.ID, &o.Size, &sha, &o.Etag, &o.StorageClass, &path, &o.CreatedAt, &owner)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("object %q: %w", key, ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	o.SHA256, o.DiskPath, o.OwnerID = sha.String, path.String, owner.Int64
	return o, nil
}

// activeRetrieve reports the newest non-terminal restore request for an
// object, so HEAD/GET can surface x-amz-restore state.
func activeRetrieve(db *sql.DB, objectID int64) (state string, expires time.Time, found bool, err error) {
	var exp sql.NullTime
	err = db.QueryRow(`
		SELECT state, expires_at FROM retrieve_requests
		WHERE object_id = $1 AND state IN ('pending','waiting_for_media','restoring','restored')
		ORDER BY id DESC LIMIT 1`, objectID).Scan(&state, &exp)
	if errors.Is(err, sql.ErrNoRows) {
		return "", time.Time{}, false, nil
	}
	if err != nil {
		return "", time.Time{}, false, err
	}
	if exp.Valid {
		expires = exp.Time
	}
	return state, expires, true, nil
}

// insertObject records an already-staged disk copy as a STANDARD object.
func insertObject(db *sql.DB, bucket, key string, size int64, sha256hex, etag, diskPath string, owner int64) (*Object, error) {
	bid, err := bucketIDByName(db, bucket)
	if err != nil {
		return nil, err
	}
	o := &Object{Key: key, Size: size, SHA256: sha256hex, Etag: etag, StorageClass: "STANDARD", DiskPath: diskPath, OwnerID: owner}
	err = db.QueryRow(`
		INSERT INTO objects (bucket_id, key, size_bytes, sha256, etag, storage_class, disk_path, owner_id)
		VALUES ($1, $2, $3, $4, NULLIF($5,''), 'STANDARD', $6, NULLIF($7, 0)) RETURNING id`,
		bid, key, size, sha256hex, etag, diskPath, owner).Scan(&o.ID)
	if isUniqueViolation(err) {
		return nil, fmt.Errorf("object %q already exists (v0 has no overwrite)", key)
	}
	if err != nil {
		return nil, err
	}
	return o, nil
}

func isUniqueViolation(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "23505"
}

// ---- archive queue ----

func createArchiveRequest(db *sql.DB, objectID int64, pool string, part int) error {
	_, err := db.Exec(`INSERT INTO archive_requests (object_id, pool, part_number) VALUES ($1, $2, $3)`, objectID, pool, part)
	return err
}

type ArchiveReq struct {
	ID         int64
	ObjectID   int64
	Key        string
	Size       int64
	DiskPath   string
	PartNumber int
}

func pendingArchives(db *sql.DB, pool string) ([]ArchiveReq, error) {
	rows, err := db.Query(`
		SELECT r.id, r.object_id, o.key,
		       COALESCE(op.size_bytes, o.size_bytes),
		       COALESCE(op.disk_path, o.disk_path),
		       r.part_number
		FROM archive_requests r
		JOIN objects o ON o.id = r.object_id
		LEFT JOIN object_parts op
		  ON op.object_id = r.object_id AND op.part_number = r.part_number
		WHERE r.state = 'pending' AND r.pool = $1 AND o.deleted_at IS NULL
		ORDER BY r.requested_at`, pool)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ArchiveReq
	for rows.Next() {
		var r ArchiveReq
		var path sql.NullString
		if err := rows.Scan(&r.ID, &r.ObjectID, &r.Key, &r.Size, &path, &r.PartNumber); err != nil {
			return nil, err
		}
		r.DiskPath = path.String
		out = append(out, r)
	}
	return out, rows.Err()
}

// pickTapeExcluding is pickTape minus one tape (the repack source).
func pickTapeExcluding(db *sql.DB, pool string, size int64, exclude int64) (*Tape, error) {
	q := `SELECT t.id, t.barcode, t.capacity_bytes, t.written_bytes
		FROM tapes t JOIN tape_pools p ON p.id = t.pool_id
		WHERE p.name = $2 AND t.state = 'active' AND NOT t.is_cleaning
		  AND t.id != $3 AND t.written_bytes + $1 <= t.capacity_bytes
		ORDER BY t.written_bytes DESC, t.id ASC LIMIT 1`
	t := &Tape{}
	err := db.QueryRow(q, size, pool, exclude).
		Scan(&t.ID, &t.Barcode, &t.CapacityBytes, &t.WrittenBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("no active tape in pool %q fits %d bytes: %w", pool, size, ErrNotFound)
	}
	return t, err
}

type Tape struct {
	ID            int64
	Barcode       string
	CapacityBytes int64
	WrittenBytes  int64
}

// pickTape chooses, from the configured pool, the most-written active
// tape that nominally still fits the object (fills tapes in sequence,
// CTA-style); ties break to the lowest tape id so the fill order is
// deterministic; falls back to the least-written one and lets the
// drive's EOM be the judge. Pool isolation keeps one rig's tapes (e.g.
// an mhvtl dev library) invisible to another's worker.
func pickTape(db *sql.DB, pool string, size int64) (*Tape, error) {
	q := `SELECT t.id, t.barcode, t.capacity_bytes, t.written_bytes
		FROM tapes t JOIN tape_pools p ON p.id = t.pool_id
		WHERE p.name = $2 AND t.state = 'active' AND NOT t.is_cleaning
		  AND t.written_bytes + $1 <= t.capacity_bytes
		ORDER BY t.written_bytes %s, t.id ASC LIMIT 1`
	t := &Tape{}
	err := db.QueryRow(fmt.Sprintf(q, "DESC"), size, pool).
		Scan(&t.ID, &t.Barcode, &t.CapacityBytes, &t.WrittenBytes)
	if errors.Is(err, sql.ErrNoRows) {
		err = db.QueryRow(fmt.Sprintf(q, "ASC"), size, pool).
			Scan(&t.ID, &t.Barcode, &t.CapacityBytes, &t.WrittenBytes)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("no active tape left: %w", ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	return t, nil
}

func setArchiveAssigned(db *sql.DB, reqID, tapeID int64) error {
	_, err := db.Exec(`UPDATE archive_requests SET state='assigned', tape_id=$2, assigned_at=now(), updated_at=now() WHERE id=$1`, reqID, tapeID)
	return err
}

func setArchivePending(db *sql.DB, reqID int64) error {
	_, err := db.Exec(`UPDATE archive_requests SET state='pending', tape_id=NULL, assigned_at=NULL, updated_at=now() WHERE id=$1`, reqID)
	return err
}

func markArchiveWritten(db *sql.DB, reqID int64) error {
	_, err := db.Exec(`UPDATE archive_requests SET state='written', written_at=now(), updated_at=now() WHERE id=$1`, reqID)
	return err
}

func failArchive(db *sql.DB, reqID int64, msg string) error {
	_, err := db.Exec(`UPDATE archive_requests SET state='failed', error=$2, updated_at=now() WHERE id=$1`, reqID, msg)
	return err
}

func markTapeFull(db *sql.DB, tapeID int64) error {
	_, err := db.Exec(`UPDATE tapes SET state='full', updated_at=now() WHERE id=$1`, tapeID)
	return err
}

func markTapeRetired(db *sql.DB, tapeID int64) error {
	_, err := db.Exec(`UPDATE tapes SET state='retired', updated_at=now() WHERE id=$1`, tapeID)
	return err
}

// softDeleteObject tombstones the object (delete marker), cancels its
// queued archive requests, and clears catalog disk references; the
// caller removes the returned files. Tape bytes stay until the repack
// pass reclaims them.
func softDeleteObject(db *sql.DB, objectID int64) (removedFiles []string, err error) {
	var path sql.NullString
	err = db.QueryRow(`UPDATE objects SET deleted_at = now(), disk_path = NULL
		WHERE id = $1 AND deleted_at IS NULL RETURNING disk_path`, objectID).Scan(&path)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil // already deleted — idempotent
	}
	if err != nil {
		return nil, err
	}
	if path.String != "" {
		removedFiles = append(removedFiles, path.String)
	}
	if _, err := db.Exec(`UPDATE archive_requests SET state='cancelled', updated_at=now()
		WHERE object_id = $1 AND state IN ('pending','assigned')`, objectID); err != nil {
		return nil, err
	}
	// Part disk copies too (multipart buffered or pinned). SELECT the
	// paths FIRST: RETURNING would report the post-UPDATE value (NULL,
	// since we just nulled it) and the scan would fail — which leaked
	// buffered part files on every delete/abort of an object whose
	// parts were not yet on tape.
	rows, err := db.Query(`SELECT disk_path FROM object_parts
		WHERE object_id = $1 AND disk_path IS NOT NULL`, objectID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			rows.Close()
			return nil, err
		}
		removedFiles = append(removedFiles, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if _, err := db.Exec(`UPDATE object_parts SET disk_path = NULL
		WHERE object_id = $1 AND disk_path IS NOT NULL`, objectID); err != nil {
		return nil, err
	}
	return removedFiles, nil
}

// RepackTape is a tape whose dead (deleted-object) share makes it
// worth rewriting: live files move to other tapes, the source retires.
type RepackTape struct {
	ID      int64
	Barcode string
	Written int64
	Live    int64
	Dead    int64 // Written - Live
}

// repackCandidates lists tapes in the pool whose dead bytes meet the
// threshold (dead >= pct% of written). CTA-style repack.
func repackCandidates(db *sql.DB, pool string, pct int) ([]RepackTape, error) {
	rows, err := db.Query(`
		SELECT t.id, t.barcode, t.written_bytes,
		       COALESCE(sum(tf.size_bytes) FILTER (
		         WHERE o.deleted_at IS NULL), 0) AS live
		FROM tapes t
		JOIN tape_pools p ON p.id = t.pool_id
		LEFT JOIN tape_files tf ON tf.tape_id = t.id
		LEFT JOIN objects o ON o.id = tf.object_id
		WHERE p.name = $1 AND t.state IN ('active','full') AND t.written_bytes > 0
		GROUP BY t.id, t.barcode, t.written_bytes
		HAVING t.written_bytes - COALESCE(sum(tf.size_bytes) FILTER (
		          WHERE o.deleted_at IS NULL), 0)
		       >= ($2::numeric / 100.0) * t.written_bytes
		ORDER BY t.id`, pool, pct)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RepackTape
	for rows.Next() {
		var t RepackTape
		if err := rows.Scan(&t.ID, &t.Barcode, &t.Written, &t.Live); err != nil {
			return nil, err
		}
		t.Dead = t.Written - t.Live
		out = append(out, t)
	}
	return out, rows.Err()
}

// LiveTapeFile is one file to re-archive during repack.
type LiveTapeFile struct {
	TapeFileID   int64
	ObjectID     int64
	PartNumber   int64
	PhysicalFile int64
	Size         int64
	Sha256       string // object's (or part's) verifier
	ShardNumber  int64  // >= 0 for RAIT shards, -1 for whole files
}

// liveTapeFiles lists the source tape's live files in write order.
// The verifier is the SHARD's sha for RAIT shards (a shard's bytes do
// not hash to the object's sha), the part's for multipart parts, and
// the object's otherwise.
func liveTapeFiles(db *sql.DB, tapeID int64) ([]LiveTapeFile, error) {
	rows, err := db.Query(`
		SELECT tf.id, tf.object_id, tf.part_number, tf.physical_file, tf.size_bytes,
		       COALESCE(os.sha256, op.sha256, o.sha256), COALESCE(tf.shard_number, -1)
		FROM tape_files tf
		JOIN objects o ON o.id = tf.object_id
		LEFT JOIN object_parts op
		  ON op.object_id = tf.object_id AND op.part_number = tf.part_number
		LEFT JOIN object_shards os
		  ON os.object_id = tf.object_id AND os.part_number = tf.part_number
		 AND os.shard_number = tf.shard_number
		WHERE tf.tape_id = $1 AND o.deleted_at IS NULL
		ORDER BY tf.physical_file`, tapeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LiveTapeFile
	for rows.Next() {
		var f LiveTapeFile
		if err := rows.Scan(&f.TapeFileID, &f.ObjectID, &f.PartNumber, &f.PhysicalFile, &f.Size, &f.Sha256, &f.ShardNumber); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// moveTapeFile records that a live file now lives at a new position:
// delete the old placement row, insert the new one. Shards keep their
// shard number. The old bytes become dead space on the source tape
// (which is being retired).
func moveTapeFile(db *sql.DB, oldTapeFileID, tapeID, objectID, partNumber, shardNumber, archiveID, physicalFile, size int64) error {
	if _, err := db.Exec(`DELETE FROM tape_files WHERE id = $1`, oldTapeFileID); err != nil {
		return err
	}
	if shardNumber >= 0 {
		return insertShardTapeFile(db, tapeID, objectID, partNumber, shardNumber, archiveID, physicalFile, size)
	}
	return insertTapeFile(db, tapeID, objectID, partNumber, archiveID, physicalFile, size)
}

// ECConfig is a pool's (or frozen object's) erasure-coding policy:
// k data + m parity shards, one shard per tape.
type ECConfig struct{ K, M int }

// poolEC reports the pool's EC policy (nil = no erasure coding).
func poolEC(db *sql.DB, pool string) (*ECConfig, error) {
	var k, m sql.NullInt64
	err := db.QueryRow(`SELECT ec_k, ec_m FROM tape_pools WHERE name = $1`, pool).Scan(&k, &m)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("pool %q: %w", pool, ErrNotFound)
	}
	if err != nil || !k.Valid || !m.Valid {
		return nil, err
	}
	return &ECConfig{K: int(k.Int64), M: int(m.Int64)}, nil
}

// setPoolEC sets (or clears, k=0) a pool's EC policy.
func setPoolEC(db *sql.DB, pool string, k, m int) error {
	var res sql.Result
	var err error
	if k == 0 {
		res, err = db.Exec(`UPDATE tape_pools SET ec_k = NULL, ec_m = NULL WHERE name = $1`, pool)
	} else {
		res, err = db.Exec(`UPDATE tape_pools SET ec_k = $2, ec_m = $3 WHERE name = $1`, pool, k, m)
	}
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("pool %q: %w", pool, ErrNotFound)
	}
	return nil
}

// objectEC is the EC policy frozen on the object at archive time.
func objectEC(db *sql.DB, objectID int64) (*ECConfig, error) {
	var k, m sql.NullInt64
	err := db.QueryRow(`SELECT ec_k, ec_m FROM objects WHERE id = $1`, objectID).Scan(&k, &m)
	if err != nil || !k.Valid || !m.Valid {
		return nil, err
	}
	return &ECConfig{K: int(k.Int64), M: int(m.Int64)}, nil
}

func setObjectEC(db *sql.DB, objectID int64, k, m int) error {
	_, err := db.Exec(`UPDATE objects SET ec_k = $2, ec_m = $3 WHERE id = $1`, objectID, k, m)
	return err
}

// insertShardTapeFile records a shard placement (like insertTapeFile
// plus the shard number).
func insertShardTapeFile(db *sql.DB, tapeID, objectID, partNumber, shardNumber, archiveID, physicalFile, size int64) error {
	_, err := db.Exec(`INSERT INTO tape_files
		(tape_id, object_id, part_number, shard_number, archive_id, physical_file, size_bytes)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		tapeID, objectID, partNumber, shardNumber, archiveID, physicalFile, size)
	return err
}

func insertShardVerifier(db *sql.DB, objectID, part, shard int64, size int64, sha string) error {
	_, err := db.Exec(`INSERT INTO object_shards
		(object_id, part_number, shard_number, size_bytes, sha256) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (object_id, part_number, shard_number) DO NOTHING`,
		objectID, part, shard, size, sha)
	return err
}

// ShardPlacement is where one shard lives (and how to check it).
type ShardPlacement struct {
	TapeID       int64
	Barcode      string
	PhysicalFile int64
	Size         int64
	ShardNumber  int64
	SHA256       string
}

// shardPlacements lists a part's shard placements, shard order.
func shardPlacements(db *sql.DB, objectID, partNumber int64) ([]ShardPlacement, error) {
	rows, err := db.Query(`
		SELECT tf.tape_id, t.barcode, tf.physical_file, tf.size_bytes,
		       tf.shard_number, os.sha256
		FROM tape_files tf
		JOIN tapes t ON t.id = tf.tape_id
		JOIN object_shards os
		  ON os.object_id = tf.object_id AND os.part_number = tf.part_number
		 AND os.shard_number = tf.shard_number
		WHERE tf.object_id = $1 AND tf.part_number = $2 AND tf.shard_number IS NOT NULL
		ORDER BY tf.shard_number`, objectID, partNumber)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ShardPlacement
	for rows.Next() {
		var p ShardPlacement
		if err := rows.Scan(&p.TapeID, &p.Barcode, &p.PhysicalFile, &p.Size, &p.ShardNumber, &p.SHA256); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// pickShardTarget chooses a tape for one shard: it must not be a tape
// already picked for this stripe NOR one already holding a sibling
// shard of the same object-part (two shards of one file on one tape
// would halve RAIT's survivability).
func pickShardTarget(db *sql.DB, pool string, size int64, exclude []int64, objectID, partNumber int64) (*Tape, error) {
	q := `SELECT t.id, t.barcode, t.capacity_bytes, t.written_bytes
		FROM tapes t JOIN tape_pools p ON p.id = t.pool_id
		WHERE p.name = $2 AND t.state = 'active' AND NOT t.is_cleaning
		  AND t.written_bytes + $1 <= t.capacity_bytes
		  AND t.id <> ALL($3::bigint[])
		  AND t.id NOT IN (
		    SELECT tape_id FROM tape_files
		    WHERE object_id = $4 AND part_number = $5 AND shard_number IS NOT NULL)
		ORDER BY t.written_bytes DESC, t.id ASC LIMIT 1`
	t := &Tape{}
	err := db.QueryRow(q, size, pool, pq.Array(exclude), objectID, partNumber).
		Scan(&t.ID, &t.Barcode, &t.CapacityBytes, &t.WrittenBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("no distinct active tape in pool %q fits a %d-byte shard: %w", pool, size, ErrNotFound)
	}
	return t, err
}

func addTapeWritten(db *sql.DB, tapeID, n int64) error {
	_, err := db.Exec(`UPDATE tapes SET written_bytes = written_bytes + $2, updated_at=now() WHERE id=$1`, tapeID, n)
	return err
}

func nextArchiveID(db *sql.DB, tapeID int64) (int64, error) {
	var id int64
	err := db.QueryRow(`SELECT COALESCE(MAX(archive_id), 0) + 1 FROM tape_files WHERE tape_id = $1`, tapeID).Scan(&id)
	return id, err
}

func insertTapeFile(db *sql.DB, tapeID, objectID, partNumber, archiveID, physicalFile, size int64) error {
	_, err := db.Exec(`INSERT INTO tape_files (tape_id, object_id, part_number, archive_id, physical_file, size_bytes)
		VALUES ($1, $2, $3, $4, $5, $6)`, tapeID, objectID, partNumber, archiveID, physicalFile, size)
	return err
}

func objectToStandardMultipart(db *sql.DB, objectID int64) error {
	_, err := db.Exec(`UPDATE objects SET storage_class = 'STANDARD', disk_path = NULL WHERE id = $1`, objectID)
	return err
}

func objectToGlacier(db *sql.DB, objectID int64) error {
	_, err := db.Exec(`UPDATE objects SET storage_class='GLACIER', disk_path=NULL WHERE id=$1`, objectID)
	return err
}

func objectToStandard(db *sql.DB, objectID int64, path string) error {
	_, err := db.Exec(`UPDATE objects SET storage_class='STANDARD', disk_path=$2 WHERE id=$1`, objectID, path)
	return err
}

// ---- restore queue ----

// createRetrieveRequest queues a restore and returns the pool it was
// queued in. The pool comes from where the object's bytes actually are
// (placementPool) — only the worker serving that pool has a library
// holding those tapes. The caller's pool (the bucket's binding for S3,
// TAPEBERG_POOL for the CLI) is just the fallback for an object with
// nothing on tape yet. Trusting the caller's pool sent restores to the
// wrong worker: a bucket rebound after archiving, or a CLI ingest made
// with a different TAPEBERG_POOL, queued the request in a pool whose
// library has never seen the tape.
func createRetrieveRequest(db *sql.DB, objectID int64, days int, fallbackPool string) (string, error) {
	pool, err := placementPool(db, objectID)
	if err != nil {
		return "", err
	}
	if pool == "" {
		pool = fallbackPool
	}
	_, err = db.Exec(`INSERT INTO retrieve_requests (object_id, pin_days, pool) VALUES ($1, $2, $3)`, objectID, days, pool)
	return pool, err
}

// errSpansPools marks the one permanent placementPool failure (as
// opposed to a transient DB error, which must not fail a restore).
var errSpansPools = errors.New("a restore is served by one library, but the object spans several pools")

// placementPool reports the pool of the tapes holding an object's bytes
// (every part and shard), or "" when nothing of it is on tape yet. An
// object spread over several pools is an error: a restore is drained by
// one worker, and one worker drives one library.
func placementPool(db *sql.DB, objectID int64) (string, error) {
	rows, err := db.Query(`
		SELECT DISTINCT p.name
		FROM tape_files tf
		JOIN tapes t ON t.id = tf.tape_id
		JOIN tape_pools p ON p.id = t.pool_id
		WHERE tf.object_id = $1
		ORDER BY 1`, objectID)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var pools []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return "", err
		}
		pools = append(pools, p)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	switch len(pools) {
	case 0:
		return "", nil
	case 1:
		return pools[0], nil
	default:
		return "", fmt.Errorf("object %d has tape placements in pools %v: %w", objectID, pools, errSpansPools)
	}
}

// setRetrievePool moves a pending restore to another pool's queue.
func setRetrievePool(db *sql.DB, reqID int64, pool string) error {
	_, err := db.Exec(`UPDATE retrieve_requests SET pool = $2, updated_at = now()
		WHERE id = $1 AND state = 'pending'`, reqID, pool)
	return err
}

// ---- multipart uploads ----
// Parts stay parts on disk and tape; the S3/CLI layer merges them.
// Each part is one atomic tape file (parts are <= 5GB), so objects
// larger than any single tape archive as many parts.

type PartRow struct {
	Number   int
	Size     int64
	MD5      string
	SHA256   string
	DiskPath string // "" when tape-only
}

func createUpload(db *sql.DB, bucket, key, uploadID string, owner int64) error {
	bid, err := bucketIDByName(db, bucket)
	if err != nil {
		return err
	}
	_, err = db.Exec(`INSERT INTO objects (bucket_id, key, size_bytes, storage_class, upload_id, owner_id)
		VALUES ($1, $2, 0, 'STANDARD', $3, NULLIF($4, 0))`, bid, key, uploadID, owner)
	return err
}

// objectByUpload resolves an open upload to its object id and owner
// (tenancy: parts may only be managed by the upload's owner or admins).
func objectByUpload(db *sql.DB, bucket, key, uploadID string) (int64, int64, error) {
	bid, err := bucketIDByName(db, bucket)
	if err != nil {
		return 0, 0, err
	}
	var id int64
	var owner sql.NullInt64
	err = db.QueryRow(`SELECT id, owner_id FROM objects
		WHERE bucket_id = $1 AND key = $2 AND upload_id = $3`, bid, key, uploadID).Scan(&id, &owner)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, fmt.Errorf("upload %q %q: %w", uploadID, key, ErrNotFound)
	}
	return id, owner.Int64, err
}

// insertPart records an uploaded part. Re-uploading a part number
// replaces it: any pending archive request is dropped and an already
// written tape file becomes dead space on that tape (v0 has no
// reclamation; the catalog row is removed so it is never read).
func insertPart(db *sql.DB, objectID int64, number int, size int64, md5, sha, diskPath string) error {
	if _, err := db.Exec(`DELETE FROM archive_requests
		WHERE object_id = $1 AND part_number = $2 AND state IN ('pending','assigned')`, objectID, number); err != nil {
		return err
	}
	if _, err := db.Exec(`DELETE FROM tape_files WHERE object_id = $1 AND part_number = $2`, objectID, number); err != nil {
		return err
	}
	if _, err := db.Exec(`DELETE FROM object_parts WHERE object_id = $1 AND part_number = $2`, objectID, number); err != nil {
		return err
	}
	_, err := db.Exec(`INSERT INTO object_parts (object_id, part_number, size_bytes, md5, sha256, disk_path)
		VALUES ($1, $2, $3, $4, $5, $6)`, objectID, number, size, md5, sha, diskPath)
	return err
}

func listParts(db *sql.DB, objectID int64) ([]PartRow, error) {
	rows, err := db.Query(`SELECT part_number, size_bytes, md5, sha256, COALESCE(disk_path,'')
		FROM object_parts WHERE object_id = $1 ORDER BY part_number`, objectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PartRow
	for rows.Next() {
		var p PartRow
		if err := rows.Scan(&p.Number, &p.Size, &p.MD5, &p.SHA256, &p.DiskPath); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func isMultipart(db *sql.DB, objectID int64) (bool, error) {
	var n int
	err := db.QueryRow(`SELECT count(*) FROM object_parts WHERE object_id = $1`, objectID).Scan(&n)
	return n > 0, err
}

// completeUpload seals a multipart upload. The S3-style ETag
// (md5-of-md5s-N) lives in objects.etag; there is deliberately no
// whole-file sha256 for multipart objects (the part checksums are
// the integrity record).
func completeUpload(db *sql.DB, objectID int64, etag string) error {
	_, err := db.Exec(`UPDATE objects SET upload_id = NULL, etag = $2,
		size_bytes = (SELECT COALESCE(sum(size_bytes),0) FROM object_parts WHERE object_id = $1)
		WHERE id = $1`, objectID, etag)
	return err
}

func setPartDiskPath(db *sql.DB, objectID int64, number int, path string) error {
	// "" maps to NULL: the part is tape-only.
	_, err := db.Exec(`UPDATE object_parts SET disk_path = NULLIF($3, '')
		WHERE object_id = $1 AND part_number = $2`, objectID, number, path)
	return err
}

// clearPartDiskPaths drops all restored/buffered part files (pin
// expiry). Returns the removed paths.
// abandonedUploads lists open multipart uploads older than ttlDays
// (created but never completed or aborted — the S3 lifecycle
// "AbortIncompleteMultipartUpload" behavior), in this rig's pool only:
// pool-scoped workers must not janitor another rig's uploads.
func abandonedUploads(db *sql.DB, ttlDays int, pool string) ([]int64, error) {
	rows, err := db.Query(`
		SELECT o.id FROM objects o
		JOIN buckets b ON b.id = o.bucket_id
		WHERE o.upload_id IS NOT NULL
		  AND o.deleted_at IS NULL
		  AND b.pool = $2
		  AND o.created_at < now() - ($1 || ' days')::interval
		ORDER BY o.id`, ttlDays, pool)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// objectKeyByID is for janitor logging.
func objectKeyByID(db *sql.DB, objectID int64) (string, error) {
	var key string
	err := db.QueryRow(`SELECT key FROM objects WHERE id = $1`, objectID).Scan(&key)
	return key, err
}

func clearPartDiskPaths(db *sql.DB, objectID int64) ([]string, error) {
	rows, err := db.Query(`SELECT disk_path FROM object_parts
		WHERE object_id = $1 AND disk_path IS NOT NULL`, objectID)
	if err != nil {
		return nil, err
	}
	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			rows.Close()
			return nil, err
		}
		paths = append(paths, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	_, err = db.Exec(`UPDATE object_parts SET disk_path = NULL WHERE object_id = $1`, objectID)
	return paths, err
}

// objectPendingParts counts archive requests still queued for the
// object — the worker flips it to GLACIER only when the last part is
// on tape.
func objectPendingParts(db *sql.DB, objectID int64) (int, error) {
	var n int
	err := db.QueryRow(`SELECT count(*) FROM archive_requests
		WHERE object_id = $1 AND state IN ('pending','assigned')`, objectID).Scan(&n)
	return n, err
}

// PartTapeFile locates one part's bytes for restoreOne.
type PartTapeFile struct {
	PartRow
	Barcode      string
	PhysicalFile int64
}

func multipartTapeFiles(db *sql.DB, objectID int64) ([]PartTapeFile, error) {
	rows, err := db.Query(`
		SELECT op.part_number, op.size_bytes, op.md5, op.sha256, COALESCE(op.disk_path,''),
		       t.barcode, tf.physical_file
		FROM object_parts op
		JOIN tape_files tf ON tf.object_id = op.object_id AND tf.part_number = op.part_number
		JOIN tapes t ON t.id = tf.tape_id
		WHERE op.object_id = $1 ORDER BY op.part_number`, objectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PartTapeFile
	for rows.Next() {
		var p PartTapeFile
		if err := rows.Scan(&p.Number, &p.Size, &p.MD5, &p.SHA256, &p.DiskPath, &p.Barcode, &p.PhysicalFile); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
type RetrieveReq struct {
	ID       int64
	ObjectID int64
	Key      string
	SHA256   string
	PinDays  int
}

func pendingRetrieves(db *sql.DB, pool string) ([]RetrieveReq, error) {
	rows, err := db.Query(`
		SELECT r.id, r.object_id, o.key, COALESCE(o.sha256, ''), r.pin_days
		FROM retrieve_requests r JOIN objects o ON o.id = r.object_id
		WHERE r.state = 'pending' AND r.pool = $1 ORDER BY r.requested_at`, pool)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RetrieveReq
	for rows.Next() {
		var r RetrieveReq
		if err := rows.Scan(&r.ID, &r.ObjectID, &r.Key, &r.SHA256, &r.PinDays); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

type TapeFileLoc struct {
	TapeID       int64
	Barcode      string
	ArchiveID    int64
	PhysicalFile int64
}

func objectTapeFile(db *sql.DB, objectID int64) (*TapeFileLoc, error) {
	t := &TapeFileLoc{}
	err := db.QueryRow(`
		SELECT tf.tape_id, t.barcode, tf.archive_id, tf.physical_file
		FROM tape_files tf JOIN tapes t ON t.id = tf.tape_id
		WHERE tf.object_id = $1 ORDER BY tf.written_at DESC LIMIT 1`, objectID).
		Scan(&t.TapeID, &t.Barcode, &t.ArchiveID, &t.PhysicalFile)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("object %d has no tape placement: %w", objectID, ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	return t, nil
}

func setRetrieveState(db *sql.DB, reqID int64, state string) error {
	// assigned_at marks the transition INTO an in-flight state; the
	// worker's stale-job reaper keys off it (issue #33). Going back to
	// pending (or serving a terminal state) clears it.
	_, err := db.Exec(`UPDATE retrieve_requests SET state=$2, assigned_at=CASE WHEN $2='restoring' THEN now() ELSE NULL END, updated_at=now() WHERE id=$1`, reqID, state)
	return err
}

func failRetrieve(db *sql.DB, reqID int64, msg string) error {
	_, err := db.Exec(`UPDATE retrieve_requests SET state='failed', error=$2, updated_at=now() WHERE id=$1`, reqID, msg)
	return err
}

func restoreComplete(db *sql.DB, reqID int64, expires time.Time) error {
	_, err := db.Exec(`
		UPDATE retrieve_requests
		SET state='restored', restored_at=now(), expires_at=$2, updated_at=now()
		WHERE id=$1`, reqID, expires)
	return err
}

// ---- pin expiry sweep ----

type ExpiredRow struct {
	ReqID    int64
	ObjectID int64
	DiskPath string
}

// expiredRestores lists pinned restores whose time is up, in this
// rig's pool only (pool-scoped workers must not expire another
// rig's pins).
func expiredRestores(db *sql.DB, pool string) ([]ExpiredRow, error) {
	rows, err := db.Query(`
		SELECT r.id, r.object_id, COALESCE(o.disk_path,'')
		FROM retrieve_requests r JOIN objects o ON o.id = r.object_id
		WHERE r.state = 'restored' AND r.expires_at < now() AND r.pool = $1
		  AND (o.disk_path IS NOT NULL OR EXISTS (
		    SELECT 1 FROM object_parts op
		    WHERE op.object_id = o.id AND op.disk_path IS NOT NULL))`, pool)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ExpiredRow
	for rows.Next() {
		var e ExpiredRow
		if err := rows.Scan(&e.ReqID, &e.ObjectID, &e.DiskPath); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func markRetrieveExpired(db *sql.DB, reqID int64) error {
	_, err := db.Exec(`UPDATE retrieve_requests SET state='expired', updated_at=now() WHERE id=$1`, reqID)
	return err
}

// ---- status ----

func printStatus(db *sql.DB) error {
	report := func(title, q string) error {
		rows, err := db.Query(q)
		if err != nil {
			return err
		}
		defer rows.Close()
		fmt.Println(title)
		for rows.Next() {
			var a string
			var b int64
			if err := rows.Scan(&a, &b); err != nil {
				return err
			}
			fmt.Printf("  %-16s %d\n", a+":", b)
		}
		return rows.Err()
	}
	if err := report("objects by storage class:", `SELECT storage_class, count(*) FROM objects GROUP BY 1 ORDER BY 1`); err != nil {
		return err
	}
	if err := report("tapes by state:", `SELECT state, count(*) FROM tapes GROUP BY 1 ORDER BY 1`); err != nil {
		return err
	}
	if err := report("archive requests:", `SELECT state, count(*) FROM archive_requests GROUP BY 1 ORDER BY 1`); err != nil {
		return err
	}
	if err := report("retrieve requests:", `SELECT state, count(*) FROM retrieve_requests GROUP BY 1 ORDER BY 1`); err != nil {
		return err
	}
	fmt.Println("tape fill:")
	rows, err := db.Query(`
		SELECT t.barcode, t.state, t.written_bytes, t.capacity_bytes, count(tf.id)
		FROM tapes t LEFT JOIN tape_files tf ON tf.tape_id = t.id
		GROUP BY t.id ORDER BY t.barcode`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var bc, st string
		var wb, cb, files int64
		if err := rows.Scan(&bc, &st, &wb, &cb, &files); err != nil {
			return err
		}
		fmt.Printf("  %-10s %-7s %8d/%d bytes, %d files\n", bc, st, wb, cb, files)
	}
	return rows.Err()
}
