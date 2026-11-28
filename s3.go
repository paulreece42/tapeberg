// S3/Glacier wire protocol for Tapeberg (work in progress).
//
// Implements the subset of Amazon S3 that makes awscli work against a
// homelab tape archive:
//   ListBuckets, CreateBucket, HeadBucket, ListObjectsV2, GetBucketLocation
//   PutObject, GetObject, HeadObject, RestoreObject
// Glacier semantics: after the worker archives an object its HEAD/GET
// report storage class GLACIER; GET returns 403 InvalidObjectState until
// RestoreObject (202 Accepted) has been drained by a worker pass; the
// restored copy is pinned for the requested days (x-amz-restore header).
//
// Auth: AWS Signature Version 4 (TAPEBERG_S3_KEY / TAPEBERG_S3_SECRET).
// With no keys configured the server runs in unsigned dev mode.
// Not implemented: multipart upload, presigned URLs, versioning, ACLs,
// DeleteObject, chunked (STREAMING-*) payloads.
package main

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const s3XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"

// cmdS3Serve runs the S3 endpoint until interrupted.
func cmdS3Serve(cfg Config, listen string) error {
	if listen == "" {
		listen = cfg.S3Listen
	}
	if listen == "" {
		listen = ":7333"
	}
	db, err := openDB(cfg.DBURL)
	if err != nil {
		return err
	}
	defer db.Close()
	auth, err := NewAuthenticator(cfg, db)
	if err != nil {
		return err
	}
	s := &s3srv{cfg: cfg, db: db, auth: auth}
	if !auth.Enabled() {
		fmt.Fprintln(os.Stderr, "s3: WARNING — no users and no TAPEBERG_S3_KEY/SECRET; running in UNSIGNED dev mode")
	}
	fmt.Printf("tapeberg s3: listening on %s (path-style, bucket-in-path, auth=%s)\n", listen, auth.Name())
	return http.ListenAndServe(listen, s)
}

type s3srv struct {
	cfg  Config
	db   *sql.DB
	auth Authenticator
}

// ---------------- SigV4 ----------------

type s3Auth struct {
	accessKey string
	date      string
	region    string
	service   string
	signed    []string
	signature string
}

// parseSigV4 parses the SigV4 Authorization header. Clients differ in
// spacing: awscli writes "Credential=..., SignedHeaders=..." (comma +
// space) while s3cmd packs it into one whitespace-free token — so split
// on commas and trim, never on whitespace.
func parseSigV4(h string) (*s3Auth, bool) {
	if !strings.HasPrefix(h, "AWS4-HMAC-SHA256 ") {
		return nil, false
	}
	fields := make(map[string]string)
	for _, part := range strings.Split(strings.TrimPrefix(h, "AWS4-HMAC-SHA256 "), ",") {
		if k, v, ok := strings.Cut(strings.TrimSpace(part), "="); ok {
			fields[strings.TrimSpace(k)] = v
		}
	}
	cred := strings.Split(fields["Credential"], "/")
	if len(cred) != 5 || fields["SignedHeaders"] == "" || fields["Signature"] == "" {
		return nil, false
	}
	return &s3Auth{
		accessKey: cred[0], date: cred[1], region: cred[2], service: cred[3],
		signed:    strings.Split(fields["SignedHeaders"], ";"),
		signature: fields["Signature"],
	}, true
}

func hmacSHA256(key []byte, s string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(s))
	return h.Sum(nil)
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// awsURIEncode implements the S3 flavor of RFC 3986 percent-encoding.
func awsURIEncode(s string, isPath bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '.', c == '_', c == '~':
			b.WriteByte(c)
		case isPath && c == '/':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// canonicalQuery rebuilds the query string per SigV4 rules.
func canonicalQuery(raw string) string {
	if raw == "" {
		return ""
	}
	type kv struct{ k, v string }
	var pairs []kv
	for _, p := range strings.Split(raw, "&") {
		k, v, _ := strings.Cut(p, "=")
		dk, e1 := urlUnescape(k)
		dv, e2 := urlUnescape(v)
		if e1 != nil {
			dk = k
		}
		if e2 != nil {
			dv = v
		}
		pairs = append(pairs, kv{awsURIEncode(dk, false), awsURIEncode(dv, false)})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k != pairs[j].k {
			return pairs[i].k < pairs[j].k
		}
		return pairs[i].v < pairs[j].v
	})
	parts := make([]string, len(pairs))
	for i, p := range pairs {
		parts[i] = p.k + "=" + p.v
	}
	return strings.Join(parts, "&")
}

func urlUnescape(s string) (string, error) {
	return url.QueryUnescape(s)
}

// canonicalRequest assembles the SigV4 canonical request from a live
// http.Request. Payload integrity is taken from the client-declared
// x-amz-content-sha256 (standard practice for streamed bodies).
func canonicalRequest(r *http.Request) (string, string) {
	payloadHash := r.Header.Get("x-amz-content-sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}
	headers := map[string]string{"host": r.Host}
	for name, vals := range r.Header {
		lower := strings.ToLower(name)
		// request line forms arrive as "X-Amz-Date"; map canonical names
		headers[lower] = strings.Join(vals, ",")
	}
	signed := make([]string, 0, len(headers))
	for h := range headers {
		signed = append(signed, h)
	}
	sort.Strings(signed)
	var ch strings.Builder
	for _, h := range signed {
		v := strings.TrimSpace(headers[h])
		v = strings.Join(strings.Fields(v), " ")
		ch.WriteString(h)
		ch.WriteString(":")
		ch.WriteString(v)
		ch.WriteString("\n")
	}
	signedList := strings.Join(signed, ";")
	return strings.Join([]string{
		r.Method,
		awsURIEncode(r.URL.Path, true),
		canonicalQuery(r.URL.RawQuery),
		ch.String(),
		signedList,
		payloadHash,
	}, "\n"), payloadHash
}

// verifySigV4 checks the Authorization header against the configured
// authenticator (builtin DB users by default; the resolved user's
// secret signs the request). Auth is pluggable: LDAP and AD will
// implement the same interface.
func (s *s3srv) verifySigV4(r *http.Request) (AuthUser, error) {
	if !s.auth.Enabled() {
		return AuthUser{UserID: "anonymous", IsAdmin: true}, nil // unsigned dev mode
	}
	auth, ok := parseSigV4(r.Header.Get("Authorization"))
	if !ok {
		return AuthUser{}, errS3("AccessDenied", "missing AWS SigV4 Authorization header", http.StatusForbidden)
	}
	secret, user, aerr := s.auth.LookupSecret(auth.accessKey)
	if errors.Is(aerr, ErrUnknownAccessKey) {
		return AuthUser{}, errS3("InvalidAccessKeyId", "unknown access key", http.StatusForbidden)
	}
	if aerr != nil {
		return AuthUser{}, aerr
	}
	if user.Suspended {
		return AuthUser{}, errS3("AccessDenied", "user "+user.UserID+" is suspended", http.StatusForbidden)
	}
	if auth.service != "s3" {
		return AuthUser{}, errS3("AccessDenied", "credential scope is not for s3", http.StatusForbidden)
	}
	// Rebuild the canonical request using the client's signed-headers list.
	headers := map[string]string{"host": r.Host}
	for name, vals := range r.Header {
		headers[strings.ToLower(name)] = strings.Join(vals, ",")
	}
	payloadHash := r.Header.Get("x-amz-content-sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}
	signed := append([]string(nil), auth.signed...)
	sort.Strings(signed)
	var ch strings.Builder
	for _, h := range signed {
		v := strings.TrimSpace(headers[h])
		v = strings.Join(strings.Fields(v), " ")
		ch.WriteString(h + ":" + v + "\n")
	}
	cr := strings.Join([]string{
		r.Method,
		awsURIEncode(r.URL.Path, true),
		canonicalQuery(r.URL.RawQuery),
		ch.String(),
		strings.Join(signed, ";"),
		payloadHash,
	}, "\n")
	scope := strings.Join([]string{auth.date, auth.region, auth.service, "aws4_request"}, "/")
	sts := "AWS4-HMAC-SHA256\n" + r.Header.Get("x-amz-date") + "\n" + scope + "\n" + sha256Hex([]byte(cr))
	kDate := hmacSHA256([]byte("AWS4"+secret), auth.date)
	kRegion := hmacSHA256(kDate, auth.region)
	kService := hmacSHA256(kRegion, auth.service)
	kSigning := hmacSHA256(kService, "aws4_request")
	want := hex.EncodeToString(hmacSHA256(kSigning, sts))
	if subtle.ConstantTimeCompare([]byte(want), []byte(auth.signature)) != 1 {
		if os.Getenv("TAPEBERG_S3_DEBUG") != "" {
			fmt.Fprintf(os.Stderr, "SIGV4 MISMATCH\n--- canonical request ---\n%s\n--- string to sign ---\n%s\n--- got %s want %s\n", cr, sts, auth.signature, want)
		}
		return AuthUser{}, errS3("SignatureDoesNotMatch", "signature mismatch", http.StatusForbidden)
	}
	return user, nil
}

// ---------------- XML shapes ----------------

type s3ErrorBody struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

type errS3T struct {
	code    string
	message string
	status  int
}

func (e *errS3T) Error() string { return e.code + ": " + e.message }

func errS3(code, msg string, status int) error {
	return &errS3T{code: code, message: msg, status: status}
}

func writeS3Error(w http.ResponseWriter, e error) {
	se, ok := e.(*errS3T)
	if !ok {
		se = &errS3T{code: "InternalError", message: e.Error(), status: http.StatusInternalServerError}
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(se.status)
	xml.NewEncoder(w).Encode(s3ErrorBody{
		Code: se.code, Message: se.message,
		Resource: "", RequestID: strconv.FormatInt(time.Now().UnixNano(), 10),
	})
}

type bucketXML struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type listBucketsXML struct {
	XMLName xml.Name    `xml:"ListAllMyBucketsResult"`
	Xmlns   string      `xml:"xmlns,attr"`
	Owner   ownerXML    `xml:"Owner"`
	Buckets []bucketXML `xml:"Buckets>Bucket"`
}

type ownerXML struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type objectXML struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type listObjectsXML struct {
	XMLName     xml.Name    `xml:"ListBucketResult"`
	Xmlns       string      `xml:"xmlns,attr"`
	Name        string      `xml:"Name"`
	Prefix      string      `xml:"Prefix"`
	KeyCount    int         `xml:"KeyCount"`
	MaxKeys     int         `xml:"MaxKeys"`
	IsTruncated bool        `xml:"IsTruncated"`
	Contents    []objectXML `xml:"Contents"`
}

type restoreRequestXML struct {
	Days int `xml:"Days"`
}

// ---------------- handlers ----------------

type userCtxKey struct{}

// resolveUser verifies the request and returns the caller. When auth
// is disabled (unsigned dev mode) the caller is an omniscient
// "anonymous" admin — the single-user homelab bootstrap.
func (s *s3srv) resolveUser(r *http.Request) (AuthUser, error) {
	if !s.auth.Enabled() {
		return AuthUser{UserID: "anonymous", IsAdmin: true}, nil
	}
	u, err := s.verifySigV4(r)
	if err != nil {
		return u, err
	}
	return u, nil
}

func (s *s3srv) user(r *http.Request) AuthUser {
	if u, ok := r.Context().Value(userCtxKey{}).(AuthUser); ok {
		return u
	}
	return AuthUser{UserID: "anonymous", IsAdmin: true}
}

// canSee reports whether the caller may access an owned row (object or
// bucket). Owner 0 = system (CLI ingests, legacy rows): admin-only.
func canSee(u AuthUser, owner int64) bool {
	return u.IsAdmin || owner == u.ID
}

func (s *s3srv) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u, err := s.resolveUser(r)
	if err != nil {
		writeS3Error(w, err)
		return
	}
	r = r.WithContext(context.WithValue(r.Context(), userCtxKey{}, u))
	path := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(path, "/")
	if key != "" {
		if err := checkObjectKey(key); err != nil {
			writeS3Error(w, err)
			return
		}
	}

	switch {
	case bucket == "":
		err = s.serveRoot(w, r)
	case key == "":
		err = s.serveBucket(w, r, bucket)
	default:
		err = s.serveObject(w, r, bucket, key)
	}
	if err != nil {
		writeS3Error(w, err)
	}
}

func (s *s3srv) serveRoot(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodGet {
		return errS3("MethodNotAllowed", "operation not supported", http.StatusMethodNotAllowed)
	}
	rows, err := listBucketsFor(s.db, s.user(r))
	if err != nil {
		return err
	}
	out := listBucketsXML{Xmlns: s3XMLNS, Owner: ownerXML{ID: s.user(r).UserID, DisplayName: s.user(r).FullName}}
	for _, b := range rows {
		out.Buckets = append(out.Buckets, bucketXML{Name: b.Name, CreationDate: b.Created.Format(time.RFC3339)})
	}
	w.Header().Set("Content-Type", "application/xml")
	return xml.NewEncoder(w).Encode(out)
}

func (s *s3srv) serveBucket(w http.ResponseWriter, r *http.Request, bucket string) error {
	switch r.Method {
	case http.MethodPut:
		// CreateBucket: the bucket binds to this server's tape pool —
		// pool = tape set, so the S3 namespace mirrors the pools — and
		// to the creating user (tenancy: non-admins see only their own).
		u := s.user(r)
		if owner, err := bucketOwner(s.db, bucket); err == nil && !canSee(u, owner) {
			return errS3("BucketAlreadyExists", "bucket exists and is not yours", http.StatusConflict)
		}
		if err := createBucketIfAbsent(s.db, bucket, s.cfg.Pool, u.ID); err != nil {
			return err
		}
		w.Header().Set("Location", "/"+bucket)
		w.WriteHeader(http.StatusOK)
		return nil
	case http.MethodHead:
		owner, err := bucketOwner(s.db, bucket)
		if err != nil {
			return notFoundOr(err, "NoSuchBucket", "bucket does not exist")
		}
		if !canSee(s.user(r), owner) {
			return errS3("NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		}
		w.WriteHeader(http.StatusOK)
		return nil
	case http.MethodGet:
		if r.URL.Query().Has("location") {
			owner, err := bucketOwner(s.db, bucket)
			if err != nil || !canSee(s.user(r), owner) {
				return errS3("NoSuchBucket", "bucket does not exist", http.StatusNotFound)
			}
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprintf(w, `<LocationConstraint xmlns=%q>us-east-1</LocationConstraint>`, s3XMLNS)
			return nil
		}
		owner, err := bucketOwner(s.db, bucket)
		if err != nil {
			return notFoundOr(err, "NoSuchBucket", "bucket does not exist")
		}
		if !canSee(s.user(r), owner) {
			return errS3("NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		}
		objs, err := listObjectsFor(s.db, bucket, r.URL.Query().Get("prefix"), s.user(r))
		if err != nil {
			return err
		}
		out := listObjectsXML{Xmlns: s3XMLNS, Name: bucket, Prefix: r.URL.Query().Get("prefix"),
			MaxKeys: 1000, KeyCount: len(objs)}
		for _, o := range objs {
			sc := o.StorageClass
			if sc == "STANDARD" {
				sc = "" // STANDARD is the default; S3 omits it
			}
			out.Contents = append(out.Contents, objectXML{
				Key: o.Key, LastModified: o.CreatedAt.Format(time.RFC3339),
				ETag: `"` + o.etagOf() + `"`, Size: o.Size, StorageClass: sc,
			})
		}
		w.Header().Set("Content-Type", "application/xml")
		return xml.NewEncoder(w).Encode(out)
	default:
		return errS3("NotImplemented", "operation not supported (v0)", http.StatusNotImplemented)
	}
}

func (s *s3srv) serveObject(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodPost && q.Has("uploads"):
		return s.initiateMultipart(w, r, bucket, key)
	case r.Method == http.MethodPut && q.Has("uploadId") && q.Has("partNumber"):
		return s.putPart(w, r, bucket, key)
	case r.Method == http.MethodGet && q.Has("uploadId"):
		return s.listUploadParts(w, r, bucket, key)
	case r.Method == http.MethodPost && q.Has("uploadId"):
		return s.completeMultipart(w, r, bucket, key)
	case r.Method == http.MethodDelete && q.Has("uploadId"):
		return s.abortMultipart(w, r, bucket, key)
	case r.Method == http.MethodDelete && !q.Has("uploadId"):
		return s.deleteObject(w, r, bucket, key)
	case r.Method == http.MethodPut && !q.Has("tagging"):
		return s.putObject(w, r, bucket, key)
	case r.Method == http.MethodGet || r.Method == http.MethodHead:
		return s.getObject(w, r, bucket, key)
	case r.Method == http.MethodPost && q.Has("restore"):
		return s.restoreObject(w, r, bucket, key)
	default:
		return errS3("NotImplemented", "operation not supported (v0)", http.StatusNotImplemented)
	}
}

// notFoundOr maps a catalog lookup failure to S3's "doesn't exist"
// error — but only for genuine ErrNotFound. Anything else (catalog
// down, a constraint, a timeout) must surface as InternalError: the
// old blanket 404/204 made DeleteObject report success for nothing and
// served 404s for fully real objects during a blip (issue #35).
func notFoundOr(err error, code, msg string) error {
	if errors.Is(err, ErrNotFound) {
		return errS3(code, msg, http.StatusNotFound)
	}
	return errS3("InternalError", err.Error(), http.StatusInternalServerError)
}

func (s *s3srv) putObject(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	u := s.user(r)
	owner, err := bucketOwner(s.db, bucket)
	if err != nil {
		return notFoundOr(err, "NoSuchBucket", "bucket does not exist")
	}
	if !canSee(u, owner) {
		return errS3("NoSuchBucket", "bucket does not exist", http.StatusNotFound)
	}
	// The bucket's bound pool decides which tape set archives this object.
	pool, err := bucketPool(s.db, bucket)
	if err != nil {
		return notFoundOr(err, "NoSuchBucket", "bucket does not exist")
	}
	obj, err := ingestStream(s.db, s.cfg, bucket, key, requestBody(r), pool, u.ID)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return errS3("OperationAborted", "key exists (v0 has no overwrite)", http.StatusConflict)
		}
		return err
	}
	w.Header().Set("ETag", `"`+obj.etagOf()+`"`)
	w.WriteHeader(http.StatusOK)
	return nil
}

// setObjectHeaders writes the common object metadata headers.
func setObjectHeaders(w http.ResponseWriter, obj *Object, st string, exp time.Time, found bool) {
	w.Header().Set("ETag", `"`+obj.etagOf()+`"`)
	w.Header().Set("Last-Modified", obj.CreatedAt.UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
	if obj.StorageClass == "GLACIER" || obj.StorageClass == "DEEP_ARCHIVE" {
		w.Header().Set("x-amz-storage-class", obj.StorageClass)
	}
	if found {
		switch st {
		case "restored":
			w.Header().Set("x-amz-restore",
				`ongoing-request="false", expiry-date="`+exp.UTC().Format(http.TimeFormat)+`"`)
		case "pending", "waiting_for_media", "restoring":
			w.Header().Set("x-amz-restore", `ongoing-request="true"`)
		}
	}
}

func (s *s3srv) getObject(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	obj, err := getObjectIn(s.db, bucket, key)
	if err != nil {
		return notFoundOr(err, "NoSuchKey", "key does not exist")
	}
	// Tenancy: only the owner or an admin reads an object.
	if !canSee(s.user(r), obj.OwnerID) {
		return errS3("AccessDenied", "access denied", http.StatusForbidden)
	}
	// Multipart: merge at the API layer — the parts concatenate in
	// order (Range requests included, via the concat seek reader).
	if mp, err := isMultipart(s.db, obj.ID); err != nil {
		return err
	} else if mp {
		return s.getMultipart(w, r, obj, key)
	}
	if obj.DiskPath == "" {
		if obj.StorageClass == "GLACIER" || obj.StorageClass == "DEEP_ARCHIVE" {
			if r.Method == http.MethodHead {
				// HEAD is metadata-only and always succeeds (S3 clients
				// discover the storage class this way); GET is refused.
				st, exp, found, err := activeRetrieve(s.db, obj.ID)
				if err != nil {
					return err
				}
				setObjectHeaders(w, obj, st, exp, found)
				w.WriteHeader(http.StatusOK)
				return nil
			}
			return errS3("InvalidObjectState",
				"The operation is not valid for the object's storage class ("+obj.StorageClass+") — run restore-object first",
				http.StatusForbidden)
		}
		return errS3("InternalError", "object has no readable copy", http.StatusInternalServerError)
	}
	st, exp, found, err := activeRetrieve(s.db, obj.ID)
	if err != nil {
		return err
	}
	f, err := os.Open(obj.DiskPath)
	if err != nil {
		return err
	}
	defer f.Close()
	setObjectHeaders(w, obj, st, exp, found)
	// ServeContent handles Content-Length, ranges, and HEAD no-body.
	http.ServeContent(w, r, key, obj.CreatedAt, f)
	return nil
}

// getMultipart serves a multipart object: HEAD with metadata, GET by
// concatenating the materialized part files (the merge lives in the
// concat reader; the parts themselves never coalesce on disk/tape).
func (s *s3srv) getMultipart(w http.ResponseWriter, r *http.Request, obj *Object, key string) error {
	parts, err := listParts(s.db, obj.ID)
	if err != nil {
		return err
	}
	paths := make([]string, 0, len(parts))
	for _, p := range parts {
		if p.DiskPath == "" {
			paths = nil
			break
		}
		paths = append(paths, p.DiskPath)
	}
	if paths == nil {
		if obj.StorageClass == "GLACIER" || obj.StorageClass == "DEEP_ARCHIVE" {
			st, exp, found, err := activeRetrieve(s.db, obj.ID)
			if err != nil {
				return err
			}
			if r.Method == http.MethodHead {
				setObjectHeaders(w, obj, st, exp, found)
				w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
				w.WriteHeader(http.StatusOK)
				return nil
			}
			return errS3("InvalidObjectState",
				"The operation is not valid for the object's storage class ("+obj.StorageClass+") — run restore-object first",
				http.StatusForbidden)
		}
		return errS3("InvalidObjectState",
			"parts are still being archived — wait for the worker pass, or run restore-object",
			http.StatusForbidden)
	}
	st, exp, found, err := activeRetrieve(s.db, obj.ID)
	if err != nil {
		return err
	}
	c, err := newConcatReadSeeker(paths)
	if err != nil {
		return err
	}
	defer c.Close()
	setObjectHeaders(w, obj, st, exp, found)
	http.ServeContent(w, r, key, obj.CreatedAt, c)
	return nil
}

// deleteObject implements S3 DeleteObject as a soft delete: the object
// is tombstoned (GET/HEAD/list stop seeing it; queued archives are
// cancelled; disk copies are dropped) and the tape bytes stay until
// the worker's repack pass reclaims the tape. Idempotent: a missing
// key also returns 204.
func (s *s3srv) deleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	obj, err := getObjectIn(s.db, bucket, key)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			return err // transient catalog failure is NOT "already gone"
		}
		w.WriteHeader(http.StatusNoContent) // never existed (or already deleted)
		return nil
	}
	if !canSee(s.user(r), obj.OwnerID) {
		return errS3("AccessDenied", "access denied", http.StatusForbidden)
	}
	files, err := softDeleteObject(s.db, obj.ID)
	if err != nil {
		return err
	}
	for _, f := range files {
		if f != "" {
			_ = os.Remove(f)
		}
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *s3srv) restoreObject(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	obj, err := getObjectIn(s.db, bucket, key)
	if err != nil {
		return notFoundOr(err, "NoSuchKey", "key does not exist")
	}
	if !canSee(s.user(r), obj.OwnerID) {
		return errS3("AccessDenied", "access denied", http.StatusForbidden)
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	var req restoreRequestXML
	if len(body) > 0 {
		if err := xml.Unmarshal(body, &req); err != nil {
			return errS3("MalformedXML", "could not parse RestoreRequest", http.StatusBadRequest)
		}
	}
	days := req.Days
	if days == 0 {
		days = 1
	}
	if days < 1 || days > 30 {
		return errS3("InvalidArgument", "Days must be 1-30", http.StatusBadRequest)
	}
	st, _, found, err := activeRetrieve(s.db, obj.ID)
	if err != nil {
		return err
	}
	if found && st == "restored" {
		w.WriteHeader(http.StatusOK) // already restored — idempotent per S3
		return nil
	}
	if found {
		return errS3("RestoreAlreadyInProgress", "object restore is already in progress", http.StatusConflict)
	}
	// The bucket's bound pool is only the fallback: a restore queues
	// where the object's tapes are (createRetrieveRequest).
	pool, err := bucketPool(s.db, bucket)
	if err != nil {
		return notFoundOr(err, "NoSuchBucket", "bucket does not exist")
	}
	if _, err := createRetrieveRequest(s.db, obj.ID, days, pool); err != nil {
		return err
	}
	w.WriteHeader(http.StatusAccepted)
	return nil
}


// ---- multipart uploads ----
// Parts stay parts on disk and tape (each part one atomic tape file);
// GET merges at the API layer by concatenating parts in order. Parts
// archive as they arrive, so a tar-pipe larger than the buffer disk
// streams to tape while the upload is still running.

// awsChunkedReader decodes the aws-chunked framing that awscli uses
// for streaming uploads (x-amz-content-sha256: STREAMING-AWS4-...):
// "<hex-size>[;chunk-signature=hex]\r\n<data>\r\n" repeated, "0...\r\n"
// to finish. Go's http server does not strip this application-layer
// encoding; without it every part would carry ~54 bytes of framing.
type awsChunkedReader struct {
	br        *bufio.Reader
	remaining int64
	done      bool
}

func newAwsChunkedReader(r io.Reader) *awsChunkedReader {
	return &awsChunkedReader{br: bufio.NewReader(r)}
}

func (c *awsChunkedReader) Read(p []byte) (int, error) {
	for c.remaining == 0 && !c.done {
		line, err := c.br.ReadString('\n')
		if err != nil {
			return 0, io.ErrUnexpectedEOF
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		sizeStr, _, _ := strings.Cut(line, ";")
		n, perr := strconv.ParseInt(sizeStr, 16, 64)
		if perr != nil {
			return 0, fmt.Errorf("bad aws-chunked size %q", line)
		}
		if n == 0 {
			c.done = true
			break
		}
		c.remaining = n
	}
	if c.done {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	if int64(len(p)) > c.remaining {
		p = p[:c.remaining]
	}
	n, err := c.br.Read(p)
	c.remaining -= int64(n)
	return n, err
}

// requestBody returns the decoded request body (aws-chunked framing
// stripped when the client streamed it).
func requestBody(r *http.Request) io.Reader {
	if strings.HasPrefix(r.Header.Get("x-amz-content-sha256"), "STREAMING") {
		return newAwsChunkedReader(r.Body)
	}
	return r.Body
}

type initiateMultipartResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

func (s *s3srv) initiateMultipart(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	// Tenancy guard, same rule as putObject: initiating an upload only
	// checks that the bucket EXISTS otherwise, any authenticated user
	// could park uploads in another user's bucket, holding the key
	// hostage (the owner's own PUT of it 409s) with objects the owner
	// cannot see. putPart and completeMultipart check the upload's
	// owner — which is the caller — so initiation is the gate.
	u := s.user(r)
	owner, err := bucketOwner(s.db, bucket)
	if err != nil {
		return notFoundOr(err, "NoSuchBucket", "bucket does not exist")
	}
	if !canSee(u, owner) {
		return errS3("NoSuchBucket", "bucket does not exist", http.StatusNotFound)
	}
	// v0 has no overwrite; the upload's placeholder object row would
	// otherwise collide on objects_bucket_key_live_uniq and surface as
	// a 500. Say 409 like putObject does (issue #37).
	if _, err := getObjectIn(s.db, bucket, key); err == nil {
		return errS3("OperationAborted", "key exists (v0 has no overwrite)", http.StatusConflict)
	}
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return err
	}
	uploadID := hex.EncodeToString(raw)
	if err := createUpload(s.db, bucket, key, uploadID, s.user(r).ID); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/xml")
	return xml.NewEncoder(w).Encode(initiateMultipartResult{
		Xmlns: s3XMLNS, Bucket: bucket, Key: key, UploadID: uploadID})
}

// putPart streams one part to the buffer, hashes it (md5 = S3 part
// ETag, sha256 = restore verifier), records it, and queues the part
// for tape in the bucket's pool — the upload may still be streaming.
func (s *s3srv) putPart(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	pool, err := bucketPool(s.db, bucket)
	if err != nil {
		return notFoundOr(err, "NoSuchBucket", "bucket does not exist")
	}
	uploadID := r.URL.Query().Get("uploadId")
	part, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil || part < 1 || part > 10000 {
		return errS3("InvalidArgument", "partNumber must be 1-10000", http.StatusBadRequest)
	}
	objID, owner, err := objectByUpload(s.db, bucket, key, uploadID)
	if err != nil {
		return notFoundOr(err, "NoSuchUpload", "upload does not exist")
	}
	if !canSee(s.user(r), owner) {
		return errS3("AccessDenied", "access denied", http.StatusForbidden)
	}
	if err := os.MkdirAll(s.cfg.BufferDir, 0o755); err != nil {
		return err
	}
	dst := filepath.Join(s.cfg.BufferDir, fmt.Sprintf("part-%d-%05d", objID, part))
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	mh, sh := md5.New(), sha256.New()
	size, err := io.Copy(io.MultiWriter(f, mh, sh), requestBody(r))
	cerr := f.Close()
	if err != nil {
		_ = os.Remove(dst)
		return err
	}
	if cerr != nil {
		_ = os.Remove(dst)
		return cerr
	}
	if size == 0 {
		_ = os.Remove(dst)
		return errS3("EntityTooSmall", "part size must be > 0 (S3 minimum is 5MB for non-final parts)", http.StatusBadRequest)
	}
	if s.cfg.PartMaxBytes > 0 && size > s.cfg.PartMaxBytes {
		_ = os.Remove(dst)
		return errS3("EntityTooLarge",
			fmt.Sprintf("part size %d exceeds the configured maximum %d (TAPEBERG_PART_MAX_BYTES)", size, s.cfg.PartMaxBytes),
			http.StatusBadRequest)
	}
	if err := insertPart(s.db, objID, part, size,
		hex.EncodeToString(mh.Sum(nil)), hex.EncodeToString(sh.Sum(nil)), dst); err != nil {
		_ = os.Remove(dst)
		return err
	}
	if err := createArchiveRequest(s.db, objID, pool, part); err != nil {
		return err
	}
	w.Header().Set("ETag", `"`+hex.EncodeToString(mh.Sum(nil))+`"`)
	w.WriteHeader(http.StatusOK)
	return nil
}

type completePartXML struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeMultipartXML struct {
	XMLName xml.Name           `xml:"CompleteMultipartUpload"`
	Parts   []completePartXML `xml:"Part"`
}

type completeMultipartResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

func (s *s3srv) completeMultipart(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	uploadID := r.URL.Query().Get("uploadId")
	objID, owner, err := objectByUpload(s.db, bucket, key, uploadID)
	if err != nil {
		return notFoundOr(err, "NoSuchUpload", "upload does not exist")
	}
	if !canSee(s.user(r), owner) {
		return errS3("AccessDenied", "access denied", http.StatusForbidden)
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	var req completeMultipartXML
	if err := xml.Unmarshal(body, &req); err != nil {
		return errS3("MalformedXML", "could not parse CompleteMultipartUpload", http.StatusBadRequest)
	}
	if len(req.Parts) == 0 {
		return errS3("MalformedXML", "no parts listed", http.StatusBadRequest)
	}
	parts, err := listParts(s.db, objID)
	if err != nil {
		return err
	}
	byNumber := make(map[int]PartRow, len(parts))
	for _, p := range parts {
		byNumber[p.Number] = p
	}
	// S3 semantics: only the listed parts become the object; catalog
	// parts not listed are dropped (their tape files, if written,
	// become dead space).
	listed := make(map[int]bool, len(req.Parts))
	for _, lp := range req.Parts {
		p, ok := byNumber[lp.PartNumber]
		if !ok {
			return errS3("InvalidPart", fmt.Sprintf("part %d was not uploaded", lp.PartNumber), http.StatusBadRequest)
		}
		if et := strings.Trim(lp.ETag, `"`); et != "" && !strings.EqualFold(et, p.MD5) {
			return errS3("InvalidPart", fmt.Sprintf("part %d ETag mismatch (got %s, have %s)", lp.PartNumber, lp.ETag, p.MD5), http.StatusBadRequest)
		}
		var st string
		qerr := s.db.QueryRow(`SELECT state FROM archive_requests
			WHERE object_id = $1 AND part_number = $2
			ORDER BY id DESC LIMIT 1`, objID, lp.PartNumber).Scan(&st)
		if errors.Is(qerr, sql.ErrNoRows) {
			return errS3("InvalidPart", fmt.Sprintf("part %d has no archive request", lp.PartNumber), http.StatusBadRequest)
		}
		if qerr != nil {
			return qerr
		}
		if st == "failed" {
			return errS3("InvalidPart", fmt.Sprintf("part %d failed to archive: see the request error", lp.PartNumber), http.StatusBadRequest)
		}
		listed[lp.PartNumber] = true
	}
	// S3 contract: every listed part except the FINAL one must meet
	// the minimum size (on tape, small parts shoe-shine the drive).
	// The highest listed part number is final.
	maxPart := 0
	for _, lp := range req.Parts {
		if lp.PartNumber > maxPart {
			maxPart = lp.PartNumber
		}
	}
	for _, lp := range req.Parts {
		p := byNumber[lp.PartNumber]
		if lp.PartNumber != maxPart && s.cfg.PartMinBytes > 0 && p.Size < s.cfg.PartMinBytes {
			return errS3("EntityTooSmall",
				fmt.Sprintf("part %d is %d bytes; non-final parts must be at least %d (TAPEBERG_PART_MIN_BYTES)",
					lp.PartNumber, p.Size, s.cfg.PartMinBytes),
				http.StatusBadRequest)
		}
	}
	// v0 has no overwrite: the completed key must be free. This check
	// MUST come before the unlisted-part drops below, or the 409 a
	// client can correct (retry with a different key) arrives after we
	// already destroyed the parts it would retry with (issue #37).
	if _, err := getObjectIn(s.db, bucket, key); err == nil {
		return errS3("OperationAborted", "key exists (v0 has no overwrite)", http.StatusConflict)
	}
	for _, p := range parts {
		if !listed[p.Number] {
			if _, err := s.db.Exec(`DELETE FROM archive_requests
				WHERE object_id = $1 AND part_number = $2 AND state IN ('pending','assigned')`, objID, p.Number); err != nil {
				return err
			}
			if _, err := s.db.Exec(`DELETE FROM tape_files WHERE object_id = $1 AND part_number = $2`, objID, p.Number); err != nil {
				return err
			}
			if _, err := s.db.Exec(`DELETE FROM object_shards WHERE object_id = $1 AND part_number = $2`, objID, p.Number); err != nil {
				return err
			}
			if p.DiskPath != "" {
				_ = os.Remove(p.DiskPath)
			}
			if _, err := s.db.Exec(`DELETE FROM object_parts WHERE object_id = $1 AND part_number = $2`, objID, p.Number); err != nil {
				return err
			}
		}
	}
	// S3-style multipart ETag: md5 of the concatenated binary part md5s.
	h := md5.New()
	var total int64
	for _, lp := range req.Parts {
		p := byNumber[lp.PartNumber]
		raw, _ := hex.DecodeString(p.MD5)
		h.Write(raw)
		total += p.Size
	}
	etag := hex.EncodeToString(h.Sum(nil)) + "-" + strconv.Itoa(len(req.Parts))
	if err := completeUpload(s.db, objID, etag); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/xml")
	return xml.NewEncoder(w).Encode(completeMultipartResult{
		Xmlns:    s3XMLNS,
		Location: "http://" + r.Host + "/" + bucket + "/" + key,
		Bucket:   bucket, Key: key, ETag: etag,
	})
}

func (s *s3srv) abortMultipart(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	uploadID := r.URL.Query().Get("uploadId")
	objID, owner, err := objectByUpload(s.db, bucket, key, uploadID)
	if err != nil {
		return notFoundOr(err, "NoSuchUpload", "upload does not exist")
	}
	if !canSee(s.user(r), owner) {
		return errS3("AccessDenied", "access denied", http.StatusForbidden)
	}
	// Abort kills the in-progress object the same way a delete kills
	// a finished one: a tombstone. Queued/assigned part archives are
	// cancelled; buffered disk parts are removed now; parts already
	// on tape stay as dead bytes until repack reclaims them (the
	// worker may even be mid-write — the tombstone is what makes
	// those bytes dead, so hard-DELETEing here would lose the
	// accounting AND trip object_shards' foreign key).
	paths, err := softDeleteObject(s.db, objID)
	if err != nil {
		return err
	}
	for _, p := range paths {
		_ = os.Remove(p)
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

type listPartsResult struct {
	XMLName  xml.Name `xml:"ListPartsResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
	Parts    []struct {
		PartNumber   int    `xml:"PartNumber"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
		LastModified string `xml:"LastModified"`
	} `xml:"Part"`
}

func (s *s3srv) listUploadParts(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	uploadID := r.URL.Query().Get("uploadId")
	objID, owner, err := objectByUpload(s.db, bucket, key, uploadID)
	if err != nil {
		return notFoundOr(err, "NoSuchUpload", "upload does not exist")
	}
	if !canSee(s.user(r), owner) {
		return errS3("AccessDenied", "access denied", http.StatusForbidden)
	}
	parts, err := listParts(s.db, objID)
	if err != nil {
		return err
	}
	out := listPartsResult{Xmlns: s3XMLNS, Bucket: bucket, Key: key, UploadID: uploadID}
	for _, p := range parts {
		out.Parts = append(out.Parts, struct {
			PartNumber   int    `xml:"PartNumber"`
			ETag         string `xml:"ETag"`
			Size         int64  `xml:"Size"`
			LastModified string `xml:"LastModified"`
		}{p.Number, p.MD5, p.Size, time.Now().UTC().Format(time.RFC3339)})
	}
	w.Header().Set("Content-Type", "application/xml")
	return xml.NewEncoder(w).Encode(out)
}

// concatReadSeeker reads across an ordered list of part files as one
// virtual file — the multipart merge, including Range requests via
// http.ServeContent.
type concatReadSeeker struct {
	parts []*os.File
	sizes []int64
	pos   int64
}

func newConcatReadSeeker(paths []string) (*concatReadSeeker, error) {
	c := &concatReadSeeker{}
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			c.Close()
			return nil, err
		}
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			c.Close()
			return nil, err
		}
		c.parts = append(c.parts, f)
		c.sizes = append(c.sizes, fi.Size())
	}
	return c, nil
}

func (c *concatReadSeeker) Close() {
	for _, f := range c.parts {
		_ = f.Close()
	}
	c.parts = nil
}

func (c *concatReadSeeker) total() int64 {
	var t int64
	for _, s := range c.sizes {
		t += s
	}
	return t
}

func (c *concatReadSeeker) Read(p []byte) (int, error) {
	if c.pos >= c.total() {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	// Locate the part containing pos and read at the exact offset.
	// ReadAt keeps every part file positionless: the old per-file cursor
	// went stale after a backward seek (Seek repositioned only the target
	// part), and the next Read then continued a later part from mid-file
	// — bytes skipped, served with 200/206 (issue #41).
	var base int64
	for i, s := range c.sizes {
		if c.pos < base+s {
			n, err := c.parts[i].ReadAt(p, c.pos-base)
			c.pos += int64(n)
			if err != nil && err != io.EOF {
				return n, err
			}
			// ReadAt reports EOF whenever it hit this part's end, but
			// more parts may follow: EOF is only true at the very end.
			if n > 0 && c.pos < c.total() {
				err = nil
			}
			return n, err
		}
		base += s
	}
	return 0, io.EOF
}

func (c *concatReadSeeker) Seek(offset int64, whence int) (int64, error) {
	total := c.total()
	var target int64
	switch whence {
	case io.SeekStart:
		target = offset
	case io.SeekCurrent:
		target = c.pos + offset
	case io.SeekEnd:
		target = total + offset
	}
	if target < 0 {
		return 0, fmt.Errorf("negative seek")
	}
	c.pos = target
	return c.pos, nil
}
