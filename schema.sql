-- Tapeberg catalog schema, generated from db/migrations/ (0001 … 0012_stale_jobs.sql — update when they change).
--
-- db/migrations/ remains the canonical history; this file is a
-- convenience for reading the whole schema, diffing across versions, or
-- bootstrap of tools that can't run migrations. db/seed.sql is optional
-- fixture data. Regenerate with: scripts/gen-schema.sh
--
-- PostgreSQL database dump
--



SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
--



SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: archive_requests; Type: TABLE; Schema: -; Owner: -
--

CREATE TABLE archive_requests (
    id bigint NOT NULL,
    object_id bigint NOT NULL,
    state text DEFAULT 'pending'::text NOT NULL,
    tape_id bigint,
    error text,
    requested_at timestamp with time zone DEFAULT now() NOT NULL,
    written_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    pool text DEFAULT 'default'::text NOT NULL,
    part_number integer DEFAULT 1 NOT NULL,
    assigned_at timestamp with time zone,
    CONSTRAINT archive_requests_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'assigned'::text, 'written'::text, 'failed'::text, 'cancelled'::text])))
);


--
-- Name: archive_requests_id_seq; Type: SEQUENCE; Schema: -; Owner: -
--

CREATE SEQUENCE archive_requests_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: archive_requests_id_seq; Type: SEQUENCE OWNED BY; Schema: -; Owner: -
--

ALTER SEQUENCE archive_requests_id_seq OWNED BY archive_requests.id;


--
-- Name: buckets; Type: TABLE; Schema: -; Owner: -
--

CREATE TABLE buckets (
    id bigint NOT NULL,
    name text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    pool text DEFAULT 'default'::text NOT NULL,
    owner_id bigint
);


--
-- Name: buckets_id_seq; Type: SEQUENCE; Schema: -; Owner: -
--

CREATE SEQUENCE buckets_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: buckets_id_seq; Type: SEQUENCE OWNED BY; Schema: -; Owner: -
--

ALTER SEQUENCE buckets_id_seq OWNED BY buckets.id;


--
-- Name: object_parts; Type: TABLE; Schema: -; Owner: -
--

CREATE TABLE object_parts (
    object_id bigint NOT NULL,
    part_number integer NOT NULL,
    size_bytes bigint NOT NULL,
    md5 text NOT NULL,
    sha256 text NOT NULL,
    disk_path text
);


--
-- Name: object_shards; Type: TABLE; Schema: -; Owner: -
--

CREATE TABLE object_shards (
    object_id bigint NOT NULL,
    part_number integer DEFAULT 0 NOT NULL,
    shard_number integer NOT NULL,
    size_bytes bigint NOT NULL,
    sha256 text NOT NULL
);


--
-- Name: objects; Type: TABLE; Schema: -; Owner: -
--

CREATE TABLE objects (
    id bigint NOT NULL,
    bucket_id bigint NOT NULL,
    key text NOT NULL,
    size_bytes bigint NOT NULL,
    sha256 text,
    storage_class text DEFAULT 'STANDARD'::text NOT NULL,
    disk_path text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    upload_id text,
    owner_id bigint,
    deleted_at timestamp with time zone,
    ec_k integer,
    ec_m integer,
    etag text,
    CONSTRAINT objects_storage_class_check CHECK ((storage_class = ANY (ARRAY['STANDARD'::text, 'GLACIER'::text, 'DEEP_ARCHIVE'::text])))
);


--
-- Name: objects_id_seq; Type: SEQUENCE; Schema: -; Owner: -
--

CREATE SEQUENCE objects_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: objects_id_seq; Type: SEQUENCE OWNED BY; Schema: -; Owner: -
--

ALTER SEQUENCE objects_id_seq OWNED BY objects.id;


--
-- Name: retrieve_requests; Type: TABLE; Schema: -; Owner: -
--

CREATE TABLE retrieve_requests (
    id bigint NOT NULL,
    object_id bigint NOT NULL,
    state text DEFAULT 'pending'::text NOT NULL,
    pin_days integer DEFAULT 1 NOT NULL,
    expires_at timestamp with time zone,
    error text,
    requested_at timestamp with time zone DEFAULT now() NOT NULL,
    restored_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    pool text DEFAULT 'default'::text NOT NULL,
    assigned_at timestamp with time zone,
    CONSTRAINT retrieve_requests_pin_days_check CHECK (((pin_days >= 1) AND (pin_days <= 30))),
    CONSTRAINT retrieve_requests_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'waiting_for_media'::text, 'restoring'::text, 'restored'::text, 'expired'::text, 'failed'::text, 'cancelled'::text])))
);


--
-- Name: retrieve_requests_id_seq; Type: SEQUENCE; Schema: -; Owner: -
--

CREATE SEQUENCE retrieve_requests_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: retrieve_requests_id_seq; Type: SEQUENCE OWNED BY; Schema: -; Owner: -
--

ALTER SEQUENCE retrieve_requests_id_seq OWNED BY retrieve_requests.id;


--
-- Name: s3_keys; Type: TABLE; Schema: -; Owner: -
--

CREATE TABLE s3_keys (
    id bigint NOT NULL,
    user_id bigint NOT NULL,
    access_key text NOT NULL,
    secret_key text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: s3_keys_id_seq; Type: SEQUENCE; Schema: -; Owner: -
--

CREATE SEQUENCE s3_keys_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: s3_keys_id_seq; Type: SEQUENCE OWNED BY; Schema: -; Owner: -
--

ALTER SEQUENCE s3_keys_id_seq OWNED BY s3_keys.id;


--
-- Name: s3_users; Type: TABLE; Schema: -; Owner: -
--

CREATE TABLE s3_users (
    id bigint NOT NULL,
    user_id text NOT NULL,
    full_name text NOT NULL,
    email text,
    is_admin boolean DEFAULT false NOT NULL,
    suspended boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: s3_users_id_seq; Type: SEQUENCE; Schema: -; Owner: -
--

CREATE SEQUENCE s3_users_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: s3_users_id_seq; Type: SEQUENCE OWNED BY; Schema: -; Owner: -
--

ALTER SEQUENCE s3_users_id_seq OWNED BY s3_users.id;


--
-- Name: schema_migrations; Type: TABLE; Schema: -; Owner: -
--

CREATE TABLE schema_migrations (
    version integer NOT NULL,
    filename text NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: tape_files; Type: TABLE; Schema: -; Owner: -
--

CREATE TABLE tape_files (
    id bigint NOT NULL,
    tape_id bigint NOT NULL,
    object_id bigint NOT NULL,
    archive_id bigint NOT NULL,
    size_bytes bigint NOT NULL,
    written_at timestamp with time zone DEFAULT now() NOT NULL,
    physical_file bigint NOT NULL,
    part_number integer DEFAULT 1 NOT NULL,
    shard_number integer
);


--
-- Name: tape_files_id_seq; Type: SEQUENCE; Schema: -; Owner: -
--

CREATE SEQUENCE tape_files_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: tape_files_id_seq; Type: SEQUENCE OWNED BY; Schema: -; Owner: -
--

ALTER SEQUENCE tape_files_id_seq OWNED BY tape_files.id;


--
-- Name: tape_pools; Type: TABLE; Schema: -; Owner: -
--

CREATE TABLE tape_pools (
    id bigint NOT NULL,
    name text NOT NULL,
    description text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    ec_k integer,
    ec_m integer,
    CONSTRAINT tape_pools_ec_k_check CHECK (((ec_k IS NULL) OR (ec_k >= 1))),
    CONSTRAINT tape_pools_ec_m_check CHECK (((ec_m IS NULL) OR (ec_m >= 1)))
);


--
-- Name: tape_pools_id_seq; Type: SEQUENCE; Schema: -; Owner: -
--

CREATE SEQUENCE tape_pools_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: tape_pools_id_seq; Type: SEQUENCE OWNED BY; Schema: -; Owner: -
--

ALTER SEQUENCE tape_pools_id_seq OWNED BY tape_pools.id;


--
-- Name: tapes; Type: TABLE; Schema: -; Owner: -
--

CREATE TABLE tapes (
    id bigint NOT NULL,
    barcode text NOT NULL,
    pool_id bigint NOT NULL,
    state text DEFAULT 'active'::text NOT NULL,
    is_cleaning boolean DEFAULT false NOT NULL,
    capacity_bytes bigint NOT NULL,
    written_bytes bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    last_verified_at timestamp with time zone,
    CONSTRAINT tapes_state_check CHECK ((state = ANY (ARRAY['active'::text, 'full'::text, 'retired'::text, 'lost'::text])))
);


--
-- Name: tapes_id_seq; Type: SEQUENCE; Schema: -; Owner: -
--

CREATE SEQUENCE tapes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: tapes_id_seq; Type: SEQUENCE OWNED BY; Schema: -; Owner: -
--

ALTER SEQUENCE tapes_id_seq OWNED BY tapes.id;


--
-- Name: archive_requests id; Type: DEFAULT; Schema: -; Owner: -
--

ALTER TABLE ONLY archive_requests ALTER COLUMN id SET DEFAULT nextval('archive_requests_id_seq'::regclass);


--
-- Name: buckets id; Type: DEFAULT; Schema: -; Owner: -
--

ALTER TABLE ONLY buckets ALTER COLUMN id SET DEFAULT nextval('buckets_id_seq'::regclass);


--
-- Name: objects id; Type: DEFAULT; Schema: -; Owner: -
--

ALTER TABLE ONLY objects ALTER COLUMN id SET DEFAULT nextval('objects_id_seq'::regclass);


--
-- Name: retrieve_requests id; Type: DEFAULT; Schema: -; Owner: -
--

ALTER TABLE ONLY retrieve_requests ALTER COLUMN id SET DEFAULT nextval('retrieve_requests_id_seq'::regclass);


--
-- Name: s3_keys id; Type: DEFAULT; Schema: -; Owner: -
--

ALTER TABLE ONLY s3_keys ALTER COLUMN id SET DEFAULT nextval('s3_keys_id_seq'::regclass);


--
-- Name: s3_users id; Type: DEFAULT; Schema: -; Owner: -
--

ALTER TABLE ONLY s3_users ALTER COLUMN id SET DEFAULT nextval('s3_users_id_seq'::regclass);


--
-- Name: tape_files id; Type: DEFAULT; Schema: -; Owner: -
--

ALTER TABLE ONLY tape_files ALTER COLUMN id SET DEFAULT nextval('tape_files_id_seq'::regclass);


--
-- Name: tape_pools id; Type: DEFAULT; Schema: -; Owner: -
--

ALTER TABLE ONLY tape_pools ALTER COLUMN id SET DEFAULT nextval('tape_pools_id_seq'::regclass);


--
-- Name: tapes id; Type: DEFAULT; Schema: -; Owner: -
--

ALTER TABLE ONLY tapes ALTER COLUMN id SET DEFAULT nextval('tapes_id_seq'::regclass);


--
-- Name: archive_requests archive_requests_pkey; Type: CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY archive_requests
    ADD CONSTRAINT archive_requests_pkey PRIMARY KEY (id);


--
-- Name: buckets buckets_name_key; Type: CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY buckets
    ADD CONSTRAINT buckets_name_key UNIQUE (name);


--
-- Name: buckets buckets_pkey; Type: CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY buckets
    ADD CONSTRAINT buckets_pkey PRIMARY KEY (id);


--
-- Name: object_parts object_parts_pkey; Type: CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY object_parts
    ADD CONSTRAINT object_parts_pkey PRIMARY KEY (object_id, part_number);


--
-- Name: object_shards object_shards_pkey; Type: CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY object_shards
    ADD CONSTRAINT object_shards_pkey PRIMARY KEY (object_id, part_number, shard_number);


--
-- Name: objects objects_pkey; Type: CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY objects
    ADD CONSTRAINT objects_pkey PRIMARY KEY (id);


--
-- Name: retrieve_requests retrieve_requests_pkey; Type: CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY retrieve_requests
    ADD CONSTRAINT retrieve_requests_pkey PRIMARY KEY (id);


--
-- Name: s3_keys s3_keys_access_key_key; Type: CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY s3_keys
    ADD CONSTRAINT s3_keys_access_key_key UNIQUE (access_key);


--
-- Name: s3_keys s3_keys_pkey; Type: CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY s3_keys
    ADD CONSTRAINT s3_keys_pkey PRIMARY KEY (id);


--
-- Name: s3_users s3_users_pkey; Type: CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY s3_users
    ADD CONSTRAINT s3_users_pkey PRIMARY KEY (id);


--
-- Name: s3_users s3_users_user_id_key; Type: CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY s3_users
    ADD CONSTRAINT s3_users_user_id_key UNIQUE (user_id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: tape_files tape_files_object_part_shard_uniq; Type: CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY tape_files
    ADD CONSTRAINT tape_files_object_part_shard_uniq UNIQUE (object_id, part_number, shard_number);


--
-- Name: tape_files tape_files_pkey; Type: CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY tape_files
    ADD CONSTRAINT tape_files_pkey PRIMARY KEY (id);


--
-- Name: tape_files tape_files_tape_id_archive_id_key; Type: CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY tape_files
    ADD CONSTRAINT tape_files_tape_id_archive_id_key UNIQUE (tape_id, archive_id);


--
-- Name: tape_pools tape_pools_name_key; Type: CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY tape_pools
    ADD CONSTRAINT tape_pools_name_key UNIQUE (name);


--
-- Name: tape_pools tape_pools_pkey; Type: CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY tape_pools
    ADD CONSTRAINT tape_pools_pkey PRIMARY KEY (id);


--
-- Name: tapes tapes_barcode_key; Type: CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY tapes
    ADD CONSTRAINT tapes_barcode_key UNIQUE (barcode);


--
-- Name: tapes tapes_pkey; Type: CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY tapes
    ADD CONSTRAINT tapes_pkey PRIMARY KEY (id);


--
-- Name: archive_requests_object_idx; Type: INDEX; Schema: -; Owner: -
--

CREATE INDEX archive_requests_object_idx ON archive_requests USING btree (object_id);


--
-- Name: archive_requests_pool_idx; Type: INDEX; Schema: -; Owner: -
--

CREATE INDEX archive_requests_pool_idx ON archive_requests USING btree (pool, state);


--
-- Name: archive_requests_state_idx; Type: INDEX; Schema: -; Owner: -
--

CREATE INDEX archive_requests_state_idx ON archive_requests USING btree (state);


--
-- Name: objects_bucket_idx; Type: INDEX; Schema: -; Owner: -
--

CREATE INDEX objects_bucket_idx ON objects USING btree (bucket_id);


--
-- Name: objects_bucket_key_live_uniq; Type: INDEX; Schema: -; Owner: -
--

CREATE UNIQUE INDEX objects_bucket_key_live_uniq ON objects USING btree (bucket_id, key) WHERE (deleted_at IS NULL);


--
-- Name: objects_class_idx; Type: INDEX; Schema: -; Owner: -
--

CREATE INDEX objects_class_idx ON objects USING btree (storage_class);


--
-- Name: objects_upload_id_idx; Type: INDEX; Schema: -; Owner: -
--

CREATE UNIQUE INDEX objects_upload_id_idx ON objects USING btree (upload_id) WHERE (upload_id IS NOT NULL);


--
-- Name: retrieve_requests_expiry_idx; Type: INDEX; Schema: -; Owner: -
--

CREATE INDEX retrieve_requests_expiry_idx ON retrieve_requests USING btree (expires_at) WHERE (state = 'restored'::text);


--
-- Name: retrieve_requests_object_idx; Type: INDEX; Schema: -; Owner: -
--

CREATE INDEX retrieve_requests_object_idx ON retrieve_requests USING btree (object_id);


--
-- Name: retrieve_requests_pool_idx; Type: INDEX; Schema: -; Owner: -
--

CREATE INDEX retrieve_requests_pool_idx ON retrieve_requests USING btree (pool, state);


--
-- Name: retrieve_requests_state_idx; Type: INDEX; Schema: -; Owner: -
--

CREATE INDEX retrieve_requests_state_idx ON retrieve_requests USING btree (state);


--
-- Name: s3_keys_user_idx; Type: INDEX; Schema: -; Owner: -
--

CREATE INDEX s3_keys_user_idx ON s3_keys USING btree (user_id);


--
-- Name: tape_files_ec_idx; Type: INDEX; Schema: -; Owner: -
--

CREATE INDEX tape_files_ec_idx ON tape_files USING btree (object_id, part_number) WHERE (shard_number IS NOT NULL);


--
-- Name: tape_files_object_idx; Type: INDEX; Schema: -; Owner: -
--

CREATE INDEX tape_files_object_idx ON tape_files USING btree (object_id);


--
-- Name: tape_files_tape_idx; Type: INDEX; Schema: -; Owner: -
--

CREATE INDEX tape_files_tape_idx ON tape_files USING btree (tape_id);


--
-- Name: tapes_pool_idx; Type: INDEX; Schema: -; Owner: -
--

CREATE INDEX tapes_pool_idx ON tapes USING btree (pool_id);


--
-- Name: tapes_state_idx; Type: INDEX; Schema: -; Owner: -
--

CREATE INDEX tapes_state_idx ON tapes USING btree (state);


--
-- Name: archive_requests archive_requests_object_id_fkey; Type: FK CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY archive_requests
    ADD CONSTRAINT archive_requests_object_id_fkey FOREIGN KEY (object_id) REFERENCES objects(id);


--
-- Name: archive_requests archive_requests_tape_id_fkey; Type: FK CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY archive_requests
    ADD CONSTRAINT archive_requests_tape_id_fkey FOREIGN KEY (tape_id) REFERENCES tapes(id);


--
-- Name: buckets buckets_owner_id_fkey; Type: FK CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY buckets
    ADD CONSTRAINT buckets_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES s3_users(id) ON DELETE SET NULL;


--
-- Name: object_parts object_parts_object_id_fkey; Type: FK CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY object_parts
    ADD CONSTRAINT object_parts_object_id_fkey FOREIGN KEY (object_id) REFERENCES objects(id) ON DELETE CASCADE;


--
-- Name: object_shards object_shards_object_id_fkey; Type: FK CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY object_shards
    ADD CONSTRAINT object_shards_object_id_fkey FOREIGN KEY (object_id) REFERENCES objects(id) ON DELETE CASCADE;


--
-- Name: objects objects_bucket_id_fkey; Type: FK CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY objects
    ADD CONSTRAINT objects_bucket_id_fkey FOREIGN KEY (bucket_id) REFERENCES buckets(id);


--
-- Name: objects objects_owner_id_fkey; Type: FK CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY objects
    ADD CONSTRAINT objects_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES s3_users(id) ON DELETE SET NULL;


--
-- Name: retrieve_requests retrieve_requests_object_id_fkey; Type: FK CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY retrieve_requests
    ADD CONSTRAINT retrieve_requests_object_id_fkey FOREIGN KEY (object_id) REFERENCES objects(id);


--
-- Name: s3_keys s3_keys_user_id_fkey; Type: FK CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY s3_keys
    ADD CONSTRAINT s3_keys_user_id_fkey FOREIGN KEY (user_id) REFERENCES s3_users(id) ON DELETE CASCADE;


--
-- Name: tape_files tape_files_object_id_fkey; Type: FK CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY tape_files
    ADD CONSTRAINT tape_files_object_id_fkey FOREIGN KEY (object_id) REFERENCES objects(id);


--
-- Name: tape_files tape_files_tape_id_fkey; Type: FK CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY tape_files
    ADD CONSTRAINT tape_files_tape_id_fkey FOREIGN KEY (tape_id) REFERENCES tapes(id);


--
-- Name: tapes tapes_pool_id_fkey; Type: FK CONSTRAINT; Schema: -; Owner: -
--

ALTER TABLE ONLY tapes
    ADD CONSTRAINT tapes_pool_id_fkey FOREIGN KEY (pool_id) REFERENCES tape_pools(id);


--
-- PostgreSQL database dump complete
--


