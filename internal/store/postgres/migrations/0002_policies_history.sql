-- Phase 2 + 3: flag rollouts, traffic policies, revision history and the
-- audit log.
--
-- Phase 1 rows are upgraded in place: flags become fully rolled out with
-- their key as salt and no allowlist (exactly how Phase 1 evaluated them),
-- and namespaces get an unknown creator. History is not backfilled, so
-- revisions written before this migration have no history entry.

ALTER TABLE namespaces ADD COLUMN created_by TEXT NOT NULL DEFAULT '';

-- The defaults stay so that Phase 1 replicas still running during a rolling
-- upgrade can keep inserting flags; evaluation treats an empty salt as the key.
ALTER TABLE flags
    ADD COLUMN rollout_percent DOUBLE PRECISION NOT NULL DEFAULT 100,
    ADD COLUMN salt            TEXT             NOT NULL DEFAULT '',
    ADD COLUMN allowlist       TEXT[]           NOT NULL DEFAULT '{}',
    -- model.RolloutPlan as JSON; NULL until a rollout is started.
    ADD COLUMN rollout         JSONB;

UPDATE flags SET salt = key;

-- Every replica's rollout controller polls for active plans.
CREATE INDEX flags_active_rollouts ON flags (namespace, key) WHERE rollout ->> 'state' = 'active';

CREATE TABLE rate_limits (
    namespace           TEXT             NOT NULL REFERENCES namespaces (name) ON DELETE CASCADE,
    key                 TEXT             NOT NULL,
    enabled             BOOLEAN          NOT NULL,
    description         TEXT             NOT NULL DEFAULT '',
    requests_per_second DOUBLE PRECISION NOT NULL,
    burst               BIGINT           NOT NULL,
    revision            BIGINT           NOT NULL,
    updated_at          TIMESTAMPTZ      NOT NULL,
    updated_by          TEXT             NOT NULL,
    PRIMARY KEY (namespace, key)
);

-- Durations are nanoseconds, as in time.Duration.
CREATE TABLE circuit_breakers (
    namespace              TEXT             NOT NULL REFERENCES namespaces (name) ON DELETE CASCADE,
    key                    TEXT             NOT NULL,
    enabled                BOOLEAN          NOT NULL,
    description            TEXT             NOT NULL DEFAULT '',
    failure_rate_threshold DOUBLE PRECISION NOT NULL,
    min_requests           BIGINT           NOT NULL,
    window_ns              BIGINT           NOT NULL,
    open_duration_ns       BIGINT           NOT NULL,
    half_open_max_requests BIGINT           NOT NULL,
    revision               BIGINT           NOT NULL,
    updated_at             TIMESTAMPTZ      NOT NULL,
    updated_by             TEXT             NOT NULL,
    PRIMARY KEY (namespace, key)
);

-- One row per namespace revision with the complete namespace state
-- (model.Snapshot as JSON), for diffs and rollback.
CREATE TABLE revisions (
    namespace  TEXT        NOT NULL REFERENCES namespaces (name) ON DELETE CASCADE,
    revision   BIGINT      NOT NULL,
    actor      TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    summary    TEXT        NOT NULL,
    snapshot   JSONB       NOT NULL,
    PRIMARY KEY (namespace, revision)
);

-- One row per changed entity. There is deliberately no foreign key: the
-- audit trail must outlive the namespaces it describes.
CREATE TABLE audit_events (
    id           BIGSERIAL   PRIMARY KEY,
    namespace    TEXT        NOT NULL,
    revision     BIGINT      NOT NULL,
    actor        TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL,
    action       TEXT        NOT NULL,
    entity_type  TEXT        NOT NULL,
    entity_key   TEXT        NOT NULL,
    -- model.EncodeEntry of the entity; NULL where it did not exist.
    before_image JSONB,
    after_image  JSONB,
    message      TEXT        NOT NULL DEFAULT ''
);

-- Listings are newest first (id DESC) within the filters.
CREATE INDEX audit_events_namespace ON audit_events (namespace, id);
CREATE INDEX audit_events_entity ON audit_events (namespace, entity_type, entity_key, id);
CREATE INDEX audit_events_actor ON audit_events (actor, id);
CREATE INDEX audit_events_created_at ON audit_events (created_at);
