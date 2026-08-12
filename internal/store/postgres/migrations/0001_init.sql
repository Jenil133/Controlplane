CREATE TABLE namespaces (
    name        TEXT        PRIMARY KEY,
    description TEXT        NOT NULL DEFAULT '',
    revision    BIGINT      NOT NULL DEFAULT 1,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE configs (
    namespace   TEXT        NOT NULL REFERENCES namespaces (name) ON DELETE CASCADE,
    key         TEXT        NOT NULL,
    value       JSONB       NOT NULL,
    description TEXT        NOT NULL DEFAULT '',
    revision    BIGINT      NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    updated_by  TEXT        NOT NULL,
    PRIMARY KEY (namespace, key)
);

CREATE TABLE flags (
    namespace   TEXT        NOT NULL REFERENCES namespaces (name) ON DELETE CASCADE,
    key         TEXT        NOT NULL,
    enabled     BOOLEAN     NOT NULL,
    description TEXT        NOT NULL DEFAULT '',
    revision    BIGINT      NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    updated_by  TEXT        NOT NULL,
    PRIMARY KEY (namespace, key)
);

CREATE TABLE experiments (
    namespace   TEXT        NOT NULL REFERENCES namespaces (name) ON DELETE CASCADE,
    key         TEXT        NOT NULL,
    enabled     BOOLEAN     NOT NULL,
    description TEXT        NOT NULL DEFAULT '',
    salt        TEXT        NOT NULL,
    variants    JSONB       NOT NULL,
    revision    BIGINT      NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    updated_by  TEXT        NOT NULL,
    PRIMARY KEY (namespace, key)
);
