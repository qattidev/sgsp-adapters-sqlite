-- +goose Up
CREATE TABLE sgsp_group_assignments (
    app_id text NOT NULL,
    group_key text NOT NULL,
    app_version text NOT NULL,
    owner_id text NOT NULL,
    incarnation blob NOT NULL CHECK (length(incarnation) = 16),
    endpoint_address text NOT NULL,
    endpoint_server_name text NOT NULL,
    closed boolean NOT NULL DEFAULT false,
    created_at text NOT NULL DEFAULT CURRENT_TIMESTAMP,
    closed_at text,
    PRIMARY KEY (app_id, group_key),
    CHECK (closed = (closed_at IS NOT NULL))
);

-- +goose Down
DROP TABLE sgsp_group_assignments;
