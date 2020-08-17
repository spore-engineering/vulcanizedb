-- +goose Up
CREATE TABLE public.storage_diff
(
    id             BIGSERIAL PRIMARY KEY,
    block_height   BIGINT,
    block_hash     BYTEA,
    hashed_address BYTEA,
    storage_key    BYTEA,
    storage_value  BYTEA,
    eth_node_id    INTEGER NOT NULL REFERENCES public.eth_nodes (id) ON DELETE CASCADE,
    checked        BOOLEAN   NOT NULL DEFAULT FALSE,
    from_backfill  BOOLEAN   NOT NULL DEFAULT FALSE,
    created        TIMESTAMP NOT NULL DEFAULT NOW(),
    updated        TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (block_height, block_hash, hashed_address, storage_key, storage_value)
);

-- +goose StatementBegin
CREATE FUNCTION set_storage_updated() RETURNS TRIGGER AS
$$
BEGIN
    NEW.updated = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER storage_updated
    BEFORE UPDATE
    ON public.storage_diff
    FOR EACH ROW
EXECUTE PROCEDURE set_storage_updated();

CREATE INDEX storage_diff_checked_index
    ON public.storage_diff (checked) WHERE checked = false;
CREATE INDEX storage_diff_eth_node
    ON public.storage_diff (eth_node_id);

-- +goose Down
DROP TRIGGER storage_updated ON public.storage_diff;
DROP FUNCTION set_storage_updated();

DROP TABLE public.storage_diff;
