CREATE TABLE IF NOT EXISTS expert_invites (
    code        VARCHAR(16) PRIMARY KEY,
    created_by  BIGINT       NOT NULL,
    created_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    used_by_tg  BIGINT       NULL,
    used_at     TIMESTAMP    NULL
);
