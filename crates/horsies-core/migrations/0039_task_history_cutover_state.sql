-- Schema v35: migration currency and cutover completion are separate state.
CREATE TABLE IF NOT EXISTS horsies_cutover_state (
    cutover_name text PRIMARY KEY,
    completed_at timestamptz NOT NULL DEFAULT NOW()
);
