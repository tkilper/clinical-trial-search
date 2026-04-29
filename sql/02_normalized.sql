CREATE TABLE IF NOT EXISTS trials (
    id             SERIAL       PRIMARY KEY,
    nct_id         VARCHAR(20)  UNIQUE NOT NULL,
    title          TEXT,
    brief_summary  TEXT,
    overall_status VARCHAR(100),
    phase          VARCHAR(50),
    start_date     VARCHAR(20),
    sponsor        TEXT,
    ingested_at    TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS conditions (
    id             SERIAL   PRIMARY KEY,
    trial_id       INTEGER  NOT NULL REFERENCES trials(id) ON DELETE CASCADE,
    condition_name TEXT     NOT NULL
);

CREATE TABLE IF NOT EXISTS interventions (
    id          SERIAL      PRIMARY KEY,
    trial_id    INTEGER     NOT NULL REFERENCES trials(id) ON DELETE CASCADE,
    name        TEXT,
    type        VARCHAR(50),
    description TEXT
);

CREATE TABLE IF NOT EXISTS eligibility (
    id                 SERIAL   PRIMARY KEY,
    trial_id           INTEGER  NOT NULL REFERENCES trials(id) ON DELETE CASCADE,
    criteria_text      TEXT,
    min_age            VARCHAR(30),
    max_age            VARCHAR(30),
    gender             VARCHAR(20),
    healthy_volunteers BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_trials_status      ON trials(overall_status);
CREATE INDEX IF NOT EXISTS idx_trials_phase       ON trials(phase);
CREATE INDEX IF NOT EXISTS idx_conditions_trial   ON conditions(trial_id);
CREATE INDEX IF NOT EXISTS idx_interventions_trial ON interventions(trial_id);
