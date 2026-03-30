-- =============================================================================
-- Migration 001 — Normalized Lead Enrichment Schema
-- DB: lead_enrichment  |  Host: 172.105.58.70
--
-- Splits the monolithic enriched_leads (75+ cols) into 7 focused tables.
-- Each table has ≤20 columns, a single clear purpose, and targeted indexes.
--
-- Table map:
--   leads               → core identity (name, email, phone, location)
--   lead_professional   → career & company basics
--   lead_company_intel  → company enrichment + website intelligence (JSONB)
--   lead_signals        → intent signals
--   lead_scores         → scoring (ICP / intent / timing)
--   lead_outreach       → AI-generated outreach copy
--   lead_crm            → CRM state, tags, audit meta
--
-- Existing tables kept as-is (no migration risk):
--   enrichment_jobs, enrichment_sub_jobs, import_unmapped_data,
--   enrichment_audit_log, lead_notes, processed_snapshots
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. LEADS — core identity
--    Primary lookup table. Everything else references lead_id = leads.id.
--    PK = MD5(normalized linkedin_url), same as current enriched_leads.id
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS leads (
    id               TEXT        PRIMARY KEY,               -- MD5(linkedin_url)
    organization_id  TEXT        NOT NULL DEFAULT 'default',
    linkedin_url     TEXT        NOT NULL,
    name             TEXT,
    first_name       TEXT,
    last_name        TEXT,
    work_email       TEXT,
    personal_email   TEXT,
    direct_phone     TEXT,
    twitter          TEXT,
    avatar_url       TEXT,
    city             TEXT,
    country          TEXT,
    timezone         TEXT,
    status           TEXT        NOT NULL DEFAULT 'enriched',
    lead_source      TEXT        NOT NULL DEFAULT 'LinkedIn URL',
    enriched_at      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- 19 columns

-- Fast list queries by org (most common: "show me all leads for org X sorted by date")
CREATE INDEX IF NOT EXISTS idx_leads_org_date
    ON leads (organization_id, enriched_at DESC);

-- Filter/search by email
CREATE INDEX IF NOT EXISTS idx_leads_org_email
    ON leads (organization_id, work_email)
    WHERE work_email IS NOT NULL;

-- LinkedIn URL dedup / lookup
CREATE UNIQUE INDEX IF NOT EXISTS idx_leads_linkedin
    ON leads (organization_id, linkedin_url);


-- ---------------------------------------------------------------------------
-- 2. LEAD_PROFESSIONAL — career & company basics
--    One row per lead. Separated from identity to keep leads table thin.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lead_professional (
    lead_id          TEXT        PRIMARY KEY REFERENCES leads(id) ON DELETE CASCADE,
    title            TEXT,
    seniority_level  TEXT,
    department       TEXT,
    years_in_role    TEXT,
    about            TEXT,
    company          TEXT,
    company_website  TEXT,
    industry         TEXT,
    employee_count   INTEGER,
    hq_location      TEXT,
    founded_year     SMALLINT,
    funding_stage    TEXT,
    total_funding    TEXT,
    last_funding_date TEXT,
    lead_investor    TEXT,
    annual_revenue   TEXT,
    skills           JSONB,       -- ["Python","SQL",...]
    education        JSONB        -- [{"school":"MIT","degree":"BS CS","year":2015},...]
);
-- 19 columns

CREATE INDEX IF NOT EXISTS idx_prof_company
    ON lead_professional (company);

CREATE INDEX IF NOT EXISTS idx_prof_industry
    ON lead_professional (industry);

CREATE INDEX IF NOT EXISTS idx_prof_seniority
    ON lead_professional (seniority_level);


-- ---------------------------------------------------------------------------
-- 3. LEAD_COMPANY_INTEL — company enrichment + website intelligence
--    Heavier JSONB blobs kept here so joins only happen when needed.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lead_company_intel (
    lead_id              TEXT    PRIMARY KEY REFERENCES leads(id) ON DELETE CASCADE,
    company_logo         TEXT,
    company_email        TEXT,
    company_description  TEXT,
    company_linkedin     TEXT,
    company_twitter      TEXT,
    company_phone        TEXT,
    hiring_velocity      TEXT,
    tech_stack           JSONB,  -- ["React","AWS","Stripe",...]
    wappalyzer_tech      JSONB,  -- raw wappalyzer output
    website_intelligence JSONB,  -- full scraped homepage/about intelligence blob
    product_offerings    TEXT,
    value_proposition    TEXT,
    target_customers     TEXT,
    business_model       TEXT,
    pricing_signals      TEXT,
    product_category     TEXT,
    waterfall_log        TEXT,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- 19 columns

-- GIN index on JSONB for tech_stack queries ("find all leads using Salesforce")
CREATE INDEX IF NOT EXISTS idx_intel_tech_stack
    ON lead_company_intel USING GIN (tech_stack);


-- ---------------------------------------------------------------------------
-- 4. LEAD_SIGNALS — intent signals
--    Updated frequently (async enrichment). Narrow table = fast writes.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lead_signals (
    lead_id              TEXT        PRIMARY KEY REFERENCES leads(id) ON DELETE CASCADE,
    recent_funding_event TEXT,
    hiring_signal        TEXT,
    job_change           TEXT,
    linkedin_activity    TEXT,
    news_mention         TEXT,
    product_launch       TEXT,
    competitor_usage     TEXT,
    review_activity      TEXT,
    warm_signal          TEXT,
    behavioural_signals  JSONB,   -- structured signal objects
    activity_feed        JSONB,   -- timeline of events
    signal_updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- 13 columns

-- "Which leads had a funding event recently?" — filtered across org
CREATE INDEX IF NOT EXISTS idx_signals_funding
    ON lead_signals (recent_funding_event)
    WHERE recent_funding_event IS NOT NULL;


-- ---------------------------------------------------------------------------
-- 5. LEAD_SCORES — scoring (ICP / intent / timing / data completeness)
--    Queried heavily for dashboards and tier-filtered lists.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lead_scores (
    lead_id                 TEXT        PRIMARY KEY REFERENCES leads(id) ON DELETE CASCADE,
    icp_fit_score           SMALLINT    NOT NULL DEFAULT 0,  -- 0–40
    intent_score            SMALLINT    NOT NULL DEFAULT 0,  -- 0–30
    timing_score            SMALLINT    NOT NULL DEFAULT 0,  -- 0–20
    engagement_score        SMALLINT    NOT NULL DEFAULT 0,  -- 0–10
    total_score             SMALLINT    NOT NULL DEFAULT 0,  -- 0–100
    score_tier              TEXT,                            -- hot/warm/cool/cold
    icp_match_tier          TEXT,
    data_completeness_score SMALLINT    NOT NULL DEFAULT 0,
    disqualification_flags  JSONB,
    score_explanation       TEXT,
    scored_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- 12 columns

-- Dashboard: "show leads by score tier for org X"
-- Join with leads on lead_id to filter by org, then use this index
CREATE INDEX IF NOT EXISTS idx_scores_total
    ON lead_scores (total_score DESC);

CREATE INDEX IF NOT EXISTS idx_scores_tier
    ON lead_scores (score_tier);


-- ---------------------------------------------------------------------------
-- 6. LEAD_OUTREACH — AI-generated outreach copy
--    Large text blobs isolated here so SELECT * on leads stays fast.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lead_outreach (
    lead_id           TEXT        PRIMARY KEY REFERENCES leads(id) ON DELETE CASCADE,
    email_subject     TEXT,
    cold_email        TEXT,
    linkedin_note     TEXT,
    best_channel      TEXT,
    best_send_time    TEXT,
    outreach_angle    TEXT,
    sequence_type     TEXT,
    outreach_sequence TEXT,
    pitch_intelligence JSONB,
    account_pitch     TEXT,
    last_contacted    TIMESTAMPTZ,
    email_status      TEXT,
    generated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- 14 columns


-- ---------------------------------------------------------------------------
-- 7. LEAD_CRM — CRM state, tags, enrichment meta
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lead_crm (
    lead_id           TEXT        PRIMARY KEY REFERENCES leads(id) ON DELETE CASCADE,
    crm_stage         TEXT,
    tags              TEXT[],     -- user-defined labels
    auto_tags         TEXT[],     -- AI-generated labels
    company_tags      TEXT[],
    assigned_owner    TEXT,
    enrichment_source TEXT,
    email_source      TEXT,
    email_confidence  TEXT,
    email_verified    BOOLEAN     NOT NULL DEFAULT FALSE,
    bounce_risk       TEXT,
    connections       INTEGER     NOT NULL DEFAULT 0,
    followers         INTEGER     NOT NULL DEFAULT 0,
    job_id            TEXT,
    lio_results_json  JSONB,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- 16 columns

-- Filter: leads assigned to a specific owner
CREATE INDEX IF NOT EXISTS idx_crm_owner
    ON lead_crm (assigned_owner)
    WHERE assigned_owner IS NOT NULL;

-- Tag search: "find all leads tagged 'ICP'"
CREATE INDEX IF NOT EXISTS idx_crm_tags
    ON lead_crm USING GIN (tags);

CREATE INDEX IF NOT EXISTS idx_crm_auto_tags
    ON lead_crm USING GIN (auto_tags);

-- job_id lookup (trace which import/enrichment job created this lead)
CREATE INDEX IF NOT EXISTS idx_crm_job
    ON lead_crm (job_id)
    WHERE job_id IS NOT NULL;


-- =============================================================================
-- SUPPORTING TABLES
-- =============================================================================

-- ---------------------------------------------------------------------------
-- enrichment_jobs — one row per bulk enrichment or import job
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enrichment_jobs (
    id               TEXT        PRIMARY KEY,
    organization_id  TEXT        NOT NULL DEFAULT 'default',
    status           TEXT        NOT NULL DEFAULT 'pending',  -- pending/running/completed/failed
    filename         TEXT,
    file_hash        TEXT,
    folder_id        TEXT,
    snapshot_id      TEXT,
    webhook_url      TEXT,
    total_urls       INTEGER     NOT NULL DEFAULT 0,
    processed        INTEGER     NOT NULL DEFAULT 0,
    failed           INTEGER     NOT NULL DEFAULT 0,
    skipped          INTEGER     NOT NULL DEFAULT 0,
    new_count        INTEGER     NOT NULL DEFAULT 0,
    updated_count    INTEGER     NOT NULL DEFAULT 0,
    error            TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- 17 columns

CREATE INDEX IF NOT EXISTS idx_jobs_org_status
    ON enrichment_jobs (organization_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_jobs_folder
    ON enrichment_jobs (folder_id)
    WHERE folder_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_filename
    ON enrichment_jobs (organization_id, filename)
    WHERE filename IS NOT NULL;


-- ---------------------------------------------------------------------------
-- enrichment_sub_jobs — chunk-level tracking for large bulk jobs
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enrichment_sub_jobs (
    id               TEXT        PRIMARY KEY,
    job_id           TEXT        NOT NULL REFERENCES enrichment_jobs(id) ON DELETE CASCADE,
    organization_id  TEXT        NOT NULL DEFAULT 'default',
    chunk_index      INTEGER     NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'pending',
    total_urls       INTEGER     NOT NULL DEFAULT 0,
    processed        INTEGER     NOT NULL DEFAULT 0,
    failed           INTEGER     NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- 10 columns

CREATE INDEX IF NOT EXISTS idx_sub_jobs_job
    ON enrichment_sub_jobs (job_id);


-- ---------------------------------------------------------------------------
-- system_prompts — default AI prompts; admin can override per org via LIO config
--   org 'default' = system-wide defaults
--   org '<org_id>' = admin override for that org
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS system_prompts (
    id         TEXT    PRIMARY KEY,
    org_id     TEXT    NOT NULL DEFAULT 'default',
    name       TEXT    NOT NULL,
    key        TEXT    NOT NULL,   -- e.g. 'cold_email', 'linkedin_note', 'icp_scoring'
    content    TEXT    NOT NULL,
    is_active  BOOLEAN NOT NULL DEFAULT TRUE,
    priority   INTEGER NOT NULL DEFAULT 100,
    created_at TEXT    NOT NULL DEFAULT '',
    updated_at TEXT    NOT NULL DEFAULT ''
);
-- 9 columns

CREATE INDEX IF NOT EXISTS idx_system_prompts_org
    ON system_prompts (org_id);


-- ---------------------------------------------------------------------------
-- processed_snapshots — webhook idempotency guard
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS processed_snapshots (
    snapshot_id  TEXT        PRIMARY KEY,
    job_id       TEXT,
    org_id       TEXT,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- 4 columns


-- ---------------------------------------------------------------------------
-- enrichment_audit_log — who triggered what action
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enrichment_audit_log (
    id           TEXT        PRIMARY KEY,
    org_id       TEXT        NOT NULL,
    action       TEXT        NOT NULL,
    lead_id      TEXT,
    linkedin_url TEXT,
    job_id       TEXT,
    user_email   TEXT,
    meta         JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- 9 columns

CREATE INDEX IF NOT EXISTS idx_audit_org
    ON enrichment_audit_log (org_id, created_at DESC);


-- ---------------------------------------------------------------------------
-- lead_notes — manual annotations on a lead
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lead_notes (
    id         TEXT        PRIMARY KEY,
    lead_id    TEXT        NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    org_id     TEXT        NOT NULL,
    note       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- 5 columns

CREATE INDEX IF NOT EXISTS idx_notes_lead
    ON lead_notes (lead_id);


COMMIT;

-- =============================================================================
-- VERIFICATION (run manually after migration)
-- =============================================================================
-- SELECT relname, n_live_tup FROM pg_stat_user_tables WHERE relname LIKE 'lead%' ORDER BY relname;
-- SELECT COUNT(*) FROM leads;
-- SELECT COUNT(*) FROM lead_professional;
-- SELECT COUNT(*) FROM lead_scores;
