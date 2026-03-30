-- =============================================================================
-- CTO-LeadEnrich PostgreSQL Schema
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. enrichment_jobs
-- (defined before enriched_leads because leads reference jobs)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enrichment_jobs (
    id                TEXT        PRIMARY KEY,
    snapshot_id       TEXT,
    total_urls        INTEGER     NOT NULL DEFAULT 0,
    processed         INTEGER     NOT NULL DEFAULT 0,
    failed            INTEGER     NOT NULL DEFAULT 0,
    status            TEXT        NOT NULL DEFAULT 'pending',   -- pending | processing | completed | failed
    error             TEXT,
    webhook_url       TEXT,
    organization_id   TEXT        NOT NULL DEFAULT 'default',
    created_at        TEXT,
    updated_at        TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_org
    ON enrichment_jobs (organization_id, created_at DESC);


-- -----------------------------------------------------------------------------
-- 2. enrichment_sub_jobs
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enrichment_sub_jobs (
    id                TEXT        PRIMARY KEY,
    job_id            TEXT        NOT NULL REFERENCES enrichment_jobs (id) ON DELETE CASCADE,
    chunk_index       INTEGER     NOT NULL,
    total_urls        INTEGER     NOT NULL DEFAULT 0,
    processed         INTEGER     NOT NULL DEFAULT 0,
    failed            INTEGER     NOT NULL DEFAULT 0,
    status            TEXT        NOT NULL DEFAULT 'pending',   -- pending | processing | completed
    organization_id   TEXT        NOT NULL DEFAULT 'default',
    created_at        TEXT,
    updated_at        TEXT
);

CREATE INDEX IF NOT EXISTS idx_sub_jobs_job
    ON enrichment_sub_jobs (job_id);


-- -----------------------------------------------------------------------------
-- 3. company_enrichments
-- (defined before enriched_leads because leads reference companies)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS company_enrichments (
    -- Identity (ID = SHA-256 of normalised LinkedIn URL)
    id                      TEXT    PRIMARY KEY,
    company_linkedin_url    TEXT    NOT NULL,
    org_id                  TEXT    NOT NULL DEFAULT 'default',

    -- Company identity
    name                    TEXT,
    domain                  TEXT,
    website                 TEXT,
    logo                    TEXT,
    description             TEXT,

    -- Company profile
    industry                TEXT,
    employee_count          INTEGER DEFAULT 0,
    hq_location             TEXT,
    founded_year            TEXT,

    -- Funding & investors (P4)
    funding_stage           TEXT,
    total_funding           TEXT,
    last_funding_date       TEXT,
    lead_investor           TEXT,

    -- Contact
    company_twitter         TEXT,
    company_email           TEXT,
    company_phone           TEXT,

    -- LinkedIn company posts (P2)
    linkedin_posts          TEXT    DEFAULT '[]',   -- JSON array
    post_themes             TEXT,

    -- Free signals layer (P4)
    crunchbase_data         TEXT    DEFAULT '{}',   -- JSON object
    news_mentions           TEXT    DEFAULT '[]',   -- JSON array
    wappalyzer_tech         TEXT    DEFAULT '[]',   -- JSON array

    -- AI analysis (P3)
    company_tags            TEXT    DEFAULT '[]',   -- JSON array
    culture_signals         TEXT    DEFAULT '{}',   -- JSON object
    account_pitch           TEXT    DEFAULT '{}',   -- JSON object

    -- Scoring (P5)
    company_score           INTEGER DEFAULT 0,
    company_score_tier      TEXT    DEFAULT 'C',

    -- Cache management
    last_enriched_at        TEXT,
    cache_expires_at        TEXT,                   -- 7-day TTL
    refresh_triggered_by    TEXT,

    -- Raw data
    raw_data                TEXT    DEFAULT '{}',

    created_at              TEXT,
    updated_at              TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_co_linkedin
    ON company_enrichments (company_linkedin_url, org_id);

CREATE INDEX IF NOT EXISTS idx_co_org
    ON company_enrichments (org_id, company_score DESC);

CREATE INDEX IF NOT EXISTS idx_co_expires
    ON company_enrichments (cache_expires_at);


-- -----------------------------------------------------------------------------
-- 4. enriched_leads  (main table)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enriched_leads (
    id                      TEXT        PRIMARY KEY,
    linkedin_url            TEXT        NOT NULL,

    -- Identity
    name                    TEXT,
    first_name              TEXT,
    last_name               TEXT,
    work_email              TEXT,
    personal_email          TEXT,
    direct_phone            TEXT,
    twitter                 TEXT,
    city                    TEXT,
    country                 TEXT,
    timezone                TEXT,

    -- Professional
    title                   TEXT,
    seniority_level         TEXT,
    department              TEXT,
    years_in_role           TEXT,
    company                 TEXT,
    previous_companies      TEXT,           -- JSON array
    top_skills              TEXT,           -- JSON array
    education               TEXT,
    certifications          TEXT,
    languages               TEXT,

    -- Company data
    company_website         TEXT,
    industry                TEXT,
    employee_count          INTEGER DEFAULT 0,
    hq_location             TEXT,
    founded_year            TEXT,
    funding_stage           TEXT,
    total_funding           TEXT,
    last_funding_date       TEXT,
    lead_investor           TEXT,
    annual_revenue          TEXT,
    tech_stack              TEXT,
    hiring_velocity         TEXT,

    -- Company enrichment (waterfall)
    avatar_url              TEXT,
    company_logo            TEXT,
    company_email           TEXT,
    company_description     TEXT,
    company_linkedin        TEXT,
    company_twitter         TEXT,
    company_phone           TEXT,
    waterfall_log           TEXT,

    -- Website intelligence (Stage 3)
    website_intelligence    TEXT,
    product_offerings       TEXT,           -- JSON array
    value_proposition       TEXT,
    target_customers        TEXT,           -- JSON array
    business_model          TEXT,
    pricing_signals         TEXT,
    product_category        TEXT,
    data_completeness_score INTEGER DEFAULT 0,

    -- Intent signals (Stage 6)
    recent_funding_event    TEXT,
    hiring_signal           TEXT,
    job_change              TEXT,
    linkedin_activity       TEXT,
    news_mention            TEXT,
    product_launch          TEXT,
    competitor_usage        TEXT,
    review_activity         TEXT,

    -- Scoring (Stage 7)
    icp_fit_score           INTEGER DEFAULT 0,
    intent_score            INTEGER DEFAULT 0,
    timing_score            INTEGER DEFAULT 0,
    engagement_score        INTEGER DEFAULT 0,
    total_score             INTEGER DEFAULT 0,
    score_tier              TEXT,
    score_explanation       TEXT,
    icp_match_tier          TEXT,
    disqualification_flags  TEXT,

    -- Outreach (Stage 8)
    email_subject           TEXT,
    cold_email              TEXT,
    linkedin_note           TEXT,
    best_channel            TEXT,
    best_send_time          TEXT,
    outreach_angle          TEXT,
    sequence_type           TEXT,
    outreach_sequence       TEXT,           -- JSON array
    last_contacted          TEXT,
    email_status            TEXT,

    -- CRM
    lead_source             TEXT    DEFAULT 'LinkedIn URL',
    enrichment_source       TEXT,
    data_completeness       INTEGER DEFAULT 0,
    crm_stage               TEXT,
    tags                    TEXT,           -- JSON array
    assigned_owner          TEXT,

    -- Raw / full data
    full_data               TEXT,
    raw_profile             TEXT,
    about                   TEXT,
    followers               INTEGER DEFAULT 0,
    connections             INTEGER DEFAULT 0,
    email_source            TEXT,
    email_confidence        TEXT,

    -- Status & tracking
    status                  TEXT    DEFAULT 'enriched',
    job_id                  TEXT    REFERENCES enrichment_jobs (id) ON DELETE SET NULL,
    organization_id         TEXT    NOT NULL DEFAULT 'default',
    enriched_at             TEXT,
    created_at              TEXT,

    -- AI analysis
    activity_feed           TEXT,           -- JSON array
    auto_tags               TEXT,           -- JSON array
    behavioural_signals     TEXT,           -- JSON object
    pitch_intelligence      TEXT,
    warm_signal             TEXT,

    -- Email waterfall (P4)
    email_verified          INTEGER DEFAULT 0,   -- 0 | 1
    bounce_risk             TEXT,

    -- Company enrichment link (P1)
    company_id              TEXT    REFERENCES company_enrichments (id) ON DELETE SET NULL,
    company_score           INTEGER DEFAULT 0,
    combined_score          INTEGER DEFAULT 0,
    company_tags            TEXT,           -- JSON array
    culture_signals         TEXT,           -- JSON object
    account_pitch           TEXT,
    wappalyzer_tech         TEXT,           -- JSON array
    news_mentions           TEXT,           -- JSON array
    crunchbase_data         TEXT,           -- JSON object
    linkedin_posts          TEXT,           -- JSON array
    company_score_tier      TEXT,

    -- LIO results
    lio_results_json        TEXT
);

CREATE INDEX IF NOT EXISTS idx_leads_org
    ON enriched_leads (organization_id, total_score DESC);

CREATE INDEX IF NOT EXISTS idx_leads_company
    ON enriched_leads (company_id);

CREATE INDEX IF NOT EXISTS idx_leads_job
    ON enriched_leads (job_id);

CREATE INDEX IF NOT EXISTS idx_leads_linkedin
    ON enriched_leads (linkedin_url);


-- -----------------------------------------------------------------------------
-- 5. workspace_configs
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS workspace_configs (
    org_id                  TEXT    PRIMARY KEY,

    -- Product
    product_name            TEXT,
    value_proposition       TEXT,

    -- ICP config
    target_titles           TEXT    DEFAULT '[]',   -- JSON array
    target_industries       TEXT    DEFAULT '[]',   -- JSON array
    company_size_min        INTEGER DEFAULT 0,
    company_size_max        INTEGER DEFAULT 0,

    -- Outreach style
    tone                    TEXT    DEFAULT 'peer',         -- peer | formal | casual
    cta_style               TEXT    DEFAULT 'question',     -- question | demo | resource | short_ask
    banned_phrases          TEXT    DEFAULT '[]',           -- JSON array
    case_studies            TEXT    DEFAULT '[]',           -- JSON array

    -- AI prompts
    lio_system_prompt       TEXT,
    lio_model               TEXT,
    lio_prompts_json        TEXT,
    workspace_context_json  TEXT,
    ai_prompts_json         TEXT,

    created_at              TEXT,
    updated_at              TEXT
);


-- -----------------------------------------------------------------------------
-- 6. enrichment_tool_configs
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enrichment_tool_configs (
    id          TEXT    PRIMARY KEY,
    org_id      TEXT    NOT NULL,
    tool_name   TEXT    NOT NULL,   -- brightdata | hunter | apollo | groq | wb_llm | ably
    is_enabled  INTEGER NOT NULL DEFAULT 1,   -- 0 | 1
    api_key     TEXT,
    extra_config TEXT,              -- JSON for tool-specific settings
    created_at  TEXT,
    updated_at  TEXT,

    UNIQUE (org_id, tool_name)
);

CREATE INDEX IF NOT EXISTS idx_tool_cfg_org
    ON enrichment_tool_configs (org_id);


-- -----------------------------------------------------------------------------
-- 7. enrichment_tool_credits
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enrichment_tool_credits (
    id              TEXT    PRIMARY KEY,
    org_id          TEXT    NOT NULL,
    tool_name       TEXT    NOT NULL,
    total_credits   INTEGER NOT NULL DEFAULT 0,
    used_credits    INTEGER NOT NULL DEFAULT 0,
    reset_at        TEXT,
    created_at      TEXT,
    updated_at      TEXT,

    UNIQUE (org_id, tool_name)
);

CREATE INDEX IF NOT EXISTS idx_credits_org
    ON enrichment_tool_credits (org_id);


-- -----------------------------------------------------------------------------
-- 8. enrichment_credit_usage_log
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enrichment_credit_usage_log (
    id                TEXT    PRIMARY KEY,
    org_id            TEXT    NOT NULL,
    tool_name         TEXT    NOT NULL,
    credits_deducted  INTEGER NOT NULL,
    lead_id           TEXT,
    job_id            TEXT,
    reason            TEXT,
    created_at        TEXT
);

CREATE INDEX IF NOT EXISTS idx_usage_log_org
    ON enrichment_credit_usage_log (org_id, tool_name);


-- -----------------------------------------------------------------------------
-- 9. processed_snapshots  (webhook idempotency)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS processed_snapshots (
    snapshot_id   TEXT    PRIMARY KEY,
    job_id        TEXT    REFERENCES enrichment_jobs (id) ON DELETE SET NULL,
    org_id        TEXT,
    processed_at  TEXT
);


-- -----------------------------------------------------------------------------
-- 10. enrichment_audit_log
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enrichment_audit_log (
    id            TEXT    PRIMARY KEY,
    org_id        TEXT    NOT NULL,
    action        TEXT    NOT NULL,   -- enrich | export | delete | …
    lead_id       TEXT,
    linkedin_url  TEXT,
    job_id        TEXT,
    user_email    TEXT,
    meta          TEXT,               -- JSON
    created_at    TEXT
);

CREATE INDEX IF NOT EXISTS idx_audit_org
    ON enrichment_audit_log (org_id, created_at DESC);


-- -----------------------------------------------------------------------------
-- 11. lead_notes
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lead_notes (
    id          TEXT    PRIMARY KEY,
    lead_id     TEXT    NOT NULL REFERENCES enriched_leads (id) ON DELETE CASCADE,
    org_id      TEXT    NOT NULL,
    note        TEXT    NOT NULL,
    created_at  TEXT
);

CREATE INDEX IF NOT EXISTS idx_notes_lead
    ON lead_notes (lead_id);
