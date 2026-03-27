# model_config.py — Auto model selection per feature task type
# Maps every feature_code → task type → best model per provider

# ── Task Types ────────────────────────────────────────────────────────────────
# EXTRACT      : Structured JSON extraction from documents (OCR, ATS, contracts, invoices)
# EXTRACT_VIS  : Same but from IMAGE files — needs vision model
# SCORE        : Numerical scoring with reasoning breakdown (lead score, ICP, health, risk)
# GENERATE     : Creative text generation (copywriting, emails, subject lines, briefs)
# ANALYZE      : Data analysis + structured reporting (forecast, win/loss, attribution)
# PLAN         : Strategic planning (project plans, templates, workflows, retros)
# NLU          : Natural language understanding / command parsing / classification
# AUTOMATE     : Multi-step pipeline simulation (lead pipeline, task creation, triggers)

TASK_TYPE_MAP: dict[str, str] = {
    # ── TARO (C1–C19) ─────────────────────────────────────────────────────────
    "C1":  "PLAN",     # Autonomous Project Planner
    "C2":  "SCORE",    # Project Health Score
    "C3":  "PLAN",     # Butterfly Effect Simulator
    "C4":  "GENERATE", # AI Standup Bot
    "C5":  "ANALYZE",  # Workload Balancer
    "C6":  "NLU",      # NL Project Control
    "C7":  "ANALYZE",  # Sprint Velocity Predictor
    "C8":  "NLU",      # NL Task Creator
    "C9":  "GENERATE", # Daily Briefing
    "C10": "ANALYZE",  # Overdue Detector
    "C11": "SCORE",    # Task Complexity Estimator
    "C12": "NLU",      # Duplicate Task Detector
    "C13": "SCORE",    # Deadline Risk Predictor
    "C14": "EXTRACT",  # Meeting Notes Extractor
    "C15": "ANALYZE",  # Scope Creep Detector
    "C16": "GENERATE", # Auto Retrospective
    "C17": "ANALYZE",  # Context-Switch Tracker
    "C18": "ANALYZE",  # Dependency Bottleneck Analyzer
    "C19": "PLAN",     # Smart Template Generator

    # ── LIO (C20–C32) ─────────────────────────────────────────────────────────
    "C20": "ANALYZE",  # ICP Engine
    "C21": "ANALYZE",  # Conversation Intelligence
    "C22": "ANALYZE",  # Revenue Forecasting
    "C23": "ANALYZE",  # Deal DNA Matching
    "C24": "GENERATE", # AE Account Brief
    "C25": "ANALYZE",  # Multi-Touch Attribution
    "C26": "SCORE",    # Lead Score Explainer
    "C27": "GENERATE", # Outreach Draft Generator
    "C28": "GENERATE", # Cold Lead Re-engagement
    "C29": "SCORE",    # Intent Scoring Engine
    "C30": "ANALYZE",  # Relationship Map
    "C31": "ANALYZE",  # Win/Loss Analysis
    "C32": "ANALYZE",  # Deal Threads Timeline

    # ── EVOX (C33–C44) ────────────────────────────────────────────────────────
    "C33": "PLAN",     # Persona Journey Builder
    "C34": "ANALYZE",  # Campaign Autopsy
    "C35": "GENERATE", # Ghost Writer
    "C36": "GENERATE", # Subject Line Generator
    "C37": "PLAN",     # A/B Test Generator
    "C38": "ANALYZE",  # Audience Segmenter
    "C39": "GENERATE", # Content Personalizer
    "C40": "ANALYZE",  # Send Time Optimizer
    "C41": "SCORE",    # Landing Page Scorer
    "C42": "GENERATE", # Social Copy Generator
    "C43": "PLAN",     # Campaign Brief Builder
    "C44": "ANALYZE",  # Competitor Analysis

    # ── INZO (C45–C55) ────────────────────────────────────────────────────────
    "C45": "EXTRACT",  # Invoice Data Extractor
    "C46": "SCORE",    # Payment Predictor
    "C47": "EXTRACT",  # Expense Categorizer
    "C48": "ANALYZE",  # Cashflow Forecaster
    "C49": "ANALYZE",  # Tax Compliance Checker
    "C50": "SCORE",    # Vendor Performance Scorer
    "C51": "ANALYZE",  # Budget vs Actual Analyzer
    "C52": "EXTRACT",  # PO Matching Engine
    "C53": "NLU",      # Duplicate Invoice Detector
    "C54": "EXTRACT",  # GST/VAT Calculator
    "C55": "SCORE",    # Financial Health Score

    # ── MONI (C56–C59) ────────────────────────────────────────────────────────
    "C56": "ANALYZE",  # Spend Anomaly Detector
    "C57": "SCORE",    # ROI Calculator
    "C58": "ANALYZE",  # Cost Reduction Advisor
    "C59": "GENERATE", # Financial Report Generator

    # ── SIGI (C60–C62, C77) ───────────────────────────────────────────────────
    "C60": "ANALYZE",  # Contract Risk Analyzer
    "C61": "EXTRACT",  # Clause Extractor
    "C62": "GENERATE", # Contract Summary Generator
    "C77": "ANALYZE",  # Contract Template Optimizer

    # ── ZENI (C63–C65) ────────────────────────────────────────────────────────
    "C63": "NLU",      # Ticket Classifier
    "C64": "GENERATE", # Resolution Recommender
    "C65": "SCORE",    # CSAT Predictor

    # ── PRAX (C66–C68) ────────────────────────────────────────────────────────
    "C66": "PLAN",     # Workflow Mapper
    "C67": "PLAN",     # Approval Chain Builder
    "C68": "SCORE",    # SLA Breach Predictor

    # ── CROSS (C69–C71) ───────────────────────────────────────────────────────
    "C69": "NLU",      # Cross-Module Search
    "C70": "NLU",      # Entity Linker
    "C71": "GENERATE", # AI Executive Summary

    # ── ATS (C72) ─────────────────────────────────────────────────────────────
    "C72": "EXTRACT",  # CV / Resume Parser

    # ── TARO — New Features (C79–C80) ───────────────────────────────────────
    "C79": "NLU",      # Smart Notifications AI
    "C80": "ANALYZE",  # AI Project Health Assessment (unified)

    # ── TARO — Agile Sprint Planner ──────────────────────────────────────────
    "P3-T3": "PLAN",   # Agile Sprint Planner

    # ── LIO — New Features (C78) ─────────────────────────────────────────────
    "C78": "SCORE",    # Timing Score

    # ── EVOX — Spam Analyzer + New Features (C73–C76) ────────────────────────
    "C73": "SCORE",    # Pre-Send Spam Analyzer
    "C74": "ANALYZE",  # Full-Funnel Campaign Attribution Analyzer
    "C75": "ANALYZE",  # Email Objection Predictor
    "C76": "GENERATE", # Outreach Angle Recommender

    # ── Special endpoints ─────────────────────────────────────────────────────
    "OCR": "EXTRACT",
    "ATS": "EXTRACT",

    # ── Automation P1 (15 features) ───────────────────────────────────────────
    "P1-L1": "AUTOMATE", # New Lead Processing Pipeline
    "P1-L2": "SCORE",    # AI Lead Scoring
    "P1-L3": "SCORE",    # ICP Fit Score
    "P1-L4": "GENERATE", # AI Prospect Brief
    "P1-L5": "AUTOMATE", # Smart Lead Assignment
    "P1-L6": "ANALYZE",  # Cold Lead Detection
    "P1-T1": "AUTOMATE", # Auto Task Creation from Events
    "P1-T2": "SCORE",    # Smart Priority Rebalancer
    "P1-T3": "AUTOMATE", # Deadline Auto-Adjust
    "P1-S1": "ANALYZE",  # Contract Expiry Alert
    "P1-S2": "EXTRACT",  # Auto Contract Summarizer
    "P1-S3": "AUTOMATE", # Deal-Ready Contract Generator (LIO signal)
    "P1-S4": "AUTOMATE", # Signing Reminder Pipeline (Day 3, Day 7, escalation)
    "P1-E1": "ANALYZE",  # Email Campaign Optimizer
    "P1-E2": "AUTOMATE", # Campaign Auto-Trigger
    "P1-E3": "SCORE",    # Pre-Send Spam Analyzer
    "P1-E4": "AUTOMATE", # Auto A/B Champion — unified test + auto-promote pipeline
    "P1-I1": "EXTRACT",  # Invoice Auto-Categorizer
    "P1-I2": "GENERATE", # Payment Reminder Generator

    # ── Automation P2 (13 features) ───────────────────────────────────────────
    "P2-L1": "ANALYZE",  # Lead Nurture Sequence Builder
    "P2-L2": "GENERATE", # Objection Handler
    "P2-L3": "SCORE",    # Churn Risk Score
    "P2-L4": "ANALYZE",  # Competitive Intelligence
    "P2-L5": "GENERATE", # Follow-up Email Generator
    "P2-L6": "AUTOMATE", # Buying Signal Upgrade Pipeline
    "P2-T1": "ANALYZE",  # Sprint Capacity Planner
    "P2-T2": "SCORE",    # Burnout Risk Detector
    "P2-T3": "PLAN",     # Auto-Resource Allocator
    "P2-T4": "AUTOMATE", # Manager Proactive Alerts
    "P2-T5": "AUTOMATE", # Auto-Rebalance Pipeline
    "P2-T6": "AUTOMATE", # Budget Overrun Detection Pipeline
    "P2-S1": "ANALYZE",  # Contract Clause Risk Scorer
    "P2-S2": "ANALYZE",  # Risk Detection in Contracts
    "P2-S3": "PLAN",     # Multi-Party Signing Orchestrator
    "P2-S4": "ANALYZE",  # Missing Clause Detector (vs standard templates)
    "P2-E1": "GENERATE", # Campaign Copy Generator
    "P2-E2": "ANALYZE",  # Engagement Drop Detector
    "P2-I1": "ANALYZE",  # Aged Invoice Analyzer
    "P2-I2": "SCORE",    # Credit Risk Scorer
    "P2-I3": "ANALYZE",  # Overdue Escalation Intelligence

    # ── Automation P3 (9 features) ────────────────────────────────────────────
    "P3-L1": "SCORE",    # Predictive Deal Score
    "P3-L2": "ANALYZE",  # Market Expansion Advisor
    "P3-T1": "PLAN",     # AI Roadmap Generator
    "P3-T2": "ANALYZE",  # Talent Gap Analyzer
    "P3-S1": "ANALYZE",  # Legal Risk Monitor
    "P3-E1": "GENERATE", # Content Calendar Generator
    "P3-E2": "ANALYZE",  # Attribution Forecaster
    "P3-I1": "ANALYZE",  # Financial Anomaly Detector
    "P3-I2": "SCORE",    # Vendor Risk Scorer
    "P3-I3": "AUTOMATE", # Revenue Recognition Automation
}

# ── Model recommendations per task type per provider ──────────────────────────
# worksbuddy = WorksBuddy's own hosted LLM (OpenAI-compatible endpoint)
# Host:  WB_LLM_HOST env var  (default: http://ai-llm.worksbuddy.ai)
# Key:   WB_LLM_API_KEY env var
# Models: wb-pro (reasoning), wb-fast (generation), wb-extract, wb-vision
MODEL_MAP: dict[str, dict[str, str | None]] = {
    "PLAN": {
        "groq":         "qwen/qwen3-32b",
        "huggingface":  "deepseek-ai/DeepSeek-R1-Distill-Qwen-32B",
        "ollama":       "deepseek-r1:14b",
        "worksbuddy":   "wb-pro",
    },
    "SCORE": {
        "groq":         "qwen/qwen3-32b",
        "huggingface":  "Qwen/QwQ-32B",
        "ollama":       "qwq",
        "worksbuddy":   "wb-pro",
    },
    "GENERATE": {
        "groq":         "moonshotai/kimi-k2-instruct",
        "huggingface":  "Qwen/Qwen2.5-72B-Instruct",
        "ollama":       "llama3.3",
        "worksbuddy":   "wb-fast",
    },
    "ANALYZE": {
        "groq":         "llama-3.3-70b-versatile",
        "huggingface":  "Qwen/Qwen2.5-72B-Instruct",
        "ollama":       "llama3.3",
        "worksbuddy":   "wb-pro",
    },
    "EXTRACT": {
        "groq":         "llama-3.3-70b-versatile",
        "huggingface":  "Qwen/Qwen2.5-72B-Instruct",
        "ollama":       "llama3.3",
        "worksbuddy":   "wb-extract",
    },
    "EXTRACT_VIS": {   # image files — vision model required
        "groq":         "meta-llama/llama-4-scout-17b-16e-instruct",
        "huggingface":  None,   # not supported on featherless router
        "ollama":       "llava",
        "worksbuddy":   "wb-vision",
    },
    "NLU": {
        "groq":         "llama-3.3-70b-versatile",
        "huggingface":  "Qwen/Qwen2.5-7B-Instruct",
        "ollama":       "llama3.3",
        "worksbuddy":   "wb-fast",
    },
    "AUTOMATE": {
        "groq":         "llama-3.3-70b-versatile",
        "huggingface":  "Qwen/Qwen2.5-72B-Instruct",
        "ollama":       "llama3.3",
        "worksbuddy":   "wb-fast",
    },
}


# ── TARO-specific Ollama model (Agile Coder 14B GGUF) ─────────────────────────
# Used for all project & task management features when provider is Ollama
TARO_OLLAMA_MODEL = "hf.co/mradermacher/Qwen2.5-Agile-Coder-14B-Instruct-i1-GGUF"
TARO_FEATURE_CODES: set[str] = {
    # Features Lab — TARO (C1–C19, C79–C80)
    *[f"C{i}" for i in range(1, 20)], "C79", "C80",
    # Automation P1 — TARO
    "P1-T1", "P1-T2", "P1-T3",
    # Automation P2 — TARO
    "P2-T1", "P2-T2", "P2-T3", "P2-T4", "P2-T5", "P2-T6",
    # Automation P3 — TARO
    "P3-T1", "P3-T2", "P3-T3",
}


def get_task_type(feature_code: str) -> str:
    """Return task type for a feature code. Falls back to ANALYZE."""
    return TASK_TYPE_MAP.get(feature_code, "ANALYZE")


def get_recommended_model(feature_code: str, provider: str, is_vision: bool = False) -> tuple[str | None, str]:
    """
    Returns (recommended_model, task_type).
    TARO features on Ollama use Qwen2.5-Agile-Coder-14B-Instruct-i1-GGUF.
    recommended_model may be None if provider doesn't support vision.
    """
    task_type = TASK_TYPE_MAP.get(feature_code, "ANALYZE")
    if is_vision and task_type in ("EXTRACT", "EXTRACT_VIS"):
        task_type = "EXTRACT_VIS"
    model = MODEL_MAP.get(task_type, MODEL_MAP["ANALYZE"]).get(provider)
    # TARO features override: use dedicated Agile Coder model on Ollama
    if provider == "ollama" and feature_code in TARO_FEATURE_CODES:
        model = TARO_OLLAMA_MODEL
    return model, task_type
