// leadConfig.jsx — constants used across lead enrichment components
import { Flame, Star, ThumbsUp, Snowflake, User, Building2, Mail, Globe, Target, Database } from 'lucide-react'

export const TIER = {
  hot:  { color: '#ef4444', bg: 'rgba(239,68,68,0.10)',  border: 'rgba(239,68,68,0.28)',  icon: <Flame    size={11} />, label: 'Hot'  },
  warm: { color: '#f97316', bg: 'rgba(249,115,22,0.10)', border: 'rgba(249,115,22,0.28)', icon: <Star     size={11} />, label: 'Warm' },
  cool: { color: '#3b82f6', bg: 'rgba(59,130,246,0.10)', border: 'rgba(59,130,246,0.28)', icon: <ThumbsUp size={11} />, label: 'Cool' },
  cold: { color: '#6b7280', bg: 'rgba(107,114,128,0.08)',border: 'rgba(107,114,128,0.22)',icon: <Snowflake size={11} />, label: 'Cold' },
}

// ISO 3166-1 alpha-2 → full country name (frontend fallback for existing DB records)
export const COUNTRY_CODES = {
  AF:'Afghanistan',AL:'Albania',DZ:'Algeria',AR:'Argentina',AU:'Australia',
  AT:'Austria',BE:'Belgium',BR:'Brazil',CA:'Canada',CL:'Chile',CN:'China',
  CO:'Colombia',CZ:'Czech Republic',DK:'Denmark',EG:'Egypt',FI:'Finland',
  FR:'France',DE:'Germany',GH:'Ghana',GR:'Greece',HK:'Hong Kong',HU:'Hungary',
  IN:'India',ID:'Indonesia',IE:'Ireland',IL:'Israel',IT:'Italy',JP:'Japan',
  KE:'Kenya',KR:'South Korea',MY:'Malaysia',MX:'Mexico',MA:'Morocco',
  NL:'Netherlands',NZ:'New Zealand',NG:'Nigeria',NO:'Norway',PK:'Pakistan',
  PE:'Peru',PH:'Philippines',PL:'Poland',PT:'Portugal',RO:'Romania',RU:'Russia',
  SA:'Saudi Arabia',ZA:'South Africa',ES:'Spain',SE:'Sweden',CH:'Switzerland',
  TW:'Taiwan',TH:'Thailand',TR:'Turkey',UA:'Ukraine',AE:'United Arab Emirates',
  GB:'United Kingdom',US:'United States',VN:'Vietnam',
}

export const ENRICH_TABS = [
  { id: 'linkedin',           label: 'LinkedIn Enrich',    icon: '🔗', color: '#6366f1' },
  { id: 'email',              label: 'Email Enrich',       icon: '✉️', color: '#10b981' },
  { id: 'outreach',           label: 'Outreach Enrich',    icon: '📤', color: '#f59e0b' },
  { id: 'company',            label: 'Company Enrich',     icon: '🏢', color: '#3b82f6' },
  { id: 'brightdata_profile', label: 'BD Profile (Raw)',   icon: '👤', color: '#8b5cf6', rawData: true },
  { id: 'brightdata_company', label: 'BD Company (Raw)',   icon: '🏗️', color: '#06b6d4', rawData: true },
  { id: 'apollo_raw',         label: 'Apollo Raw',         icon: '🚀', color: '#f97316', rawData: true },
  { id: 'website_scrap',      label: 'Website Scrap',      icon: '🌐', color: '#84cc16', rawData: true },
]

// IDs of tabs that load from the single /raw-data endpoint
export const RAW_DATA_TABS = new Set(['brightdata_profile', 'brightdata_company', 'apollo_raw', 'website_scrap'])

export const AI_MODULES = [
  { id: 'identity',             name: 'Identity',             route: 'identity',             color: '#06b6d4', icon: '👤', group: 'core' },
  { id: 'contact',              name: 'Contact',              route: 'contact',              color: '#10b981', icon: '📧', group: 'core' },
  { id: 'scores',               name: 'Scores',               route: 'scores',               color: '#f59e0b', icon: '📊', group: 'core' },
  { id: 'icp_match',            name: 'ICP Match',            route: 'icp-match',            color: '#8b5cf6', icon: '🎯', group: 'intel' },
  { id: 'behavioural_signals',  name: 'Behavioural Signals',  route: 'behavioural-signals',  color: '#6366f1', icon: '🧠', group: 'intel' },
  { id: 'pitch_intelligence',   name: 'Pitch Intelligence',   route: 'pitch-intelligence',   color: '#ec4899', icon: '💡', group: 'intel' },
  { id: 'activity',             name: 'Activity',             route: 'activity',             color: '#14b8a6', icon: '⚡', group: 'intel' },
  { id: 'tags',                 name: 'Tags',                 route: 'tags',                 color: '#a78bfa', icon: '🏷️', group: 'output' },
  { id: 'outreach',             name: 'Outreach',             route: 'outreach',             color: '#ef4444', icon: '✉️', group: 'output' },
  { id: 'persona_analysis',     name: 'Persona Analysis',     route: 'persona-analysis',     color: '#f97316', icon: '🔍', group: 'output' },
]

export const STREAM_STAGES = [
  { id: 'profile',  label: 'BD Profile Fetch',       icon: <User      size={13} />, desc: 'Name · Title · Company · Avatar' },
  { id: 'company',  label: 'Company Waterfall',       icon: <Building2 size={13} />, desc: 'Logo · Website · Industry · Size' },
  { id: 'contact',  label: 'Contact Waterfall',       icon: <Mail      size={13} />, desc: 'Work Email · Phone (Hunter/Apollo)' },
  { id: 'website',  label: 'Website Intelligence',    icon: <Globe     size={13} />, desc: 'Value Prop · Products · Tech Stack' },
  { id: 'scoring',  label: 'LLM Scoring + Outreach',  icon: <Target    size={13} />, desc: 'ICP Score · Intent · Cold Email' },
  { id: 'complete', label: 'Save & Complete',         icon: <Database  size={13} />, desc: 'Lead persisted to DB' },
]

export const STAGE_COLORS = { waiting: '#6b7280', loading: '#6366f1', done: '#10b981', error: '#ef4444' }
