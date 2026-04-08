import { Globe, Layers, Users, Zap, Megaphone, ArrowRight, Code2, Newspaper } from 'lucide-react'
import { Grid, Field, TagList } from '../../ui'
import { parseArr } from '../../../utils/leadFormatters'

export default function WebsiteIntelSection({ lead, full }) {
  // Merge from multiple sources: flat DB fields + full_data.website_intelligence or website_intel
  const wi = full.website_intelligence || full.website_intel || {}
  const productOfferings = parseArr(lead.product_offerings || wi.product_offerings)
  const targetCustomers = parseArr(lead.target_customers || wi.target_customers)
  const useCases = parseArr(wi.use_cases)
  const keyMessaging = parseArr(wi.key_messaging)
  const integrations = parseArr(wi.integrations_mentioned)
  const techClues = parseArr(wi.tech_stack_clues)
  const openRoles = parseArr(wi.open_roles)
  const recentBlog = parseArr(wi.recent_blog_topics)

  const hasData = !!(lead.value_proposition || wi.company_description || lead.business_model || productOfferings.length)

  const BMODEL_COLORS = {
    'saas': '#6366f1', 'marketplace': '#f97316', 'agency': '#10b981',
    'hardware': '#3b82f6', 'service': '#8b5cf6',
  }
  const bmColor = BMODEL_COLORS[(lead.business_model || '').toLowerCase()] || '#6b7280'

  const HIRING_COLOR = { active: '#10b981', moderate: '#f97316', limited: '#6b7280' }
  const hiringStatus = wi.hiring_signals || ''
  const hiringColor = HIRING_COLOR[hiringStatus.toLowerCase()] || '#6b7280'

  if (!hasData) {
    return (
      <div style={{
        textAlign: 'center', padding: '40px 20px',
        border: '1px dashed var(--border-1)', borderRadius: 10,
        color: 'var(--text-3)',
      }}>
        <Globe size={28} style={{ opacity: 0.2, marginBottom: 8 }} />
        <div style={{ fontSize: 13, marginBottom: 4 }}>Website Intelligence not yet scraped</div>
        <div style={{ fontSize: 11 }}>Re-enrich this lead to trigger Stage 3 website scraping</div>
      </div>
    )
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      {/* Business identity row */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill,minmax(180px,1fr))', gap: 10 }}>
        {lead.business_model && (
          <div style={{ padding: '10px 14px', borderRadius: 9, background: `${bmColor}10`, border: `1px solid ${bmColor}30` }}>
            <div style={{ fontSize: 9, fontWeight: 700, color: bmColor, textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 4 }}>Business Model</div>
            <div style={{ fontSize: 14, fontWeight: 700, color: bmColor }}>{lead.business_model}</div>
          </div>
        )}
        {lead.product_category && (
          <div style={{ padding: '10px 14px', borderRadius: 9, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
            <div style={{ fontSize: 9, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 4 }}>Product Category</div>
            <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)' }}>{lead.product_category}</div>
          </div>
        )}
        {wi.market_positioning && (
          <div style={{ padding: '10px 14px', borderRadius: 9, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
            <div style={{ fontSize: 9, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 4 }}>Market Position</div>
            <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-1)' }}>{wi.market_positioning}</div>
          </div>
        )}
        {lead.pricing_signals && (
          <div style={{ padding: '10px 14px', borderRadius: 9, background: 'rgba(16,185,129,0.06)', border: '1px solid rgba(16,185,129,0.2)' }}>
            <div style={{ fontSize: 9, fontWeight: 700, color: '#10b981', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 4 }}>Pricing Model</div>
            <div style={{ fontSize: 13, fontWeight: 600, color: '#10b981' }}>{lead.pricing_signals}</div>
          </div>
        )}
        {hiringStatus && (
          <div style={{ padding: '10px 14px', borderRadius: 9, background: `${hiringColor}10`, border: `1px solid ${hiringColor}25` }}>
            <div style={{ fontSize: 9, fontWeight: 700, color: hiringColor, textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 4 }}>Hiring Status</div>
            <div style={{ fontSize: 13, fontWeight: 600, color: hiringColor }}>{hiringStatus}</div>
          </div>
        )}
      </div>

      {/* Value proposition */}
      {(lead.value_proposition || wi.value_proposition) && (
        <div style={{ padding: '12px 14px', borderRadius: 9, background: 'rgba(99,102,241,0.06)', border: '1px solid rgba(99,102,241,0.2)' }}>
          <div style={{ fontSize: 9, fontWeight: 700, color: '#a5b4fc', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 6 }}>Value Proposition</div>
          <div style={{ fontSize: 13, color: 'var(--text-1)', lineHeight: 1.6, fontStyle: 'italic' }}>
            "{lead.value_proposition || wi.value_proposition}"
          </div>
        </div>
      )}

      {/* Problem solved */}
      {wi.problem_solved && (
        <div style={{ padding: '12px 14px', borderRadius: 9, background: 'rgba(239,68,68,0.05)', border: '1px solid rgba(239,68,68,0.15)' }}>
          <div style={{ fontSize: 9, fontWeight: 700, color: '#ef4444', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 6 }}>Problem They Solve</div>
          <div style={{ fontSize: 13, color: 'var(--text-1)', lineHeight: 1.6 }}>{wi.problem_solved}</div>
        </div>
      )}

      {/* Company description */}
      {wi.company_description && (
        <div style={{ padding: '12px 14px', borderRadius: 9, background: 'var(--bg-elevated)', border: '1px solid var(--border-1)' }}>
          <div style={{ fontSize: 9, fontWeight: 700, color: 'var(--text-3)', textTransform: 'uppercase', letterSpacing: '0.07em', marginBottom: 6 }}>Company Description</div>
          <div style={{ fontSize: 12, color: 'var(--text-2)', lineHeight: 1.7 }}>{wi.company_description}</div>
        </div>
      )}

      <Grid>
        {productOfferings.length > 0 && (
          <Field icon={<Layers size={12} />} label="Product Offerings"
            value={<TagList items={productOfferings} color="#6366f1" />} wide />
        )}
        {targetCustomers.length > 0 && (
          <Field icon={<Users size={12} />} label="Target Customers"
            value={<TagList items={targetCustomers} color="#f97316" />} wide />
        )}
        {useCases.length > 0 && (
          <Field icon={<Zap size={12} />} label="Use Cases"
            value={<TagList items={useCases} color="#10b981" />} wide />
        )}
        {keyMessaging.length > 0 && (
          <Field icon={<Megaphone size={12} />} label="Key Messaging Themes"
            value={<TagList items={keyMessaging} color="#8b5cf6" />} wide />
        )}
        {integrations.length > 0 && (
          <Field icon={<ArrowRight size={12} />} label="Integrations Mentioned"
            value={<TagList items={integrations} color="#3b82f6" />} wide />
        )}
        {techClues.length > 0 && (
          <Field icon={<Code2 size={12} />} label="Tech Stack Clues"
            value={<TagList items={techClues} color="#6b7280" />} wide />
        )}
        {openRoles.length > 0 && (
          <Field icon={<Users size={12} />} label="Open Roles"
            value={<TagList items={openRoles} color="#10b981" />} wide />
        )}
        {recentBlog.length > 0 && (
          <Field icon={<Newspaper size={12} />} label="Recent Blog Topics"
            value={<TagList items={recentBlog} color="#f59e0b" />} wide />
        )}
      </Grid>

      {/* Pages scraped */}
      {wi.pages_scraped && wi.pages_scraped.length > 0 && (
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: 10, color: 'var(--text-3)' }}>Pages scraped:</span>
          {wi.pages_scraped.map(p => (
            <span key={p} style={{
              fontSize: 9, padding: '1px 7px', borderRadius: 10, fontWeight: 600,
              background: 'rgba(16,185,129,0.1)', color: '#10b981', border: '1px solid rgba(16,185,129,0.25)',
            }}>{p}</span>
          ))}
        </div>
      )}
    </div>
  )
}
