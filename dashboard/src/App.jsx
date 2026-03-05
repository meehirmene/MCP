import { useState, useEffect, useRef, useCallback } from 'react'
import './App.css'

const API_BASE = 'http://localhost:8000'

/* ─────────────────────────────────────────────
   API Helpers
   ───────────────────────────────────────────── */
async function api(path, options = {}) {
  try {
    const res = await fetch(`${API_BASE}${path}`, {
      headers: { 'Content-Type': 'application/json' },
      ...options,
    })
    return await res.json()
  } catch (e) {
    return { error: e.message }
  }
}

/* ─────────────────────────────────────────────
   Sidebar
   ───────────────────────────────────────────── */
function Sidebar({ active, onNavigate }) {
  const items = [
    { id: 'dashboard', icon: '📊', label: 'Dashboard' },
    { id: 'pipelines', icon: '🔄', label: 'Pipelines' },
    { id: 'agents', icon: '🤖', label: 'Agents' },
    { id: 'databases', icon: '🗄️', label: 'Databases' },
    { id: 'events', icon: '📜', label: 'Events' },
  ]

  return (
    <aside className="sidebar">
      <div className="sidebar-logo">
        <div className="logo-icon">🔥</div>
        <h1>FlowForge</h1>
      </div>
      <nav className="sidebar-nav">
        {items.map(item => (
          <div
            key={item.id}
            className={`nav-item ${active === item.id ? 'active' : ''}`}
            onClick={() => onNavigate(item.id)}
          >
            <span className="icon">{item.icon}</span>
            {item.label}
          </div>
        ))}
      </nav>
      <div style={{ padding: '12px 16px', borderTop: '1px solid var(--glass-border)' }}>
        <div className="nav-item" onClick={() => window.open(`${API_BASE}/docs`, '_blank')}>
          <span className="icon">📖</span>
          API Docs
        </div>
      </div>
    </aside>
  )
}

/* ─────────────────────────────────────────────
   Agent Card
   ───────────────────────────────────────────── */
function AgentCard({ name, icon, color, status = 'active', task = '' }) {
  return (
    <div className="agent-card" style={{ '--agent-color': color }}>
      <div className="agent-icon">{icon}</div>
      <div className="agent-name">{name}</div>
      <div className="agent-status">
        <span className="dot" style={{ background: status === 'active' ? 'var(--status-healthy)' : 'var(--text-muted)' }}></span>
        {status}
      </div>
      {task && <div style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '6px' }}>{task}</div>}
    </div>
  )
}

/* ─────────────────────────────────────────────
   MCP Server Card
   ───────────────────────────────────────────── */
function McpCard({ name, icon, status, latency }) {
  return (
    <div className="mcp-card">
      <div className="mcp-icon">{icon}</div>
      <div className="mcp-name">{name}</div>
      <div className="mcp-status">{status}</div>
      <div className="mcp-latency">Latency: {latency}</div>
    </div>
  )
}

/* ─────────────────────────────────────────────
   Result Panel
   ───────────────────────────────────────────── */
function ResultPanel({ result, onClose }) {
  if (!result) return null

  return (
    <div className="result-panel">
      <div className="result-header">
        <span style={{ fontSize: '20px' }}>✨</span>
        <span className="result-title">Pipeline Request Complete</span>
        <button className="btn btn-ghost" onClick={onClose} style={{ marginLeft: 'auto' }}>✕</button>
      </div>
      <div className="result-summary">{result.summary || 'Request processed successfully.'}</div>
      {result.results && (
        <div className="result-tasks">
          {Object.entries(result.results).map(([id, task]) => (
            <div key={id} className="result-task">
              <span className="task-status">{task.status === 'completed' ? '✅' : '❌'}</span>
              <span style={{ flex: 1, color: 'var(--text-secondary)' }}>{task.description || id}</span>
              <span className="task-agent">{task.agent}</span>
            </div>
          ))}
        </div>
      )}
      <div style={{ marginTop: '12px', fontSize: '12px', color: 'var(--text-muted)' }}>
        Tasks: {result.tasks_completed || 0} completed, {result.tasks_failed || 0} failed
      </div>
    </div>
  )
}

/* ─────────────────────────────────────────────
   Main App
   ───────────────────────────────────────────── */
/* ─────────────────────────────────────────────
   Demo / Mock Data for Showcase
   ───────────────────────────────────────────── */
const DEMO_PIPELINES = [
  {
    pipeline_id: 'cdc-orders-to-iceberg',
    status: 'running',
    source: 'PostgreSQL → Kafka → Flink → Iceberg',
    throughput: '1,240 events/sec',
    uptime: '2h 14m',
    tasks_completed: 8,
    created_at: new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(),
  },
  {
    pipeline_id: 'regional-order-aggregation',
    status: 'running',
    source: 'Flink Tumbling Window (5m)',
    throughput: '320 aggregations/min',
    uptime: '1h 47m',
    tasks_completed: 5,
    created_at: new Date(Date.now() - 1.8 * 60 * 60 * 1000).toISOString(),
  },
  {
    pipeline_id: 'realtime-anomaly-detection',
    status: 'running',
    source: 'Kafka → Flink CEP → MongoDB',
    throughput: '890 events/sec',
    uptime: '45m',
    tasks_completed: 6,
    created_at: new Date(Date.now() - 45 * 60 * 1000).toISOString(),
  },
]

const DEMO_EVENTS = [
  {
    agent_role: 'orchestrator',
    event_type: 'USER_REQUEST_RECEIVED',
    details: { request: 'Stream orders from Postgres to Iceberg, aggregate by region every 5 min' },
    logged_at: new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(),
  },
  {
    agent_role: 'orchestrator',
    event_type: 'TASK_COMPLETED',
    details: { task: 'Decomposed request into 8 sub-tasks', assigned_to: 'builder' },
    logged_at: new Date(Date.now() - 2 * 60 * 60 * 1000 + 3000).toISOString(),
  },
  {
    agent_role: 'builder',
    event_type: 'TASK_COMPLETED',
    details: { task: 'Inspected PostgreSQL orders table — 5 tables, 55 rows, CDC ready (wal_level=logical)' },
    logged_at: new Date(Date.now() - 2 * 60 * 60 * 1000 + 8000).toISOString(),
  },
  {
    agent_role: 'builder',
    event_type: 'TASK_COMPLETED',
    details: { task: 'Created Kafka topic cdc.ecommerce.orders (3 partitions, RF=1)' },
    logged_at: new Date(Date.now() - 2 * 60 * 60 * 1000 + 15000).toISOString(),
  },
  {
    agent_role: 'builder',
    event_type: 'TASK_COMPLETED',
    details: { task: 'Generated Flink CDC source table SQL + tumbling window aggregation' },
    logged_at: new Date(Date.now() - 2 * 60 * 60 * 1000 + 22000).toISOString(),
  },
  {
    agent_role: 'builder',
    event_type: 'TASK_COMPLETED',
    details: { task: 'Deployed Flink job: regional-order-aggregation (5m tumbling window)' },
    logged_at: new Date(Date.now() - 2 * 60 * 60 * 1000 + 30000).toISOString(),
  },
  {
    agent_role: 'monitor',
    event_type: 'TASK_COMPLETED',
    details: { task: 'Health baseline set — tracking orders schema, Flink job health, Kafka lag' },
    logged_at: new Date(Date.now() - 2 * 60 * 60 * 1000 + 35000).toISOString(),
  },
  {
    agent_role: 'monitor',
    event_type: 'ANOMALY_DETECTED',
    details: { task: 'Schema drift detected: new column discount_pct added to orders table' },
    logged_at: new Date(Date.now() - 45 * 60 * 1000).toISOString(),
  },
  {
    agent_role: 'orchestrator',
    event_type: 'TASK_IN_PROGRESS',
    details: { task: 'Routing SCHEMA_DRIFT event to Healer agent (priority: 1)' },
    logged_at: new Date(Date.now() - 44 * 60 * 1000).toISOString(),
  },
  {
    agent_role: 'healer',
    event_type: 'TASK_COMPLETED',
    details: { task: 'Auto-healed: ALTER TABLE iceberg.orders ADD COLUMN discount_pct DECIMAL(5,2)' },
    logged_at: new Date(Date.now() - 43 * 60 * 1000).toISOString(),
  },
  {
    agent_role: 'healer',
    event_type: 'TASK_COMPLETED',
    details: { task: 'Restarted Flink CDC job — pipeline restored to healthy state' },
    logged_at: new Date(Date.now() - 42 * 60 * 1000).toISOString(),
  },
  {
    agent_role: 'monitor',
    event_type: 'TASK_COMPLETED',
    details: { task: 'Post-heal verification: all 3 pipelines healthy, throughput nominal at 1,240 evt/s' },
    logged_at: new Date(Date.now() - 41 * 60 * 1000).toISOString(),
  },
]

const DEMO_AGENT_TASKS = {
  'Orchestrator': 'Coordinating 3 active pipelines',
  'Builder': 'Last: Deployed regional-order-aggregation',
  'Monitor': 'Watching 3 pipelines, 5 tables, 2 Flink jobs',
  'Healer': 'Last: Auto-healed schema drift (42m ago)',
  'Optimizer': 'Analyzing Flink parallelism for tuning',
  'Quality': 'Checking data freshness SLAs',
}

function App() {
  const [page, setPage] = useState('dashboard')
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState(null)
  const [health, setHealth] = useState(null)
  const [events, setEvents] = useState(DEMO_EVENTS)
  const [agents, setAgents] = useState([])
  const [pipelines, setPipelines] = useState(DEMO_PIPELINES)
  const inputRef = useRef(null)

  // Fetch initial data
  useEffect(() => {
    fetchHealth()
    fetchEvents()
    fetchAgents()
    fetchPipelines()
    const interval = setInterval(() => {
      fetchEvents()
      fetchHealth()
    }, 10000)
    return () => clearInterval(interval)
  }, [])

  const fetchHealth = async () => {
    const data = await api('/api/health')
    if (!data.error) setHealth(data)
  }

  const fetchEvents = async () => {
    const data = await api('/api/events')
    if (!data.error && data.events && data.events.length > 0) {
      setEvents(data.events)
    }
    // Keep demo events if API returns empty
  }

  const fetchAgents = async () => {
    const data = await api('/api/agents')
    if (!data.error && data.agents) setAgents(data.agents)
  }

  const fetchPipelines = async () => {
    const data = await api('/api/pipelines')
    if (!data.error && data.pipelines && data.pipelines.length > 0) {
      setPipelines(data.pipelines)
    }
    // Keep demo pipelines if API returns empty
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    if (!input.trim() || loading) return

    setLoading(true)
    setResult(null)

    const data = await api('/api/request', {
      method: 'POST',
      body: JSON.stringify({ request: input }),
    })

    setResult(data)
    setLoading(false)
    setInput('')
    fetchEvents()
    fetchPipelines()
  }

  const handleGenerateData = async (target) => {
    await api('/api/generate-data', {
      method: 'POST',
      body: JSON.stringify({ target, count: 50 }),
    })
    fetchEvents()
  }

  const handleSchemaDrift = async () => {
    const data = await api('/api/simulate/schema-drift', { method: 'POST' })
    setEvents(prev => [{
      agent_role: 'system',
      event_type: 'SCHEMA_DRIFT_SIMULATED',
      details: data,
      logged_at: new Date().toISOString(),
    }, ...prev])
  }

  const agentDefs = [
    { name: 'Orchestrator', icon: '🧠', color: 'var(--agent-orchestrator)' },
    { name: 'Builder', icon: '🔨', color: 'var(--agent-builder)' },
    { name: 'Monitor', icon: '👁', color: 'var(--agent-monitor)' },
    { name: 'Healer', icon: '🩺', color: 'var(--agent-healer)' },
    { name: 'Optimizer', icon: '⚡', color: 'var(--agent-optimizer)' },
    { name: 'Quality', icon: '✅', color: 'var(--agent-quality)' },
  ]

  const mcpServers = [
    { name: 'Kafka', icon: '📨', status: 'Connected', latency: '2.1ms' },
    { name: 'Flink', icon: '⚡', status: 'Connected', latency: '3.0ms' },
    { name: 'Iceberg', icon: '🧊', status: 'Connected', latency: '4.2ms' },
    { name: 'PostgreSQL', icon: '🐘', status: 'Connected', latency: '1.5ms' },
    { name: 'MongoDB', icon: '🍃', status: 'Connected', latency: '2.8ms' },
  ]

  const eventIcons = {
    USER_REQUEST_RECEIVED: '📨',
    TASK_COMPLETED: '✅',
    TASK_FAILED: '❌',
    TASK_IN_PROGRESS: '⏳',
    ANOMALY_DETECTED: '⚠️',
    SCHEMA_DRIFT: '🔄',
    SCHEMA_DRIFT_SIMULATED: '🧪',
  }

  const overallStatus = health?.overall_status || 'unknown'
  const statusColor = { healthy: 'var(--status-healthy)', degraded: 'var(--status-degraded)', critical: 'var(--status-critical)' }

  return (
    <div className="app-layout">
      <Sidebar active={page} onNavigate={setPage} />

      <main className="main-content">
        {/* Header with NL Input */}
        <header className="header">
          <form className="nl-input-container" onSubmit={handleSubmit}>
            <input
              ref={inputRef}
              className="nl-input"
              type="text"
              placeholder="Describe your pipeline... e.g. 'Stream orders from Postgres, aggregate by region every 5 min'"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              disabled={loading}
            />
            <button className="nl-submit" type="submit" disabled={loading || !input.trim()}>
              {loading ? <span className="loading-spinner"></span> : 'Submit'}
            </button>
          </form>
          <div className="header-status">
            <span className="status-dot" style={{ background: statusColor[overallStatus] || 'var(--text-muted)' }}></span>
            {agents.length || 6} agents online
          </div>
        </header>

        {/* Dashboard Content */}
        <div className="dashboard-content">
          {/* Loading state */}
          {loading && (
            <div className="loading-overlay">
              <span className="loading-spinner"></span>
              Agents are processing your request...
            </div>
          )}

          {/* Result panel */}
          {result && <ResultPanel result={result} onClose={() => setResult(null)} />}

          {page === 'dashboard' && (
            <>
              {/* Active Agents */}
              <div className="card full-width">
                <div className="card-header">
                  <span className="card-title">Active Agents</span>
                  <span className="card-badge">{agentDefs.length} online</span>
                </div>
                <div className="agents-grid">
                  {agentDefs.map(a => (
                    <AgentCard key={a.name} {...a} status="active" task={DEMO_AGENT_TASKS[a.name] || ''} />
                  ))}
                </div>
              </div>

              <div className="dashboard-grid">
                {/* Pipeline Status */}
                <div className="card">
                  <div className="card-header">
                    <span className="card-title">Pipeline Status</span>
                    <span className="card-badge">{pipelines.length} active</span>
                  </div>
                  {pipelines.length > 0 ? pipelines.map((p, i) => (
                    <div key={i} className="pipeline-row">
                      <div className="pipeline-info">
                        <div className="pipeline-name">{p.pipeline_id || `Pipeline ${i + 1}`}</div>
                        <div className="pipeline-metric">
                          {p.source && <span style={{ color: 'var(--text-muted)', fontSize: '11px' }}>{p.source}</span>}
                        </div>
                        <div style={{ display: 'flex', gap: '12px', marginTop: '4px', fontSize: '11px' }}>
                          {p.throughput && <span style={{ color: 'var(--accent-cyan)' }}>⚡ {p.throughput}</span>}
                          {p.uptime && <span style={{ color: 'var(--status-healthy)' }}>⏱ {p.uptime}</span>}
                          {p.tasks_completed && <span style={{ color: 'var(--accent-purple)' }}>✓ {p.tasks_completed} tasks</span>}
                        </div>
                      </div>
                      <div className="pipeline-health">
                        <span style={{
                          fontSize: '11px', padding: '2px 8px', borderRadius: '12px',
                          background: p.status === 'running' ? 'rgba(16, 185, 129, 0.15)' : 'rgba(245, 158, 11, 0.15)',
                          color: p.status === 'running' ? 'var(--status-healthy)' : 'var(--status-degraded)',
                          fontWeight: 600,
                        }}>
                          {p.status === 'running' ? '● Running' : p.status}
                        </span>
                      </div>
                    </div>
                  )) : (
                    <div style={{ color: 'var(--text-muted)', fontSize: '13px', padding: '16px 0' }}>
                      No pipelines deployed yet. Send a request above to create one!
                    </div>
                  )}
                </div>

                {/* Activity Feed */}
                <div className="card">
                  <div className="card-header">
                    <span className="card-title">Agent Activity Feed</span>
                    <span className="card-badge">{events.length} events</span>
                  </div>
                  <div className="activity-list">
                    {events.length > 0 ? events.slice(0, 15).map((evt, i) => (
                      <div key={i} className="activity-item">
                        <span className="activity-icon">{eventIcons[evt.event_type] || '📌'}</span>
                        <div className="activity-content">
                          <div className="activity-text">
                            <strong style={{ color: 'var(--accent-cyan)' }}>{evt.agent_role}</strong>{' '}
                            {evt.event_type?.replace(/_/g, ' ').toLowerCase()}
                            {evt.details?.request && `: "${evt.details.request.slice(0, 50)}..."`}
                            {evt.details?.task && `: ${evt.details.task.slice(0, 50)}`}
                          </div>
                          <div className="activity-time">
                            {evt.logged_at ? new Date(evt.logged_at).toLocaleTimeString() : ''}
                          </div>
                        </div>
                      </div>
                    )) : (
                      <div style={{ color: 'var(--text-muted)', fontSize: '13px', padding: '16px 0' }}>
                        No activity yet. Send a request to see agents in action!
                      </div>
                    )}
                  </div>
                </div>
              </div>

              {/* MCP Server Connections */}
              <div className="card full-width">
                <div className="card-header">
                  <span className="card-title">MCP Server Connections</span>
                  <span className="card-badge">{mcpServers.length} connected</span>
                </div>
                <div className="mcp-grid">
                  {mcpServers.map(s => (
                    <McpCard key={s.name} {...s} />
                  ))}
                </div>
              </div>

              {/* Quick Actions */}
              <div className="card full-width">
                <div className="card-header">
                  <span className="card-title">Quick Actions</span>
                </div>
                <div style={{ display: 'flex', gap: '10px', flexWrap: 'wrap' }}>
                  <button className="btn btn-primary" onClick={() => handleGenerateData('kafka')}>
                    📨 Generate Kafka Events
                  </button>
                  <button className="btn btn-primary" onClick={() => handleGenerateData('postgres')}>
                    🐘 Insert PostgreSQL Orders
                  </button>
                  <button className="btn btn-danger" onClick={handleSchemaDrift}>
                    🧪 Simulate Schema Drift
                  </button>
                  <button className="btn btn-ghost" onClick={fetchHealth}>
                    🏥 Health Check
                  </button>
                </div>
              </div>
            </>
          )}

          {page === 'agents' && (
            <div className="card full-width">
              <div className="card-header">
                <span className="card-title">Agent Registry</span>
              </div>
              <div className="agents-grid">
                {agentDefs.map(a => (
                  <AgentCard key={a.name} {...a} status="active" task={DEMO_AGENT_TASKS[a.name] || ''} />
                ))}
              </div>
            </div>
          )}

          {page === 'events' && (
            <div className="card full-width">
              <div className="card-header">
                <span className="card-title">Event Log</span>
                <button className="btn btn-ghost" onClick={fetchEvents}>Refresh</button>
              </div>
              <div className="activity-list" style={{ maxHeight: '600px' }}>
                {events.map((evt, i) => (
                  <div key={i} className="activity-item">
                    <span className="activity-icon">{eventIcons[evt.event_type] || '📌'}</span>
                    <div className="activity-content">
                      <div className="activity-text">
                        <strong style={{ color: 'var(--accent-cyan)' }}>{evt.agent_role}</strong>{' '}
                        {evt.event_type?.replace(/_/g, ' ').toLowerCase()}
                      </div>
                      <div className="activity-time">
                        {evt.logged_at ? new Date(evt.logged_at).toLocaleString() : ''}
                      </div>
                      {evt.details && (
                        <pre style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '4px', whiteSpace: 'pre-wrap' }}>
                          {JSON.stringify(evt.details, null, 2).slice(0, 200)}
                        </pre>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {page === 'databases' && (
            <div className="card full-width">
              <div className="card-header">
                <span className="card-title">Connected Databases</span>
              </div>
              <div className="mcp-grid">
                {mcpServers.map(s => (
                  <McpCard key={s.name} {...s} />
                ))}
              </div>
            </div>
          )}

          {page === 'pipelines' && (
            <div className="card full-width">
              <div className="card-header">
                <span className="card-title">All Pipelines</span>
              </div>
              {pipelines.length > 0 ? pipelines.map((p, i) => (
                <div key={i} className="pipeline-row">
                  <div className="pipeline-info">
                    <div className="pipeline-name">{p.pipeline_id}</div>
                    <div className="pipeline-metric">
                      {p.source && <span style={{ color: 'var(--text-muted)', fontSize: '12px' }}>{p.source}</span>}
                    </div>
                    <div style={{ display: 'flex', gap: '12px', marginTop: '6px', fontSize: '12px' }}>
                      {p.throughput && <span style={{ color: 'var(--accent-cyan)' }}>⚡ {p.throughput}</span>}
                      {p.uptime && <span style={{ color: 'var(--status-healthy)' }}>⏱ Uptime: {p.uptime}</span>}
                      {p.tasks_completed && <span style={{ color: 'var(--accent-purple)' }}>✓ {p.tasks_completed} tasks completed</span>}
                    </div>
                  </div>
                  <span style={{
                    fontSize: '12px', padding: '3px 10px', borderRadius: '12px',
                    background: p.status === 'running' ? 'rgba(16, 185, 129, 0.15)' : 'rgba(245, 158, 11, 0.15)',
                    color: p.status === 'running' ? 'var(--status-healthy)' : 'var(--status-degraded)',
                    fontWeight: 600,
                  }}>
                    {p.status === 'running' ? '● Running' : p.status}
                  </span>
                </div>
              )) : (
                <div style={{ color: 'var(--text-muted)', fontSize: '14px', padding: '24px 0', textAlign: 'center' }}>
                  No pipelines deployed yet.<br />
                  <span style={{ fontSize: '12px' }}>Send a natural language request to create your first pipeline.</span>
                </div>
              )}
            </div>
          )}
        </div>
      </main>
    </div>
  )
}

export default App
