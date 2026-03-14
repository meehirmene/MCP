import { useState, useEffect, useRef, useCallback, useMemo } from 'react'
import './App.css'

const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000'

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

function timeAgo(isoString) {
  if (!isoString) return ''
  const diff = Date.now() - new Date(isoString).getTime()
  if (diff < 60000) return `${Math.floor(diff / 1000)}s ago`
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`
  return new Date(isoString).toLocaleDateString()
}

/* ─────────────────────────────────────────────
   Constants
   ───────────────────────────────────────────── */
const EVENT_ICONS = {
  USER_REQUEST_RECEIVED: '📨',
  TASK_COMPLETED: '✅',
  TASK_FAILED: '❌',
  TASK_IN_PROGRESS: '⏳',
  ANOMALY_DETECTED: '⚠️',
  SCHEMA_DRIFT: '🔄',
  SCHEMA_DRIFT_SIMULATED: '🧪',
  SCHEMA_HEALED: '💚',
  JOB_RESTARTED: '🔁',
  CACHED_PLAN_USED: '⚡',
  FLINK_JOB_SUBMITTED: '🚀',
  REQUEST_COMPLETED: '🎉',
}

const AGENT_COLORS = {
  orchestrator: 'var(--agent-orchestrator)',
  builder: 'var(--agent-builder)',
  monitor: 'var(--agent-monitor)',
  healer: 'var(--agent-healer)',
  optimizer: 'var(--agent-optimizer)',
  quality: 'var(--agent-quality)',
}

const TOAST_TRIGGERS = {
  SCHEMA_DRIFT: { type: 'warning', msg: 'Schema drift detected — Healer is responding' },
  SCHEMA_HEALED: { type: 'success', msg: 'Schema drift healed successfully' },
  JOB_RESTARTED: { type: 'info', msg: 'Flink job restarted after healing' },
  TASK_FAILED: { type: 'error', msg: 'A task failed — check event log for details' },
  REQUEST_COMPLETED: { type: 'success', msg: 'Pipeline request completed' },
  SCHEDULED_RUN: { type: 'info', msg: 'Scheduled pipeline run completed' },
}

/* ─────────────────────────────────────────────
   Demo Data
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
  { agent_role: 'orchestrator', event_type: 'USER_REQUEST_RECEIVED', details: { request: 'Stream orders from Postgres to Iceberg, aggregate by region every 5 min' }, logged_at: new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString() },
  { agent_role: 'orchestrator', event_type: 'TASK_COMPLETED', details: { task: 'Decomposed request into 8 sub-tasks', assigned_to: 'builder' }, logged_at: new Date(Date.now() - 2 * 60 * 60 * 1000 + 3000).toISOString() },
  { agent_role: 'builder', event_type: 'TASK_COMPLETED', details: { task: 'Inspected PostgreSQL orders table — 5 tables, 55 rows, CDC ready (wal_level=logical)' }, logged_at: new Date(Date.now() - 2 * 60 * 60 * 1000 + 8000).toISOString() },
  { agent_role: 'builder', event_type: 'FLINK_JOB_SUBMITTED', details: { task: 'Created Kafka topic cdc.ecommerce.orders (3 partitions, RF=1)' }, logged_at: new Date(Date.now() - 2 * 60 * 60 * 1000 + 15000).toISOString() },
  { agent_role: 'builder', event_type: 'TASK_COMPLETED', details: { task: 'Generated Flink CDC source table SQL + tumbling window aggregation' }, logged_at: new Date(Date.now() - 2 * 60 * 60 * 1000 + 22000).toISOString() },
  { agent_role: 'monitor', event_type: 'TASK_COMPLETED', details: { task: 'Health baseline set — tracking orders schema, Flink job health, Kafka lag' }, logged_at: new Date(Date.now() - 2 * 60 * 60 * 1000 + 35000).toISOString() },
  { agent_role: 'monitor', event_type: 'SCHEMA_DRIFT', details: { task: 'Schema drift detected: new column discount_pct added to orders table' }, logged_at: new Date(Date.now() - 45 * 60 * 1000).toISOString() },
  { agent_role: 'orchestrator', event_type: 'TASK_IN_PROGRESS', details: { task: 'Routing SCHEMA_DRIFT event to Healer agent (priority: 1)' }, logged_at: new Date(Date.now() - 44 * 60 * 1000).toISOString() },
  { agent_role: 'healer', event_type: 'SCHEMA_HEALED', details: { task: 'Auto-healed: ALTER TABLE iceberg.orders ADD COLUMN discount_pct DECIMAL(5,2)' }, logged_at: new Date(Date.now() - 43 * 60 * 1000).toISOString() },
  { agent_role: 'healer', event_type: 'JOB_RESTARTED', details: { task: 'Restarted Flink CDC job — pipeline restored to healthy state' }, logged_at: new Date(Date.now() - 42 * 60 * 1000).toISOString() },
  { agent_role: 'monitor', event_type: 'TASK_COMPLETED', details: { task: 'Post-heal verification: all 3 pipelines healthy, throughput nominal at 1,240 evt/s' }, logged_at: new Date(Date.now() - 41 * 60 * 1000).toISOString() },
  { agent_role: 'orchestrator', event_type: 'CACHED_PLAN_USED', details: { task: 'Cache hit: reused CDC pipeline plan (success_count=4)' }, logged_at: new Date(Date.now() - 30 * 60 * 1000).toISOString() },
]

/* ─────────────────────────────────────────────
   Toast
   ───────────────────────────────────────────── */
function ToastContainer({ toasts, onDismiss }) {
  return (
    <div className="toast-container">
      {toasts.map(t => (
        <div key={t.id} className={`toast toast-${t.type}`} onClick={() => onDismiss(t.id)}>
          <span className="toast-icon">
            {t.type === 'success' ? '✅' : t.type === 'error' ? '❌' : t.type === 'warning' ? '⚠️' : 'ℹ️'}
          </span>
          {t.msg}
        </div>
      ))}
    </div>
  )
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
    { id: 'learnings', icon: '🧠', label: 'Learnings' },
    { id: 'roadmap', icon: '🗺️', label: 'Roadmap' },
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
function AgentCard({ name, icon, color, status = 'active', task = '', busy = false }) {
  return (
    <div className={`agent-card ${busy ? 'agent-busy' : ''}`} style={{ '--agent-color': color }}>
      <div className="agent-icon">{icon}</div>
      <div className="agent-name">{name}</div>
      <div className="agent-status">
        <span className="dot" style={{ background: busy ? 'var(--status-degraded)' : status === 'active' ? 'var(--status-healthy)' : 'var(--text-muted)' }}></span>
        {busy ? 'working' : status}
      </div>
      {task && <div style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '6px', lineHeight: '1.4' }}>{task}</div>}
    </div>
  )
}

/* ─────────────────────────────────────────────
   MCP Server Card
   ───────────────────────────────────────────── */
function McpCard({ name, icon, healthy, detail }) {
  return (
    <div className="mcp-card">
      <div className="mcp-icon">{icon}</div>
      <div className="mcp-name">{name}</div>
      <div className="mcp-status" style={{ color: healthy ? 'var(--status-healthy)' : 'var(--status-critical)' }}>
        {healthy ? '● Connected' : '● Offline'}
      </div>
      {detail && <div className="mcp-latency">{detail}</div>}
    </div>
  )
}

/* ─────────────────────────────────────────────
   Pipeline Detail Panel
   ───────────────────────────────────────────── */
function PipelineDetail({ pipeline, events, onClose, onSchemaDrift }) {
  const relatedEvents = events.filter(e =>
    e.details?.pipeline_id === pipeline.pipeline_id ||
    (e.details?.task || '').toLowerCase().includes(pipeline.pipeline_id.split('-')[0])
  ).slice(0, 8)

  return (
    <div className="pipeline-detail-panel">
      <div className="card-header" style={{ marginBottom: '12px' }}>
        <span className="card-title">{pipeline.pipeline_id}</span>
        <div style={{ display: 'flex', gap: '8px' }}>
          <button className="btn btn-danger" style={{ fontSize: '11px', padding: '4px 10px' }} onClick={onSchemaDrift}>
            🧪 Simulate Schema Drift
          </button>
          <button className="btn btn-ghost" style={{ fontSize: '11px', padding: '4px 10px' }} onClick={onClose}>✕</button>
        </div>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '12px', marginBottom: '16px' }}>
        {[
          { label: 'Status', value: pipeline.status || '—', color: 'var(--status-healthy)' },
          { label: 'Throughput', value: pipeline.throughput || '—', color: 'var(--accent-cyan)' },
          { label: 'Uptime', value: pipeline.uptime || '—', color: 'var(--accent-purple)' },
        ].map(({ label, value, color }) => (
          <div key={label} style={{ background: 'rgba(255,255,255,0.02)', borderRadius: '8px', padding: '10px 12px' }}>
            <div style={{ fontSize: '10px', color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: '4px' }}>{label}</div>
            <div style={{ fontSize: '14px', fontWeight: 600, color }}>{value}</div>
          </div>
        ))}
      </div>
      {pipeline.source && (
        <div style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '12px', fontFamily: 'JetBrains Mono, monospace' }}>
          {pipeline.source}
        </div>
      )}
      <div style={{ fontSize: '12px', fontWeight: 600, color: 'var(--text-secondary)', marginBottom: '8px', textTransform: 'uppercase', letterSpacing: '0.5px' }}>
        Recent Activity
      </div>
      {relatedEvents.length > 0 ? relatedEvents.map((e, i) => (
        <div key={i} style={{ display: 'flex', gap: '8px', padding: '6px 0', borderBottom: '1px solid var(--glass-border)', fontSize: '12px' }}>
          <span>{EVENT_ICONS[e.event_type] || '📌'}</span>
          <span style={{ color: 'var(--text-secondary)', flex: 1 }}>
            {(e.details?.task || e.details?.request || e.event_type).slice(0, 80)}
          </span>
          <span style={{ color: 'var(--text-muted)', whiteSpace: 'nowrap' }}>{timeAgo(e.logged_at)}</span>
        </div>
      )) : (
        <div style={{ color: 'var(--text-muted)', fontSize: '12px' }}>No events recorded for this pipeline yet.</div>
      )}
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
function App() {
  const [page, setPage] = useState('dashboard')
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState(null)
  const [health, setHealth] = useState(null)
  const [events, setEvents] = useState(DEMO_EVENTS)
  const [agents, setAgents] = useState([])
  const [pipelines, setPipelines] = useState(DEMO_PIPELINES)
  const [learnings, setLearnings] = useState([])
  const [wsStatus, setWsStatus] = useState('connecting')
  const [toasts, setToasts] = useState([])
  const [schedules, setSchedules] = useState([])
  const [showScheduleForm, setShowScheduleForm] = useState(false)
  const [scheduleReq, setScheduleReq] = useState('')
  const [scheduleCron, setScheduleCron] = useState('')
  const [scheduleName, setScheduleName] = useState('')
  const [selectedPipeline, setSelectedPipeline] = useState(null)
  const [expandedEvent, setExpandedEvent] = useState(null)
  const [filterAgent, setFilterAgent] = useState('all')
  const [filterType, setFilterType] = useState('all')
  const [filterSearch, setFilterSearch] = useState('')
  const inputRef = useRef(null)
  const wsRef = useRef(null)
  const reconnectAttempts = useRef(0)
  const toastIdRef = useRef(0)

  /* ── Derived state ── */
  const agentActivity = useMemo(() => {
    const map = {}
    for (const e of events) {
      if (!map[e.agent_role] || new Date(e.logged_at) > new Date(map[e.agent_role].logged_at)) {
        map[e.agent_role] = e
      }
    }
    return map
  }, [events])

  const eventStats = useMemo(() => ({
    total: events.length,
    cacheHits: events.filter(e => e.event_type === 'CACHED_PLAN_USED').length,
    heals: events.filter(e => e.event_type === 'SCHEMA_HEALED').length,
    failures: events.filter(e => e.event_type === 'TASK_FAILED').length,
  }), [events])

  const requestHistory = useMemo(() =>
    events
      .filter(e => e.event_type === 'USER_REQUEST_RECEIVED')
      .slice(0, 10),
    [events]
  )

  const filteredEvents = useMemo(() => {
    return events.filter(e => {
      if (filterAgent !== 'all' && e.agent_role !== filterAgent) return false
      if (filterType === 'tasks' && !['TASK_COMPLETED', 'TASK_IN_PROGRESS', 'TASK_FAILED'].includes(e.event_type)) return false
      if (filterType === 'errors' && e.event_type !== 'TASK_FAILED') return false
      if (filterType === 'schema' && !['SCHEMA_DRIFT', 'SCHEMA_DRIFT_SIMULATED', 'SCHEMA_HEALED'].includes(e.event_type)) return false
      if (filterType === 'healing' && !['SCHEMA_HEALED', 'JOB_RESTARTED'].includes(e.event_type)) return false
      if (filterSearch) {
        const q = filterSearch.toLowerCase()
        const text = `${e.event_type} ${e.agent_role} ${JSON.stringify(e.details || {})}`.toLowerCase()
        if (!text.includes(q)) return false
      }
      return true
    })
  }, [events, filterAgent, filterType, filterSearch])

  /* ── Toast helpers ── */
  const addToast = useCallback((type, msg) => {
    const id = ++toastIdRef.current
    setToasts(prev => [...prev, { id, type, msg }])
    setTimeout(() => setToasts(prev => prev.filter(t => t.id !== id)), 4000)
  }, [])

  const dismissToast = useCallback((id) => {
    setToasts(prev => prev.filter(t => t.id !== id))
  }, [])

  /* ── WebSocket ── */
  const connectWebSocket = useCallback(() => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) return

    setWsStatus('connecting')
    const ws = new WebSocket(`ws://localhost:8000/ws`)
    wsRef.current = ws

    ws.onopen = () => {
      setWsStatus('live')
      reconnectAttempts.current = 0
    }

    ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data)
        if (msg.type === 'event' && msg.data) {
          const evt = msg.data
          setEvents(prev => {
            const key = `${evt.logged_at}-${evt.event_type}`
            if (prev.some(ev => `${ev.logged_at}-${ev.event_type}` === key)) return prev
            return [evt, ...prev].slice(0, 200)
          })
          const trigger = TOAST_TRIGGERS[evt.event_type]
          if (trigger) addToast(trigger.type, trigger.msg)
        } else if (msg.type === 'pipeline_update' && msg.data) {
          setPipelines(prev => {
            const idx = prev.findIndex(p => p.pipeline_id === msg.data.pipeline_id)
            if (idx >= 0) {
              const updated = [...prev]
              updated[idx] = { ...prev[idx], ...msg.data }
              return updated
            }
            return [msg.data, ...prev]
          })
        } else if (msg.type === 'request_completed') {
          fetchEvents()
          fetchPipelines()
        } else if (msg.type === 'scheduled_run_completed') {
          addToast('info', `Scheduled run completed: ${(msg.summary || '').slice(0, 60)}`)
          fetchEvents()
        }
      } catch (_) {}
    }

    ws.onclose = () => {
      const maxRetries = 5
      if (reconnectAttempts.current < maxRetries) {
        setWsStatus('reconnecting')
        const delay = Math.min(1000 * 2 ** reconnectAttempts.current, 16000)
        reconnectAttempts.current += 1
        setTimeout(connectWebSocket, delay)
      } else {
        setWsStatus('offline')
      }
    }

    ws.onerror = () => ws.close()
  }, [addToast])

  useEffect(() => {
    fetchHealth()
    fetchEvents()
    fetchAgents()
    fetchPipelines()
    fetchLearnings()
    fetchSchedules()
    connectWebSocket()

    const interval = setInterval(() => {
      fetchHealth()
      if (wsStatus === 'offline' || wsStatus === 'reconnecting') fetchEvents()
    }, 15000)

    return () => {
      clearInterval(interval)
      if (wsRef.current) wsRef.current.close()
    }
  }, [])

  const fetchHealth = async () => {
    const data = await api('/api/health')
    if (!data.error) setHealth(data)
  }

  const fetchEvents = async () => {
    const data = await api('/api/events')
    if (!data.error && data.events && data.events.length > 0) setEvents(data.events)
  }

  const fetchAgents = async () => {
    const data = await api('/api/agents')
    if (!data.error && data.agents) setAgents(data.agents)
  }

  const fetchPipelines = async () => {
    const data = await api('/api/pipelines')
    if (!data.error && data.pipelines && data.pipelines.length > 0) setPipelines(data.pipelines)
  }

  const fetchLearnings = async () => {
    const data = await api('/api/learnings')
    if (!data.error && data.learnings) setLearnings(data.learnings)
  }

  const fetchSchedules = async () => {
    const data = await api('/api/schedules')
    if (!data.error && data.schedules) setSchedules(data.schedules)
  }

  const handleCreateSchedule = async (e) => {
    e.preventDefault()
    if (!scheduleReq.trim() || !scheduleCron.trim()) return
    const data = await api('/api/schedules', {
      method: 'POST',
      body: JSON.stringify({ request: scheduleReq, cron_expression: scheduleCron, name: scheduleName }),
    })
    if (data.error) {
      addToast('error', `Failed to create schedule: ${data.detail || data.error}`)
    } else {
      addToast('success', `Schedule created: ${scheduleCron}`)
      setSchedules(prev => [...prev, data])
      setShowScheduleForm(false)
      setScheduleReq('')
      setScheduleCron('')
      setScheduleName('')
    }
  }

  const handleDeleteSchedule = async (scheduleId) => {
    const data = await api(`/api/schedules/${scheduleId}`, { method: 'DELETE' })
    if (!data.error) {
      setSchedules(prev => prev.filter(s => s.schedule_id !== scheduleId))
      addToast('info', 'Schedule removed')
    }
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
    await api('/api/generate-data', { method: 'POST', body: JSON.stringify({ target, count: 50 }) })
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
    addToast('warning', 'Schema drift simulation triggered')
  }

  const agentDefs = [
    { name: 'Orchestrator', icon: '🧠', color: 'var(--agent-orchestrator)', role: 'orchestrator' },
    { name: 'Builder', icon: '🔨', color: 'var(--agent-builder)', role: 'builder' },
    { name: 'Monitor', icon: '👁', color: 'var(--agent-monitor)', role: 'monitor' },
    { name: 'Healer', icon: '🩺', color: 'var(--agent-healer)', role: 'healer' },
    { name: 'Optimizer', icon: '⚡', color: 'var(--agent-optimizer)', role: 'optimizer' },
    { name: 'Quality', icon: '✅', color: 'var(--agent-quality)', role: 'quality' },
  ]

  const overallStatus = health?.overall_status || 'unknown'
  const statusColor = { healthy: 'var(--status-healthy)', degraded: 'var(--status-degraded)', critical: 'var(--status-critical)' }

  /* ── MCP data from health ── */
  const mcpCards = useMemo(() => {
    const services = health?.services || {}
    return [
      { name: 'Kafka', icon: '📨', healthy: services.kafka?.status === 'healthy', detail: services.kafka?.topic_count != null ? `${services.kafka.topic_count} topics` : 'Streaming backbone' },
      { name: 'Flink', icon: '⚡', healthy: services.flink?.status === 'healthy', detail: services.flink?.running_jobs != null ? `${services.flink.running_jobs} jobs running` : 'Stream processor' },
      { name: 'Iceberg', icon: '🧊', healthy: services.iceberg?.status === 'healthy', detail: 'REST catalog' },
      { name: 'PostgreSQL', icon: '🐘', healthy: services.postgres?.status === 'healthy', detail: 'CDC source (WAL)' },
      { name: 'MongoDB', icon: '🍃', healthy: services.mongodb?.status === 'healthy', detail: 'Event store' },
      { name: 'Redis', icon: '🔴', healthy: services.redis?.status === 'healthy', detail: 'Agent memory' },
    ]
  }, [health])

  const eventSeverity = (type) => {
    if (['TASK_FAILED'].includes(type)) return 'error'
    if (['SCHEMA_DRIFT', 'ANOMALY_DETECTED', 'SCHEMA_DRIFT_SIMULATED'].includes(type)) return 'warning'
    if (['SCHEMA_HEALED', 'REQUEST_COMPLETED'].includes(type)) return 'success'
    return 'default'
  }

  return (
    <div className="app-layout">
      <ToastContainer toasts={toasts} onDismiss={dismissToast} />
      <Sidebar active={page} onNavigate={setPage} />

      <main className="main-content">
        <header className="header">
          <form className="nl-input-container" onSubmit={handleSubmit}>
            <input
              ref={inputRef}
              className="nl-input"
              type="text"
              placeholder="Describe your pipeline… e.g. 'Stream orders from Postgres, aggregate by region every 5 min'"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              disabled={loading}
            />
            <button className="nl-submit" type="submit" disabled={loading || !input.trim()}>
              {loading ? <span className="loading-spinner"></span> : 'Submit'}
            </button>
          </form>
          <div className="header-status" style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
            <span style={{ display: 'flex', alignItems: 'center', gap: '5px', fontSize: '12px', color: 'var(--text-muted)' }}>
              <span style={{
                width: '7px', height: '7px', borderRadius: '50%', display: 'inline-block',
                background: wsStatus === 'live' ? 'var(--status-healthy)' : wsStatus === 'reconnecting' ? 'var(--status-degraded)' : 'var(--text-muted)',
              }} />
              {wsStatus === 'live' ? 'Live' : wsStatus === 'reconnecting' ? 'Reconnecting…' : 'Offline'}
            </span>
            <span className="status-dot" style={{ background: statusColor[overallStatus] || 'var(--text-muted)' }}></span>
            {agents.length || 6} agents online
          </div>
        </header>

        <div className="dashboard-content">
          {loading && (
            <div className="loading-overlay">
              <span className="loading-spinner"></span>
              Agents are processing your request…
            </div>
          )}
          {result && <ResultPanel result={result} onClose={() => setResult(null)} />}

          {/* ── Dashboard ── */}
          {page === 'dashboard' && (
            <>
              <div className="card full-width">
                <div className="card-header">
                  <span className="card-title">Active Agents</span>
                  <span className="card-badge">{agentDefs.length} online</span>
                </div>
                <div className="agents-grid">
                  {agentDefs.map(a => {
                    const latest = agentActivity[a.role]
                    const busy = latest?.event_type === 'TASK_IN_PROGRESS'
                    const taskLabel = latest
                      ? `${latest.event_type.replace(/_/g, ' ').toLowerCase()} · ${timeAgo(latest.logged_at)}`
                      : ''
                    return <AgentCard key={a.name} {...a} status="active" task={taskLabel} busy={busy} />
                  })}
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
                    <div key={i} className="pipeline-row pipeline-clickable" onClick={() => setSelectedPipeline(p)}>
                      <div className="pipeline-info">
                        <div className="pipeline-name">{p.pipeline_id}</div>
                        {p.source && <div style={{ color: 'var(--text-muted)', fontSize: '11px' }}>{p.source}</div>}
                        <div style={{ display: 'flex', gap: '12px', marginTop: '4px', fontSize: '11px' }}>
                          {p.throughput && <span style={{ color: 'var(--accent-cyan)' }}>⚡ {p.throughput}</span>}
                          {p.uptime && <span style={{ color: 'var(--status-healthy)' }}>⏱ {p.uptime}</span>}
                        </div>
                      </div>
                      <span style={{
                        fontSize: '11px', padding: '2px 8px', borderRadius: '12px',
                        background: p.status === 'running' ? 'rgba(16,185,129,0.15)' : 'rgba(245,158,11,0.15)',
                        color: p.status === 'running' ? 'var(--status-healthy)' : 'var(--status-degraded)',
                        fontWeight: 600,
                      }}>
                        {p.status === 'running' ? '● Running' : p.status}
                      </span>
                    </div>
                  )) : (
                    <div style={{ color: 'var(--text-muted)', fontSize: '13px', padding: '16px 0' }}>
                      No pipelines yet. Send a request to create one!
                    </div>
                  )}
                  <div style={{ marginTop: '8px', fontSize: '11px', color: 'var(--text-muted)' }}>Click a pipeline for details</div>
                </div>

                {/* Activity Feed */}
                <div className="card">
                  <div className="card-header">
                    <span className="card-title">Agent Activity Feed</span>
                    <span className="card-badge">{events.length} events</span>
                  </div>
                  <div className="activity-list">
                    {events.slice(0, 15).map((evt, i) => (
                      <div key={i} className="activity-item">
                        <span className="activity-icon">{EVENT_ICONS[evt.event_type] || '📌'}</span>
                        <div className="activity-content">
                          <div className="activity-text">
                            <strong style={{ color: AGENT_COLORS[evt.agent_role] || 'var(--accent-cyan)' }}>{evt.agent_role}</strong>{' '}
                            {evt.event_type?.replace(/_/g, ' ').toLowerCase()}
                            {evt.details?.request && `: "${evt.details.request.slice(0, 50)}…"`}
                            {evt.details?.task && `: ${evt.details.task.slice(0, 50)}`}
                          </div>
                          <div className="activity-time">{timeAgo(evt.logged_at)}</div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>

              {/* Pipeline Detail Panel */}
              {selectedPipeline && (
                <PipelineDetail
                  pipeline={selectedPipeline}
                  events={events}
                  onClose={() => setSelectedPipeline(null)}
                  onSchemaDrift={handleSchemaDrift}
                />
              )}

              {/* Request History */}
              {requestHistory.length > 0 && (
                <div className="card full-width">
                  <div className="card-header">
                    <span className="card-title">Recent Requests</span>
                    <span className="card-badge">{requestHistory.length} requests</span>
                  </div>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                    {requestHistory.map((r, i) => (
                      <div key={i} style={{ display: 'flex', alignItems: 'center', gap: '12px', padding: '8px 10px', background: 'rgba(255,255,255,0.02)', borderRadius: '8px' }}>
                        <span style={{ fontSize: '13px', color: 'var(--text-secondary)', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                          {r.details?.request || '—'}
                        </span>
                        <span style={{ fontSize: '11px', color: 'var(--text-muted)', whiteSpace: 'nowrap' }}>{timeAgo(r.logged_at)}</span>
                        <button className="btn btn-ghost" style={{ fontSize: '11px', padding: '3px 8px', whiteSpace: 'nowrap' }}
                          onClick={() => setInput(r.details?.request || '')}>
                          ↩ Re-run
                        </button>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Quick Actions */}
              <div className="card full-width">
                <div className="card-header"><span className="card-title">Quick Actions</span></div>
                <div style={{ display: 'flex', gap: '10px', flexWrap: 'wrap' }}>
                  <button className="btn btn-primary" onClick={() => handleGenerateData('kafka')}>📨 Generate Kafka Events</button>
                  <button className="btn btn-primary" onClick={() => handleGenerateData('postgres')}>🐘 Insert PostgreSQL Orders</button>
                  <button className="btn btn-danger" onClick={handleSchemaDrift}>🧪 Simulate Schema Drift</button>
                  <button className="btn btn-ghost" onClick={fetchHealth}>🏥 Health Check</button>
                </div>
              </div>
            </>
          )}

          {/* ── Pipelines ── */}
          {page === 'pipelines' && (
            <>
              <div className="card full-width">
                <div className="card-header">
                  <span className="card-title">All Pipelines</span>
                  <span className="card-badge">{pipelines.length} active</span>
                </div>
                {pipelines.length > 0 ? pipelines.map((p, i) => {
                  const pSched = schedules.find(s =>
                    (s.name || '').toLowerCase().includes((p.pipeline_id || '').split('-')[0]) ||
                    (s.request || '').toLowerCase().includes((p.pipeline_id || '').split('-')[0])
                  )
                  return (
                    <div key={i} className="pipeline-row pipeline-clickable" onClick={() => setSelectedPipeline(p)}>
                      <div className="pipeline-info">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <div className="pipeline-name">{p.pipeline_id}</div>
                          {pSched && <span className="cron-badge">⏰ {pSched.cron_expression}</span>}
                        </div>
                        {p.source && <div style={{ color: 'var(--text-muted)', fontSize: '12px' }}>{p.source}</div>}
                        <div style={{ display: 'flex', gap: '12px', marginTop: '6px', fontSize: '12px' }}>
                          {p.throughput && <span style={{ color: 'var(--accent-cyan)' }}>⚡ {p.throughput}</span>}
                          {p.uptime && <span style={{ color: 'var(--status-healthy)' }}>⏱ Uptime: {p.uptime}</span>}
                          {p.tasks_completed && <span style={{ color: 'var(--accent-purple)' }}>✓ {p.tasks_completed} tasks</span>}
                        </div>
                      </div>
                      <span style={{
                        fontSize: '12px', padding: '3px 10px', borderRadius: '12px',
                        background: p.status === 'running' ? 'rgba(16,185,129,0.15)' : 'rgba(245,158,11,0.15)',
                        color: p.status === 'running' ? 'var(--status-healthy)' : 'var(--status-degraded)',
                        fontWeight: 600,
                      }}>
                        {p.status === 'running' ? '● Running' : p.status}
                      </span>
                    </div>
                  )
                }) : (
                  <div style={{ color: 'var(--text-muted)', fontSize: '14px', padding: '24px 0', textAlign: 'center' }}>
                    No pipelines yet.<br />
                    <span style={{ fontSize: '12px' }}>Send a natural language request to create your first pipeline.</span>
                  </div>
                )}
                {selectedPipeline && (
                  <PipelineDetail pipeline={selectedPipeline} events={events} onClose={() => setSelectedPipeline(null)} onSchemaDrift={handleSchemaDrift} />
                )}
              </div>

              {/* Schedules Section */}
              <div className="card full-width">
                <div className="card-header">
                  <span className="card-title">Scheduled Pipelines</span>
                  <span className="card-badge">{schedules.length} schedules</span>
                  <button className="btn btn-primary" style={{ marginLeft: 'auto', fontSize: '12px' }}
                    onClick={() => setShowScheduleForm(s => !s)}>
                    {showScheduleForm ? '✕ Cancel' : '+ Add Schedule'}
                  </button>
                </div>

                {showScheduleForm && (
                  <form className="schedule-form" onSubmit={handleCreateSchedule}>
                    <input
                      className="filter-input"
                      type="text"
                      placeholder="Pipeline request (e.g. calculate hourly revenue totals)"
                      value={scheduleReq}
                      onChange={e => setScheduleReq(e.target.value)}
                      required
                    />
                    <input
                      className="filter-input"
                      type="text"
                      placeholder="Cron expression (e.g. 0 * * * * = every hour)"
                      value={scheduleCron}
                      onChange={e => setScheduleCron(e.target.value)}
                      required
                    />
                    <input
                      className="filter-input"
                      type="text"
                      placeholder="Name (optional)"
                      value={scheduleName}
                      onChange={e => setScheduleName(e.target.value)}
                    />
                    <button className="btn btn-primary" type="submit">Create Schedule</button>
                    <div style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '6px' }}>
                      Cron format: <code>min hour day month weekday</code> — e.g. <code>0 * * * *</code> = every hour, <code>*/5 * * * *</code> = every 5 min
                    </div>
                  </form>
                )}

                {schedules.length === 0 ? (
                  <div style={{ color: 'var(--text-muted)', fontSize: '13px', padding: '20px 0', textAlign: 'center' }}>
                    No scheduled pipelines yet. Click "Add Schedule" to create one.
                  </div>
                ) : (
                  <div className="schedule-list">
                    {schedules.map(s => (
                      <div key={s.schedule_id} className="schedule-row">
                        <div style={{ flex: 1 }}>
                          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                            <span className="cron-badge">⏰ {s.cron_expression}</span>
                            {s.name && <span style={{ fontSize: '13px', fontWeight: 600, color: 'var(--text-primary)' }}>{s.name}</span>}
                          </div>
                          <div style={{ fontSize: '12px', color: 'var(--text-secondary)', marginTop: '4px' }}>{s.request}</div>
                          <div style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '2px' }}>
                            Created {s.created_at ? timeAgo(s.created_at) : '—'} · ID: {s.schedule_id}
                          </div>
                        </div>
                        <button className="btn btn-danger" style={{ fontSize: '11px', padding: '4px 10px' }}
                          onClick={() => handleDeleteSchedule(s.schedule_id)}>
                          Remove
                        </button>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </>
          )}

          {/* ── Agents ── */}
          {page === 'agents' && (
            <div className="card full-width">
              <div className="card-header"><span className="card-title">Agent Registry</span></div>
              <div className="agents-grid">
                {agentDefs.map(a => {
                  const latest = agentActivity[a.role]
                  const busy = latest?.event_type === 'TASK_IN_PROGRESS'
                  const taskLabel = latest
                    ? `${latest.event_type.replace(/_/g, ' ').toLowerCase()} · ${timeAgo(latest.logged_at)}`
                    : ''
                  return <AgentCard key={a.name} {...a} status="active" task={taskLabel} busy={busy} />
                })}
              </div>
            </div>
          )}

          {/* ── Events ── */}
          {page === 'events' && (
            <div className="card full-width">
              {/* Stats Bar */}
              <div className="stats-bar">
                {[
                  { label: 'Total Events', value: eventStats.total, color: 'var(--accent-cyan)' },
                  { label: 'Cache Hits', value: eventStats.cacheHits, color: 'var(--accent-purple)' },
                  { label: 'Schema Heals', value: eventStats.heals, color: 'var(--status-healthy)' },
                  { label: 'Failures', value: eventStats.failures, color: 'var(--status-critical)' },
                ].map(({ label, value, color }) => (
                  <div key={label} className="stat-pill">
                    <span style={{ fontSize: '18px', fontWeight: 700, color }}>{value}</span>
                    <span style={{ fontSize: '11px', color: 'var(--text-muted)' }}>{label}</span>
                  </div>
                ))}
              </div>

              {/* Filter Bar */}
              <div className="filter-bar">
                <input
                  className="filter-input"
                  type="text"
                  placeholder="Search events…"
                  value={filterSearch}
                  onChange={e => setFilterSearch(e.target.value)}
                />
                <select className="filter-select" value={filterAgent} onChange={e => setFilterAgent(e.target.value)}>
                  <option value="all">All Agents</option>
                  {['orchestrator', 'builder', 'monitor', 'healer', 'optimizer', 'quality'].map(a => (
                    <option key={a} value={a}>{a.charAt(0).toUpperCase() + a.slice(1)}</option>
                  ))}
                </select>
                <div className="filter-pills">
                  {[
                    { id: 'all', label: 'All' },
                    { id: 'tasks', label: 'Tasks' },
                    { id: 'errors', label: 'Errors' },
                    { id: 'schema', label: 'Schema' },
                    { id: 'healing', label: 'Healing' },
                  ].map(({ id, label }) => (
                    <button
                      key={id}
                      className={`filter-pill ${filterType === id ? 'active' : ''}`}
                      onClick={() => setFilterType(id)}
                    >
                      {label}
                    </button>
                  ))}
                </div>
                {(filterAgent !== 'all' || filterType !== 'all' || filterSearch) && (
                  <button className="btn btn-ghost" style={{ fontSize: '11px', padding: '4px 10px' }}
                    onClick={() => { setFilterAgent('all'); setFilterType('all'); setFilterSearch('') }}>
                    Clear
                  </button>
                )}
                <button className="btn btn-ghost" style={{ marginLeft: 'auto', fontSize: '12px' }} onClick={fetchEvents}>Refresh</button>
              </div>

              <div style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '8px' }}>
                Showing {filteredEvents.length} of {events.length} events
              </div>

              <div className="activity-list" style={{ maxHeight: '600px' }}>
                {filteredEvents.map((evt, i) => {
                  const sev = eventSeverity(evt.event_type)
                  const isExpanded = expandedEvent === i
                  return (
                    <div
                      key={i}
                      className={`activity-item event-expandable event-${sev}`}
                      onClick={() => setExpandedEvent(isExpanded ? null : i)}
                    >
                      <span className="activity-icon">{EVENT_ICONS[evt.event_type] || '📌'}</span>
                      <div className="activity-content" style={{ flex: 1 }}>
                        <div className="activity-text">
                          <strong style={{ color: AGENT_COLORS[evt.agent_role] || 'var(--accent-cyan)' }}>{evt.agent_role}</strong>{' '}
                          <span style={{ color: 'var(--text-secondary)' }}>{evt.event_type?.replace(/_/g, ' ').toLowerCase()}</span>
                        </div>
                        <div className="activity-time">{evt.logged_at ? new Date(evt.logged_at).toLocaleString() : ''}</div>
                        {isExpanded && (
                          <div className="event-detail">
                            {evt.details && (
                              <pre style={{ fontSize: '11px', color: 'var(--text-secondary)', whiteSpace: 'pre-wrap', marginBottom: '4px' }}>
                                {JSON.stringify(evt.details, null, 2)}
                              </pre>
                            )}
                            {evt.pipeline_id && <div style={{ fontSize: '11px', color: 'var(--accent-cyan)' }}>Pipeline: {evt.pipeline_id}</div>}
                            {evt.agent_id && <div style={{ fontSize: '11px', color: 'var(--text-muted)' }}>Agent ID: {evt.agent_id}</div>}
                          </div>
                        )}
                      </div>
                      <span style={{ fontSize: '11px', color: 'var(--text-muted)', marginLeft: '8px' }}>{isExpanded ? '▲' : '▼'}</span>
                    </div>
                  )
                })}
              </div>
            </div>
          )}

          {/* ── Databases ── */}
          {page === 'databases' && (
            <div className="card full-width">
              <div className="card-header">
                <span className="card-title">Connected Services</span>
                <span className="card-badge" style={{ color: overallStatus === 'healthy' ? 'var(--status-healthy)' : 'var(--status-degraded)' }}>
                  {overallStatus}
                </span>
                <button className="btn btn-ghost" onClick={fetchHealth} style={{ marginLeft: 'auto', fontSize: '12px' }}>Refresh</button>
              </div>
              <div className="mcp-grid">
                {mcpCards.map(s => <McpCard key={s.name} {...s} />)}
              </div>
            </div>
          )}

          {/* ── Learnings ── */}
          {page === 'learnings' && (
            <div className="card full-width">
              <div className="card-header">
                <span className="card-title">Agent Learnings</span>
                <span className="card-badge">{learnings.length} patterns</span>
                <button className="btn btn-ghost" onClick={fetchLearnings} style={{ marginLeft: 'auto' }}>Refresh</button>
              </div>
              {learnings.length === 0 ? (
                <div style={{ color: 'var(--text-muted)', fontSize: '13px', padding: '24px 0', textAlign: 'center' }}>
                  No learned patterns yet.<br />
                  <span style={{ fontSize: '12px' }}>Simulate schema drift and let the Healer fix it to seed the first pattern.</span>
                </div>
              ) : (
                <div style={{ overflowX: 'auto' }}>
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '13px' }}>
                    <thead>
                      <tr style={{ borderBottom: '1px solid var(--glass-border)', color: 'var(--text-muted)', textAlign: 'left' }}>
                        <th style={{ padding: '8px 12px' }}>Incident Type</th>
                        <th style={{ padding: '8px 12px' }}>Resolution</th>
                        <th style={{ padding: '8px 12px' }}>Successes</th>
                        <th style={{ padding: '8px 12px' }}>Success Rate</th>
                        <th style={{ padding: '8px 12px' }}>Last Used</th>
                      </tr>
                    </thead>
                    <tbody>
                      {learnings.map((l, i) => (
                        <tr key={i} style={{ borderBottom: '1px solid var(--glass-border)' }}>
                          <td style={{ padding: '10px 12px' }}>
                            <span style={{ padding: '2px 8px', borderRadius: '12px', background: 'rgba(99,102,241,0.15)', color: 'var(--accent-purple)', fontWeight: 600, fontSize: '11px' }}>
                              {l.incident_type}
                            </span>
                          </td>
                          <td style={{ padding: '10px 12px', color: 'var(--text-secondary)', maxWidth: '300px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                            {l.resolution_summary}
                          </td>
                          <td style={{ padding: '10px 12px', color: 'var(--status-healthy)', fontWeight: 600 }}>
                            {l.success_count}
                            {l.failure_count > 0 && <span style={{ color: 'var(--status-critical)', fontWeight: 400 }}> / {l.failure_count} fail</span>}
                          </td>
                          <td style={{ padding: '10px 12px' }}>
                            <span style={{ color: l.success_rate >= 0.8 ? 'var(--status-healthy)' : l.success_rate >= 0.5 ? 'var(--status-degraded)' : 'var(--status-critical)' }}>
                              {Math.round(l.success_rate * 100)}%
                            </span>
                          </td>
                          <td style={{ padding: '10px 12px', color: 'var(--text-muted)', fontSize: '12px' }}>
                            {l.last_used ? timeAgo(l.last_used) : '—'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}

          {/* ── Roadmap ── */}
          {page === 'roadmap' && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
              {[
                {
                  phase: 'Sprint 1–2 · Near-term',
                  color: 'var(--accent-cyan)',
                  items: [
                    { icon: '⏰', title: 'Scheduled pipelines', desc: 'Add cron expression to pipeline creation — Flink periodic jobs' },
                    { icon: '🔔', title: 'Alert rules', desc: 'Configure thresholds (e.g. Kafka lag > 10k → notify via toast/webhook)' },
                    { icon: '📜', title: 'Pipeline versioning', desc: 'Track Flink SQL history, enable rollback to previous job version' },
                    { icon: '🔐', title: 'Auth', desc: 'JWT authentication on API so multiple users can share the dashboard safely' },
                  ],
                },
                {
                  phase: 'Sprint 3–4 · Medium-term',
                  color: 'var(--accent-purple)',
                  items: [
                    { icon: '☁️', title: 'Cloud deployment', desc: 'FastAPI → Railway or Fly.io; dashboard already live on Vercel' },
                    { icon: '📈', title: 'Metrics store', desc: 'Pipe Flink metrics to Prometheus + Grafana for time-series graphs' },
                    { icon: '🔗', title: 'Multi-pipeline orchestration', desc: 'Let agents chain pipelines with declared dependencies' },
                    { icon: '💰', title: 'Cost tracking', desc: 'Count LLM tokens per request, show cost per request in history' },
                  ],
                },
                {
                  phase: 'Long-term',
                  color: 'var(--accent-orange)',
                  items: [
                    { icon: '🧩', title: 'Agent marketplace', desc: 'User-defined agent skills via skills/ directory — plug-and-play capabilities' },
                    { icon: '🗣️', title: 'Natural language SLA rules', desc: '"Alert me if orders lag exceeds 30 seconds" → auto-compiled threshold' },
                    { icon: '🧪', title: 'Data quality rules', desc: 'User-defined expectations (Great Expectations style) enforced by Quality agent' },
                    { icon: '👥', title: 'Multi-tenant', desc: 'Separate namespaces per team, RBAC, shared infrastructure' },
                  ],
                },
              ].map(({ phase, color, items }) => (
                <div key={phase} className="card full-width">
                  <div className="card-header">
                    <span className="card-title" style={{ color }}>{phase}</span>
                  </div>
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))', gap: '12px' }}>
                    {items.map(({ icon, title, desc }) => (
                      <div key={title} style={{ background: 'rgba(255,255,255,0.02)', borderRadius: '10px', padding: '14px 16px', border: '1px solid var(--glass-border)' }}>
                        <div style={{ fontSize: '20px', marginBottom: '6px' }}>{icon}</div>
                        <div style={{ fontSize: '14px', fontWeight: 600, color: 'var(--text-primary)', marginBottom: '4px' }}>{title}</div>
                        <div style={{ fontSize: '12px', color: 'var(--text-muted)', lineHeight: '1.5' }}>{desc}</div>
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </main>
    </div>
  )
}

export default App
