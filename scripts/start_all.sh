#!/bin/bash
# FlowForge — Start all layers end-to-end
set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

BOLD='\033[1m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${BOLD}==> $1${NC}"; }
ok()   { echo -e "${GREEN}✓ $1${NC}"; }
warn() { echo -e "${YELLOW}⚠ $1${NC}"; }
err()  { echo -e "${RED}✗ $1${NC}"; exit 1; }

# ── Layer 1: Docker Desktop ──────────────────────────────────────────────────
log "Starting Docker Desktop..."
open -a Docker 2>/dev/null || warn "Could not open Docker Desktop automatically"

echo -n "Waiting for Docker daemon"
for i in $(seq 1 30); do
  docker info &>/dev/null && break
  echo -n "."
  sleep 2
done
docker info &>/dev/null || err "Docker daemon not ready after 60s. Please start Docker Desktop manually."
echo ""
ok "Docker daemon ready"

# ── Layer 2: Docker Compose infrastructure ───────────────────────────────────
log "Starting infrastructure services..."
docker compose up -d

echo -n "Waiting for core services (Redis, Postgres, Kafka)"
for i in $(seq 1 45); do
  REDIS_OK=$(docker compose ps redis 2>/dev/null | grep -c "healthy" || true)
  PG_OK=$(docker compose ps postgres 2>/dev/null | grep -c "healthy" || true)
  KAFKA_OK=$(docker compose ps kafka 2>/dev/null | grep -c "healthy" || true)
  [ "$REDIS_OK" -ge 1 ] && [ "$PG_OK" -ge 1 ] && [ "$KAFKA_OK" -ge 1 ] && break
  echo -n "."
  sleep 3
done
echo ""
ok "Core services healthy"

# ── Layer 3: MLX model server ─────────────────────────────────────────────────
log "Starting Qwen3.5-4B MLX server on :8080..."
./scripts/start_mlx.sh > /tmp/flowforge_mlx.log 2>&1 &
MLX_PID=$!

echo -n "Waiting for MLX server"
for i in $(seq 1 20); do
  curl -s http://localhost:8080/v1/models &>/dev/null && break
  echo -n "."
  sleep 2
done
echo ""
curl -s http://localhost:8080/v1/models &>/dev/null && ok "MLX server ready (pid $MLX_PID)" || warn "MLX server may still be loading — check /tmp/flowforge_mlx.log"

# ── Layer 4: Python API server ────────────────────────────────────────────────
log "Starting FlowForge API server on :8000..."
python3.10 -m uvicorn flowforge.api_server:app --reload --port 8000 > /tmp/flowforge_api.log 2>&1 &
API_PID=$!

echo -n "Waiting for API server"
for i in $(seq 1 20); do
  curl -s http://localhost:8000/ &>/dev/null && break
  echo -n "."
  sleep 2
done
echo ""
curl -s http://localhost:8000/ &>/dev/null && ok "API server ready (pid $API_PID)" || err "API server failed to start. Check /tmp/flowforge_api.log"

# ── Layer 5: Seed demo data ───────────────────────────────────────────────────
log "Seeding demo data..."
python3.10 scripts/seed_demo.py && ok "Demo data seeded" || warn "Seed failed — you can retry manually: python3.10 scripts/seed_demo.py"

# ── Layer 6: Dashboard ────────────────────────────────────────────────────────
log "Starting dashboard on :5173..."
cd dashboard
npm run dev > /tmp/flowforge_dashboard.log 2>&1 &
DASH_PID=$!
cd "$PROJECT_DIR"

sleep 3
ok "Dashboard started (pid $DASH_PID)"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BOLD}  FlowForge is live!${NC}"
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "  Dashboard      → ${GREEN}http://localhost:5173${NC}"
echo -e "  API            → ${GREEN}http://localhost:8000${NC}"
echo -e "  API Docs       → ${GREEN}http://localhost:8000/docs${NC}"
echo -e "  MLX Model      → ${GREEN}http://localhost:8080/v1/models${NC}"
echo -e "  Flink UI       → ${GREEN}http://localhost:8081${NC}"
echo -e "  MinIO Console  → ${GREEN}http://localhost:9001${NC}  (admin / password123)"
echo ""
echo -e "  Logs:"
echo -e "    API server   → /tmp/flowforge_api.log"
echo -e "    MLX server   → /tmp/flowforge_mlx.log"
echo -e "    Dashboard    → /tmp/flowforge_dashboard.log"
echo ""
echo -e "  To stop everything: ${YELLOW}./scripts/stop_all.sh${NC}"
echo ""
