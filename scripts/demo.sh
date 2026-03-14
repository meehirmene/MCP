#!/usr/bin/env bash
# FlowForge Demo Bootstrap
# Usage: ./scripts/demo.sh
# One command to start the full stack, seed data, and launch the API server.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info()    { echo -e "${BLUE}[demo]${NC} $*"; }
success() { echo -e "${GREEN}[demo]${NC} $*"; }
warn()    { echo -e "${YELLOW}[demo]${NC} $*"; }
die()     { echo -e "${RED}[demo] ERROR:${NC} $*" >&2; exit 1; }

# ─────────────────────────────────────────────
# Step 1: Environment
# ─────────────────────────────────────────────
if [ ! -f ".env" ]; then
  info "Creating .env from .env.example..."
  cp .env.example .env
  warn "Set your LLM_API_KEY in .env before continuing."
  warn "  nano .env"
  read -rp "Press Enter when ready, or Ctrl-C to abort..." _
fi

# ─────────────────────────────────────────────
# Step 2: Start Docker Compose
# ─────────────────────────────────────────────
info "Starting Docker Compose services..."
docker compose up -d

# ─────────────────────────────────────────────
# Step 3: Wait for core services to be healthy
# ─────────────────────────────────────────────
wait_healthy() {
  local service="$1"
  local max_wait="${2:-90}"
  local elapsed=0
  info "Waiting for $service to be healthy..."
  while [ "$elapsed" -lt "$max_wait" ]; do
    status=$(docker inspect --format='{{.State.Health.Status}}' "flowforge-$service" 2>/dev/null || echo "starting")
    if [ "$status" = "healthy" ]; then
      success "$service is healthy"
      return 0
    fi
    sleep 3
    elapsed=$((elapsed + 3))
  done
  warn "$service did not become healthy within ${max_wait}s — continuing anyway"
}

wait_healthy postgres 60
wait_healthy redis 30
wait_healthy kafka 90
wait_healthy flink-jobmanager 60
wait_healthy flink-sql-gateway 90

# ─────────────────────────────────────────────
# Step 4: Install Python package
# ─────────────────────────────────────────────
info "Installing FlowForge Python package..."
pip install -e . -q

# ─────────────────────────────────────────────
# Step 5: Seed demo data
# ─────────────────────────────────────────────
info "Seeding demo data..."
python scripts/seed_demo.py

# ─────────────────────────────────────────────
# Step 6: Start API server (background)
# ─────────────────────────────────────────────
info "Starting FlowForge API server on :8000..."
uvicorn flowforge.api_server:app --host 0.0.0.0 --port 8000 --reload &
API_PID=$!
echo "$API_PID" > /tmp/flowforge-api.pid

sleep 2
if ! kill -0 "$API_PID" 2>/dev/null; then
  die "API server failed to start. Check logs above."
fi
success "API server running (PID $API_PID)"

# ─────────────────────────────────────────────
# Step 7: Start dashboard (background)
# ─────────────────────────────────────────────
if [ -d "dashboard" ] && [ -f "dashboard/package.json" ]; then
  info "Starting dashboard on :5173..."
  (cd dashboard && npm run dev &)
  sleep 2
fi

# ─────────────────────────────────────────────
# Done
# ─────────────────────────────────────────────
echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}  FlowForge Demo Running!${NC}"
echo -e "${GREEN}============================================================${NC}"
echo ""
echo "  Dashboard   : http://localhost:5173"
echo "  API server  : http://localhost:8000"
echo "  API docs    : http://localhost:8000/docs"
echo "  Flink UI    : http://localhost:8081"
echo "  MinIO       : http://localhost:9001  (admin / password123)"
echo ""
echo "Try these commands:"
echo ""
echo '  curl -X POST http://localhost:8000/api/request \'
echo '    -H "Content-Type: application/json" \'
echo '    -d '"'"'{"request": "stream orders from postgres to iceberg, aggregate by region every 5 minutes"}'"'"
echo ""
echo '  curl -X POST http://localhost:8000/api/simulate/schema-drift'
echo ""
echo "  curl http://localhost:8000/api/health | python -m json.tool"
echo ""
echo "Stop API server: kill \$(cat /tmp/flowforge-api.pid)"
echo "Stop all:        docker compose down"
echo ""
