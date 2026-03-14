#!/bin/bash
# FlowForge — Stop all layers cleanly

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

BOLD='\033[1m'
GREEN='\033[0;32m'
NC='\033[0m'

log() { echo -e "${BOLD}==> $1${NC}"; }
ok()  { echo -e "${GREEN}✓ $1${NC}"; }

log "Stopping FlowForge..."

# Kill API server
if lsof -ti:8000 &>/dev/null; then
  kill $(lsof -ti:8000) 2>/dev/null && ok "API server stopped"
fi

# Kill MLX server
if lsof -ti:8080 &>/dev/null; then
  kill $(lsof -ti:8080) 2>/dev/null && ok "MLX server stopped"
fi

# Kill dashboard dev server
if lsof -ti:5173 &>/dev/null; then
  kill $(lsof -ti:5173) 2>/dev/null && ok "Dashboard stopped"
fi

# Stop Docker services (keeps data volumes intact)
log "Stopping Docker services (data volumes preserved)..."
docker compose stop && ok "Docker services stopped"

echo ""
echo "All services stopped. Run ./scripts/start_all.sh to restart."
