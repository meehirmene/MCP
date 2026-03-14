#!/bin/bash
# Start the local MLX model server (Qwen3.5-4B) on port 8080

MODEL_PATH="/Users/sindhujarao/Library/Containers/app.locallyai.Locally/Data/Library/app.locallyai.Locally/huggingface/models/mlx-community/Qwen3.5-4B-4bit"
PORT=8080

# Kill any existing server on this port
if lsof -ti:$PORT &>/dev/null; then
  echo "Stopping existing server on port $PORT..."
  kill $(lsof -ti:$PORT)
  sleep 1
fi

echo "Starting Qwen3.5-4B MLX server on port $PORT..."
exec python3.10 -m mlx_lm.server --model "$MODEL_PATH" --port $PORT
