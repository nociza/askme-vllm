export PATH="$PATH:$HOME/.local/bin"

poetry run uvicorn fleecekmbackend.server:app --host 0.0.0.0 --port 12345