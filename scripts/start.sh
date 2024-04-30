export PATH="$PATH:$HOME/.local/bin"

poetry run uvicorn fleecekmbackend.main:app --host 0.0.0.0 --port 12346