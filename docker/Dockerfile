FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:0.11.15 /uv /uvx /bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1

WORKDIR /app

# Install locked runtime dependencies first to maximize Docker layer cache reuse.
COPY pyproject.toml uv.lock README.md .
RUN uv sync --locked --no-install-project --no-dev --no-editable

# Copy project source.
COPY . .
RUN uv sync --locked --no-dev --no-editable

# Ensure runtime folders exist when container starts.
RUN mkdir -p logs data/raw data/processed

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "-m", "pipelines.pipeline"]
