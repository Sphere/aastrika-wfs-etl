FROM python:3.10-slim

WORKDIR /app

# Install UV package manager
RUN pip install uv

# Copy dependency files first (better caching)
COPY pyproject.toml uv.lock README.md ./

# Copy source code
COPY src/ ./src/

# Install dependencies
RUN uv sync --frozen

# Create logs directory
RUN mkdir -p /app/logs

# Run ETL pipeline
CMD ["uv", "run", "python", "-m", "aastrika_telemetry"]
