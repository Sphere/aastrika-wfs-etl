# Aastrika Telemetry ETL Service

ETL pipeline for extracting telemetry data from Elasticsearch, transforming it, and loading into PostgreSQL.

## Project Structure

```
aastrika-telemetry-service/
├── .env.example                # Environment template
├── .gitignore                  # Git ignore rules
├── .vscode-team/               # Shared VSCode configurations
│   ├── launch.json.example     # Debug configuration template
│   └── settings.json.example   # Editor settings template
├── main.py                     # Entry point (development)
├── pyproject.toml              # Project configuration & dependencies
├── uv.lock                     # Locked dependency versions
├── README.md                   # This file
├── src/aastrika_telemetry/     # Main package
│   ├── __main__.py             # Module entry point (production)
│   ├── config/                 # Configuration & connections
│   ├── extractors/             # Elasticsearch data extraction
│   ├── transformers/           # Data transformation logic
│   ├── loaders/                # PostgreSQL data loading
│   ├── pipeline/               # ETL orchestration
│   ├── models/                 # Data models (Pydantic schemas)
│   └── utils/                  # Utility functions
└── tests/                      # Test suite
    ├── unit/                   # Unit tests
    └── integration/            # Integration tests
```

## Setup for New Team Members

### 1. Clone Repository
```bash
git clone https://github.com/your-org/aastrika-telemetry-service.git
cd aastrika-telemetry-service
```

### 2. Install UV (if not installed)
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 3. Create Virtual Environment & Install Dependencies
```bash
uv sync
```

### 4. Configure Environment
```bash
cp .env.example .env
# Edit .env with your actual credentials
```

Required environment variables:
- `ES_HOST`, `ES_PORT` - Elasticsearch connection
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB` - PostgreSQL connection
- See `.env.example` for complete list

### 5. Set Up IDE (Optional)

#### VSCode Users
```bash
# Copy team-shared configurations to your personal .vscode/
cp .vscode-team/launch.json.example .vscode/launch.json
cp .vscode-team/settings.json.example .vscode/settings.json

# Customize .vscode/ files as needed (they're git-ignored)
```

**Recommended VSCode Extensions:**
- Python (ms-python.python)
- Ruff (charliermarsh.ruff)
- Pylance (ms-python.vscode-pylance)

## Running the ETL Pipeline

### Quick Connection Test (Development)
```bash
# Test database connections without running the pipeline
uv run python main.py
```
**Purpose:** Validates Elasticsearch and PostgreSQL connections.
**Use when:** Setting up environment, troubleshooting connection issues.

### Run Full ETL Pipeline (Development)
```bash
# Execute the complete ETL workflow
uv run python -m aastrika_telemetry
```
**Purpose:** Runs the full Elasticsearch → Transform → PostgreSQL pipeline.
**Use when:** Testing the complete ETL process locally.

### Production
```bash
# Production execution
python -m aastrika_telemetry
```
**Purpose:** Production deployment without `uv` wrapper.

## Development

### Running Tests
```bash
uv run pytest
```

### Code Formatting
```bash
uv run ruff format .
```

### Debugging in VSCode
1. Set up VSCode configs (see Setup step 5 above)
2. Press **F5** to start debugging
3. Two debug configurations available:
   - **Debug Aastrika Telemetry ETL**: Run as module
   - **Python: Debug main.py**: Run development script

**Note:** `.vscode/` is personal (git-ignored). Team configs are in `.vscode-team/`

## Architecture

### ETL Flow
1. **Extract**: Fetch data from Elasticsearch indices
2. **Transform**: Apply business logic and data transformations
3. **Load**: Insert transformed data into PostgreSQL

### Configuration
- Settings managed via `.env` file
- Pydantic settings for type safety and validation
- Connection pooling for PostgreSQL
- Singleton pattern for Elasticsearch client

## Security

### Environment Variables
- **NEVER commit the `.env` file** - it contains sensitive credentials
- `.env` is git-ignored for security
- Use `.env.example` as a template for required variables
- Each developer should have their own `.env` file locally

### IDE Settings
- `.vscode/` and `.claude/` are personal (git-ignored)
- Use `.vscode-team/` for team-shared configurations
- Modify your personal `.vscode/` as needed without affecting the team

## Contributing

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make changes and test locally
3. Run tests: `uv run pytest`
4. Format code: `uv run ruff format .`
5. Commit changes: `git commit -m "Description"`
6. Push to GitHub: `git push origin feature/your-feature`
7. Create Pull Request

## License

[Your License Here]
