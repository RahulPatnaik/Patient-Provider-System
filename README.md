# Patient-Provider System

AI-powered patient provider data validation and enrichment system using FastAPI and Pydantic AI.

## Overview

This system automates the validation and enrichment of healthcare provider data through a multi-agent architecture. It uses AI agents to validate provider information, enrich data from multiple sources, and make intelligent decisions about provider approval.

## Architecture

The system follows a multi-agent architecture with:
- **Supervisor Agent**: Central orchestrator
- **Router Decision Logic**: Routes to simple or complex validation paths
- **Fast Validator Agent**: Quick validation for simple cases
- **Four Parallel Agents** (complex path):
  - Data Validation Agent
  - Web Scraper Agent
  - Enrichment Agent
  - Compliance Agent
- **LLM Router**: Makes final decisions (auto-approve, manual review, auto-reject)
- **Feedback Loop**: Continuous learning from results

## Project Structure

```
patient-provider-system/
├── src/
│   ├── api/              # FastAPI routes and endpoints
│   ├── agents/           # Pydantic AI agents
│   ├── core/             # Core business logic
│   ├── models/           # Pydantic models
│   ├── services/         # External API integrations
│   ├── database/         # Database layer
│   ├── cache/            # Cache layer
│   ├── utils/            # Utilities
│   ├── config/           # Configuration
│   └── main.py           # Application entry point
├── tests/                # Test suite
├── scripts/              # Utility scripts
├── docs/                 # Documentation
└── Diagrams/             # Architecture diagrams
```

## Prerequisites

- Python 3.11+
- PostgreSQL (or SQLite for development)
- Redis
- Docker and Docker Compose (optional)

## Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd patient-provider-system
```

### 2. Environment Setup

#### Using Poetry (Recommended)

```bash
# Install Poetry
curl -sSL https://install.python-poetry.org | python3 -

# Install dependencies
poetry install

# Activate virtual environment
poetry shell
```

#### Using pip

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Environment Variables

```bash
# Copy example environment file
cp .env.example .env

# Edit .env and add your API keys and configuration
```

### 4. Database Setup

```bash
# Run migrations
python scripts/migrate_db.py

# (Optional) Seed database with sample data
python scripts/seed_db.py
```

## Running the Application

### Using Docker Compose (Recommended)

```bash
# Start all services (PostgreSQL, Redis, App, Celery, Selenium)
docker-compose up -d

# View logs
docker-compose logs -f app

# Stop services
docker-compose down
```

### Manual Run

```bash
# Start Redis (in separate terminal)
redis-server

# Start PostgreSQL (in separate terminal)
# Or use your existing PostgreSQL instance

# Run the application
uvicorn src.main:app --reload --host 0.0.0.0 --port 8000

# Or use the development script
python scripts/run_dev.py
```

## API Documentation

Once the application is running, visit:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/unit/test_agents/test_supervisor.py

# Run specific test
pytest tests/unit/test_agents/test_supervisor.py::test_supervisor_routing
```

### Code Quality

```bash
# Format code
black src/ tests/

# Lint code
ruff src/ tests/

# Type checking
mypy src/

# Run all quality checks
black src/ && ruff src/ && mypy src/
```

### Pre-commit Hooks

```bash
# Install pre-commit hooks
pre-commit install

# Run manually
pre-commit run --all-files
```

## API Endpoints

### Provider Validation

- `POST /providers` - Submit provider for validation
- `GET /providers/{id}` - Get provider validation status
- `GET /providers` - List all providers
- `PUT /providers/{id}` - Update provider data
- `DELETE /providers/{id}` - Delete provider

### Manual Review (Admin)

- `GET /admin/manual-review` - Get providers pending review
- `POST /admin/manual-review/{id}/approve` - Approve provider
- `POST /admin/manual-review/{id}/reject` - Reject provider
- `GET /admin/stats` - System statistics

### Health Checks

- `GET /health` - Basic health check
- `GET /health/ready` - Readiness probe
- `GET /health/live` - Liveness probe

## Team Collaboration

### Working on Different Components

The project is highly modular. Team members can work on different components independently:

1. **Agents Team**: Work on `src/agents/` - each agent is independent
2. **API Team**: Work on `src/api/routes/` - each route file is independent
3. **Services Team**: Work on `src/services/` - each service client is independent
4. **Core Logic Team**: Work on `src/core/` - router, orchestrator, scorer, etc.
5. **Database Team**: Work on `src/database/` - models, repositories

### Git Workflow

```bash
# Create feature branch
git checkout -b feature/your-feature-name

# Make changes and commit
git add .
git commit -m "Description of changes"

# Push to remote
git push origin feature/your-feature-name

# Create pull request
```

## Configuration

Key configuration in `.env`:

- **Database**: `DATABASE_URL`
- **Redis**: `REDIS_URL`
- **API Keys**: `OPENAI_API_KEY`, `GOOGLE_MAPS_API_KEY`, etc.
- **Thresholds**: `CONFIDENCE_AUTO_APPROVE_THRESHOLD`, etc.

## Troubleshooting

### Database Connection Issues

```bash
# Check PostgreSQL is running
docker-compose ps postgres

# Check connection
psql -h localhost -U provider_user -d provider_db
```

### Redis Connection Issues

```bash
# Check Redis is running
docker-compose ps redis

# Test connection
redis-cli ping
```

### Import Errors

```bash
# Make sure you're in the virtual environment
poetry shell  # or source venv/bin/activate

# Reinstall dependencies
poetry install  # or pip install -r requirements.txt
```

## License

[Your License Here]

## Contributors

[Your Team Members]
# Patient-Provider-System
