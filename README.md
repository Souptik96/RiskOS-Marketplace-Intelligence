---
title: SQL-to-Metrics BI Agent (Marketplace Intelligence)
emoji: 📊
colorFrom: blue
colorTo: purple
sdk: streamlit
sdk_version: 1.32.0
app_file: app.py
pinned: false
license: apache-2.0
---

# Marketplace Intelligence

🛒 **Marketplace Intelligence** - AI-powered data analytics platform with dbt integration and automated dashboard generation.

## Overview

Marketplace Intelligence is a comprehensive data analytics platform that transforms natural language queries into SQL, generates dbt models, and creates interactive dashboards automatically. Built with Streamlit, FastAPI, and dbt, it provides both local and cloud-based data processing capabilities.

### Key Features

- 🗣️ **Natural Language to SQL**: Convert business questions into DuckDB SQL queries
- 📊 **Auto-Generated Dashboards**: Interactive Streamlit dashboards for any metric
- 🔧 **dbt Integration**: Generate and run dbt models automatically
- 🚀 **Dual Architecture**: Both local processing and cloud API support
- 🐳 **Docker Support**: Containerized deployment with one command
- 🔄 **CI/CD Pipeline**: Automated testing and deployment

## Quick Start

### Prerequisites

- Python 3.11+
- DuckDB (included with requirements)
- dbt Core and dbt-DuckDB (included with requirements)

### Local Development

1. **Clone and Setup**
   ```bash
   git clone https://huggingface.co/spaces/soupstick/marketplace-intelligence
   cd marketplace-intelligence
   ```

2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Environment Configuration**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

4. **Start Backend** (Terminal 1)
   ```bash
   uvicorn api.main:app --host 0.0.0.0 --port 7861
   ```

5. **Start Frontend** (Terminal 2)
   ```bash
   streamlit run app.py --server.port 7860 --server.address 0.0.0.0
   ```

6. **Open Browser**
   - **Streamlit UI**: http://localhost:7860
   - **API Documentation**: http://localhost:7861/docs

### Example Usage

Try these natural language queries:

- "Gross margin by category last quarter"
- "Top 5 electronics products by revenue in Q3"
- "Revenue trends over time by product category"
- "Average order value by category"

### Generate Custom Metrics

Use the **"Generate Metric (dbt + Dashboard)"** sidebar section:

1. Enter your business question (e.g., "Customer lifetime value by acquisition channel")
2. Optionally specify a metric slug (e.g., "clv_by_channel")
3. Click "Generate Metric"
4. View the preview table and auto-generated dashboard

## Docker Deployment

### Build and Run

```bash
# Build the Docker image
docker build -t marketplace-intelligence .

# Run with exposed ports
docker run -p 7860:7860 -p 7861:7861 marketplace-intelligence
```

### Docker Compose (Optional)

```bash
# Create docker-compose.yml
cat > docker-compose.yml << 'EOF'
version: '3.8'
services:
  marketplace-intelligence:
    build: .
    ports:
      - "7860:7860"
      - "7861:7861"
    environment:
      - AGENT_API_URL=http://localhost:7861
      - DBT_PROFILES_DIR=./dbt_project/profiles
EOF

# Run with compose
docker-compose up
```

## Architecture

### Components

- **Streamlit Frontend** (`app.py`): Interactive UI with natural language query interface
- **FastAPI Backend** (`api/main.py`): RESTful API with agent orchestration
- **LangGraph Agent** (`agent/`): AI-powered workflow for metric generation
- **dbt Integration** (`dbt_project/`): Data transformation and modeling
- **Dashboard Scaffold** (`tools/viz_scaffold.py`): Auto-generation of Streamlit dashboards

### Data Flow

1. User enters natural language query
2. Query is sent to FastAPI backend
3. LangGraph agent processes the request:
   - Parses intent
   - Generates SQL
   - Validates and executes
   - Creates dbt model
   - Runs dbt transformations
4. Results are returned to frontend
5. Dashboard is auto-generated for the metric

## Development

### Branching Strategy

```bash
# Create feature branch
git checkout -b feature/your-feature-name

# Push and create PR
git push -u origin feature/your-feature-name
```

### Running Tests

```bash
# Run all tests
pytest -q tests/

# Run dbt tests
cd dbt_project
dbt test --project-dir . --profiles-dir ./profiles
```

### Environment Variables

Key environment variables (see `.env.example`):

- `AGENT_API_URL`: Backend API endpoint (default: `http://localhost:7861`)
- `DBT_PROFILES_DIR`: dbt profiles directory (default: `./dbt_project/profiles`)
- `LLM_PROVIDER`: LLM provider (`fireworks` or `hf`)
- `FIREWORKS_API_KEY`: Fireworks API key (if using Fireworks)
- `HF_API_KEY`: Hugging Face API key (if using HF)

## Contributing

We welcome contributions! Please follow these steps:

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Commit your changes: `git commit -m 'Add amazing feature'`
4. Push to the branch: `git push origin feature/amazing-feature`
5. Open a Pull Request

### Development Guidelines

- Follow PEP 8 for Python code
- Add tests for new features
- Update documentation for API changes
- Ensure CI passes before merging

## Troubleshooting

### Common Issues

**Backend not responding**:
```bash
# Check if FastAPI is running
curl http://localhost:7861/health

# Check logs
python -c "from api.main import app; print('API imports successfully')"
```

**Dashboard creation fails**:
```bash
# Check dbt installation
dbt --version

# Verify profiles directory
ls -la dbt_project/profiles/

# Test dashboard generation manually
python -c "from tools.viz_scaffold import make_dashboard; print('OK')"
```

**Environment variables not loading**:
```bash
# Check .env file
cat .env

# Test loading
python -c "from dotenv import load_dotenv; load_dotenv(); import os; print(os.getenv('AGENT_API_URL'))"
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- **Streamlit**: For the excellent UI framework
- **FastAPI**: For the high-performance API framework
- **dbt**: For data transformation best practices
- **DuckDB**: For fast analytical queries
- **LangGraph**: For AI workflow orchestration