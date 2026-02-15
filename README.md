# Agentic Data Engineering System

> **Autonomous data pipelines powered by AI agents**

A Multi-Agent System (MAS) that transforms natural language requests into fully functional data pipelines. Describe what you want, and the system analyzes, designs, builds, and executes the pipeline automatically.

---

## 🚀 Quick Start

### 1. Setup

```bash
# Clone the repository
git clone <repo-url>
cd data_engineering_agents

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your OVH S3 and OpenAI credentials
```

### 2. Run Interactive Mode

```bash
python interact.py
```

**Example Interaction:**
```
> Ingest data from https://dummyjson.com/recipes

🕵️  RESEARCHER: DummyJSON provides a REST API at /recipes with JSON format...
🏗️  ARCHITECT: Proposed Plan - Fetch via GET, store in layer=landing/source=dummyjson...

Approve this plan? (y/n): y

🛠️  ENGINEER: Generated manifests/mas_ingest_data_from_dum.yaml

Execute this pipeline? (y/n): y

📥 INGESTION SPECIALIST: Analyzing API... Fetching 30 records... ✅ Complete
```

---

## 🧠 Architecture

The system consists of **5 AI agents** working together:

### Planning Agents
- **Researcher** 🕵️ - Analyzes data sources (API structure, format, pagination)
- **Architect** 🏗️ - Designs pipeline strategy (Landing → Silver → Gold)
- **Engineer** 🛠️ - Generates YAML manifests

### Execution Agents
- **Ingestion Specialist** 📥 - Fetches data with intelligent error handling
- **Transformation Specialist** 🔄 - Transforms data with schema inference

**Key Innovation:** All agents use LLM reasoning. No hardcoded logic.

---

## 📁 Project Structure

```
data_engineering_agents/
├── src/
│   ├── agents/
│   │   └── mas/                    # Multi-Agent System
│   │       ├── base_role.py        # AgentRole base class
│   │       ├── roles.py            # Planning agents
│   │       ├── orchestrator.py     # Workflow coordinator
│   │       ├── ingestion_specialist.py
│   │       └── transformation_specialist.py
│   └── core/
│       ├── ai_service.py           # OpenAI wrapper
│       ├── config.py               # Configuration
│       ├── runner.py               # Pipeline executor
│       └── s3_manager.py           # S3 operations
├── manifests/                      # Generated YAML configs
├── interact.py                     # Interactive CLI
├── main.py                         # Direct manifest runner
└── README.md
```

---

## 🎯 Use Cases

### 1. Ingest New API
```
> Ingest weather data from KNMI API
```
→ System analyzes API, generates manifest, fetches data to S3

### 2. Transform Data
```
> Transform Rechtspraak XML to JSON with ECLI and date fields
```
→ System reads Landing data, applies AI transformation, writes to Silver

### 3. Custom Pipelines
```
> Fetch products from Shopify and enrich with pricing data
```
→ System designs multi-step pipeline with your requirements

---

## 🛠️ Manual Pipeline Execution

You can also run manifests directly:

```bash
python main.py --manifest manifests/rechtspraak.yaml --env dev
```

---

## 📚 Documentation

- **[Functional Documentation](docs/functional_documentation.md)** - What the system does (user guide)
- **[Technical Documentation](docs/technical_documentation.md)** - How it works (architecture, code)

---

## 🔧 Configuration

### Environment Variables (`.env`)

```bash
# OVH S3 Storage
OVH_ACCESS_KEY=your_access_key
OVH_SECRET_KEY=your_secret_key
OVH_ENDPOINT=https://s3.rbx.io.cloud.ovh.net
OVH_REGION=rbx

# OpenAI
OPENAI_API_KEY=your_openai_key

# Environment
ENV=dev  # dev | prd
LLM_MODEL=gpt-3.5-turbo
```

---

## 🧪 Testing

Run the MAS test:
```bash
python test_mas.py
```

This simulates the full agent workflow and validates YAML generation.

---

## 🏗️ Data Lake Structure

Data is stored in **Hive-partitioned** S3 buckets:

```
s3://splendid-bethe/
├── layer=landing/
│   └── source=rechtspraak/
│       └── dataset=uitspraken/
│           └── year=2026/month=02/day=15/
│               └── batch_20260215120000.json
└── layer=silver/
    └── source=rechtspraak/
        └── dataset=uitspraken/
            └── year=2026/month=02/day=15/
                └── batch_20260215120000.parquet
```

**Layers:**
- **Landing**: Raw data as fetched from source
- **Silver**: Cleaned, structured data
- **Gold**: Business-ready aggregations (future)

---

## 🚧 Roadmap

- [x] Phase 1: Core ingestion & transformation agents
- [x] Phase 2: Multi-Agent System (MAS)
  - [x] Planning agents (Researcher, Architect, Engineer)
  - [x] Execution agents (Ingestion/Transformation Specialists)
- [ ] Phase 3: Deployment
  - [ ] VPS deployment
  - [ ] CI/CD pipeline
  - [ ] Scheduled runs

---

## 📝 License

MIT License - See LICENSE file for details

---

## 🤝 Contributing

This is an experimental prototype. Contributions welcome!

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

---

## ⚠️ Limitations

- **LLM Dependency**: Requires OpenAI API access
- **Cost**: Each agent interaction consumes tokens
- **Experimental**: Not production-ready without additional validation

---

**Built with ❤️ using OpenAI GPT-3.5 and OVH Cloud**
