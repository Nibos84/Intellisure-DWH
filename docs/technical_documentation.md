# Technische Documentatie: Agentic Data Engineering System

## Architectuur Overzicht

Het systeem is gebouwd als een **Multi-Agent System (MAS)** met twee lagen:

```
┌─────────────────────────────────────────────────────────┐
│                   USER INTERFACE                        │
│                    (interact.py)                        │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│              PLANNING AGENTS (MAS)                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐             │
│  │Researcher│→ │Architect │→ │ Engineer │             │
│  └──────────┘  └──────────┘  └──────────┘             │
│         │            │              │                   │
│         └────────────┴──────────────┘                   │
│                      │                                   │
│                      ▼                                   │
│              YAML Manifest                              │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│            EXECUTION AGENTS (MAS)                       │
│  ┌─────────────────┐  ┌──────────────────┐            │
│  │   Ingestion     │  │ Transformation   │            │
│  │   Specialist    │  │   Specialist     │            │
│  └─────────────────┘  └──────────────────┘            │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                 DATA STORAGE (S3)                       │
│         layer=landing / layer=silver / ...              │
└─────────────────────────────────────────────────────────┘
```

---

## Core Components

### 1. AgentRole (Base Class)

**Locatie:** `src/agents/mas/base_role.py`

**Doel:** Basisklasse voor alle MAS agents. Beheert conversatie geschiedenis en LLM interactie.

**Key Methods:**
- `__init__(name, role, goal)`: Initialiseert agent met persona
- `chat(user_input)`: Stuurt input naar LLM, onthoudt context
- `reset_memory()`: Wist conversatie geschiedenis

**Design Pattern:**
```python
class AgentRole:
    def __init__(self, name, role, goal):
        self.history = [{"role": "system", "content": system_prompt}]
    
    def chat(self, user_input):
        self.history.append({"role": "user", "content": user_input})
        response = ai_service.chat(self.history)
        self.history.append({"role": "assistant", "content": response})
        return response
```

**Waarom dit werkt:**
- Elke agent heeft zijn eigen conversatie context
- System prompt definieert de agent persona
- LLM "wordt" de agent door de prompt

---

### 2. Planning Agents

#### ResearcherAgent
**Locatie:** `src/agents/mas/roles.py`

**Verantwoordelijkheid:** Analyseert databronnen

**System Prompt Highlights:**
- "Identify the format (JSON, XML), method (GET), and pagination strategy"
- "Start your response with 'Research Findings:'"

**Voorbeeld Output:**
```
Research Findings: DummyJSON provides a REST API at /recipes. 
Format: JSON. Pagination: limit/skip parameters. 
Expected response structure: { "recipes": [...], "total": N }
```

#### ArchitectAgent
**Locatie:** `src/agents/mas/roles.py`

**Verantwoordelijkheid:** Ontwerpt pipeline strategie

**System Prompt Highlights:**
- "Define the Source → Landing → Silver flow"
- "Suggest naming conventions for S3 paths"

**Voorbeeld Output:**
```
Proposed Plan:
1. Fetch data via GET from /recipes
2. Store in layer=landing/source=dummyjson/dataset=recipes
3. Use Hive partitioning: year/month/day
4. Transform to Silver with AI extraction
```

#### EngineerAgent
**Locatie:** `src/agents/mas/roles.py`

**Verantwoordelijkheid:** Genereert YAML manifests

**Belangrijke Detail:** Krijgt YAML templates in de user message (niet system prompt) voor betere adherence.

**Template Injection (in Orchestrator):**
```python
yaml_templates = """
YAML TEMPLATE FOR INGESTION:
```yaml
pipeline_name: "source_ingestion"
agent_type: "generic_rest_api"
...
```
"""

build_input = f"Mission: {mission}\nPlan: {plan}\n{yaml_templates}\n\nAdapt one of these templates..."
```

**Waarom dit werkt:**
- Templates in user message = directere instructie
- LLM ziet het als "voorbeeld om te volgen" ipv "documentatie"

---

### 3. Orchestrator

**Locatie:** `src/agents/mas/orchestrator.py`

**Verantwoordelijkheid:** Coördineert de agent workflow

**Key Methods:**

#### `start_mission(mission: str) -> Dict`
Planning fase:
1. Researcher analyseert de mission
2. Architect ontvangt research + mission, maakt plan
3. Retourneert `{research, plan, mission}`

#### `execute_mission(context: Dict) -> str`
Execution fase:
1. Engineer ontvangt plan + templates
2. Genereert YAML
3. Extraheert YAML uit markdown code block (regex)
4. Retourneert clean YAML string

**YAML Extraction Logic:**
```python
match = re.search(r"```(?:yaml)?\n(.*?)\n```", yaml_output, re.DOTALL)
if match:
    yaml_output = match.group(1)
```

**Waarom regex?** LLMs wrappen vaak output in markdown. Dit zorgt voor clean YAML.

---

### 4. Execution Agents

#### IngestionSpecialistAgent
**Locatie:** `src/agents/mas/ingestion_specialist.py`

**Verantwoordelijkheid:** Reasoning-based data fetching

**Workflow:**
1. Analyseert manifest met LLM: "What strategy should I use?"
2. Fetcht data via `requests`
3. Bij errors: vraagt LLM om recovery strategie
4. Slaat data op in S3 met Hive partitioning

**LLM Reasoning Voorbeeld:**
```python
strategy = self.chat(
    f"I need to ingest from {url}. "
    f"Pagination: {pagination}. "
    f"What strategy should I use?"
)
# LLM response: "Use offset pagination with limit=50. Watch for rate limits."
```

**Data Extraction Intelligence:**
```python
# LLM bepaalt welke key de data bevat
analysis = self.chat(
    f"API returned dict with keys: {list(data.keys())}. "
    f"Which key contains the data records?"
)
# LLM response: "The 'recipes' key contains the array of records."
```

#### TransformationSpecialistAgent
**Locatie:** `src/agents/mas/transformation_specialist.py`

**Verantwoordelijkheid:** Reasoning-based data transformation

**Workflow:**
1. Analyseert transformatie taak met LLM
2. Leest source files van S3
3. Gebruikt `ai_service.transform_data()` voor daadwerkelijke transformatie
4. Bij errors: vraagt LLM of file geskipt moet worden
5. Slaat getransformeerde data op in Silver layer

**Key Difference vs Old GenericAITransformer:**
- **Oud:** Hardcoded loop, geen error reasoning
- **Nieuw:** LLM bepaalt strategie, handelt errors intelligent af

---

### 5. Runner

**Locatie:** `src/core/runner.py`

**Verantwoordelijkheid:** Leest manifest, start juiste execution agent

**Workflow:**
```python
manifest = yaml.safe_load(manifest_file)
agent_type = manifest["agent_type"]

if agent_type == "generic_rest_api":
    agent = IngestionSpecialistAgent()
    result = agent.execute(manifest)
elif agent_type == "generic_ai_transformer":
    agent = TransformationSpecialistAgent()
    result = agent.execute(manifest)
```

**Design Decision:** Runner is "dumb dispatcher". Alle intelligentie zit in de agents.

---

## Data Flow

### Ingestion Flow

```
User Input
    ↓
Researcher → "API analysis"
    ↓
Architect → "Pipeline design"
    ↓
Engineer → "YAML manifest"
    ↓
IngestionSpecialistAgent.execute(manifest)
    ↓
    1. LLM: Analyze strategy
    2. requests.get(url)
    3. LLM: Extract data from response
    4. s3_manager.write_file()
    ↓
S3: layer=landing/source=X/dataset=Y/year=.../batch_*.json
```

### Transformation Flow

```
User Input
    ↓
[Planning Agents] → YAML manifest
    ↓
TransformationSpecialistAgent.execute(manifest)
    ↓
    1. LLM: Analyze transformation strategy
    2. s3_manager.list_files(source_path)
    3. For each file:
        - s3_manager.read_file()
        - ai_service.transform_data() [LLM call]
        - s3_manager.write_file(target_path)
    ↓
S3: layer=silver/source=X/dataset=Y/.../batch_*.json
```

---

## Key Design Decisions

### 1. Waarom MAS ipv Single Agent?

**Voordeel van specialisatie:**
- Researcher focust op source analysis
- Architect focust op design
- Engineer focust op code generation

**Alternatief (afgewezen):**
- Eén "super agent" die alles doet
- **Probleem:** Prompt wordt te complex, LLM raakt gefocust kwijt

### 2. Waarom Templates in User Message?

**Getest:**
- Templates in system prompt → LLM ziet het als documentatie
- Templates in user message → LLM ziet het als voorbeeld om te volgen

**Resultaat:** User message werkt beter voor template adherence.

### 3. Waarom Execution Agents ook Reasoning?

**Oude aanpak:**
- Hardcoded `if/else` logic voor paginatie, errors, etc.

**Probleem:**
- Elke nieuwe API quirk = code aanpassing
- Niet schaalbaar

**Nieuwe aanpak:**
- LLM redeneert over API behavior
- Past strategie aan zonder code changes

---

## Configuration

### Environment Variables

**Locatie:** `.env`

```bash
# OVH S3
OVH_ACCESS_KEY=xxx
OVH_SECRET_KEY=xxx
OVH_ENDPOINT=https://s3.rbx.io.cloud.ovh.net
OVH_REGION=rbx

# OpenAI
OPENAI_API_KEY=xxx

# Environment
ENV=dev  # dev | prd
LLM_MODEL=gpt-3.5-turbo
```

### Config Class

**Locatie:** `src/core/config.py`

**Doel:** Centraal configuratie management

**Key Properties:**
- `env`: Environment (dev/prd)
- `ovh_*`: S3 credentials
- `llm_model`: OpenAI model naam

---

## Testing

### Test Scripts

**`test_mas.py`:**
- Simuleert volledige MAS workflow
- Valideert YAML output
- Gebruikt voor development testing

**Voorbeeld:**
```python
context = orchestrator.start_mission("Ingest from API X")
yaml_content = orchestrator.execute_mission(context)
parsed = yaml.safe_load(yaml_content)
assert "pipeline_name" in parsed
```

---

## Deployment Considerations

### LLM Costs
- Elke agent call = OpenAI API call
- Complexe pipelines = meerdere calls
- **Mitigation:** Cache common analyses, limit context window

### Error Handling
- Execution agents vragen LLM om recovery
- **Risk:** LLM kan verkeerde recovery suggereren
- **Mitigation:** Log alle LLM responses, human review bij failures

### Scalability
- Huidige implementatie: sequentieel
- **Future:** Parallel execution van multiple files
- **Blocker:** LLM rate limits

---

## Code Structure

```
src/
├── agents/
│   └── mas/
│       ├── base_role.py              # AgentRole base class
│       ├── roles.py                  # Researcher, Architect, Engineer
│       ├── orchestrator.py           # Workflow coordinator
│       ├── ingestion_specialist.py   # Execution: Ingestion
│       └── transformation_specialist.py  # Execution: Transformation
├── core/
│   ├── ai_service.py                 # OpenAI wrapper
│   ├── config.py                     # Configuration management
│   ├── runner.py                     # Pipeline executor
│   └── s3_manager.py                 # S3 operations
interact.py                            # CLI interface
main.py                                # Direct manifest runner
```

---

## Extensibility

### Nieuwe Agent Toevoegen

1. Extend `AgentRole`:
```python
class QualityCheckerAgent(AgentRole):
    def __init__(self):
        super().__init__(
            name="Quality Checker",
            role="Data Quality Analyst",
            goal="Validate data quality and completeness"
        )
```

2. Voeg toe aan Orchestrator workflow

3. Update `interact.py` om output te tonen

### Nieuwe Agent Type (Execution)

1. Implementeer agent in `src/agents/mas/`
2. Update `runner.py`:
```python
elif agent_type == "new_type":
    agent = NewAgent()
    result = agent.execute(manifest)
```

---

## Troubleshooting

### "No data extracted"
- Check LLM response in logs
- Verify source data structure
- Test with smaller dataset

### "Invalid YAML"
- Check Engineer agent output
- Verify template injection in Orchestrator
- Test YAML extraction regex

### "S3 Permission Denied"
- Verify `.env` credentials
- Check bucket permissions
- Test with `verify_s3.py`
