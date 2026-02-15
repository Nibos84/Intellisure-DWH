# Functionele Documentatie: Agentic Data Engineering System

## Wat doet het systeem?

Het Agentic Data Engineering System is een **autonome data pipeline** die natuurlijke taal begrijpt en vertaalt naar werkende data-integraties. Je beschrijft wat je wilt ("Haal data op van API X"), en het systeem:

1. **Analyseert** de bron
2. **Ontwerpt** een pipeline strategie
3. **Bouwt** de configuratie
4. **Voert uit** de data-operaties

Alles zonder dat je zelf code of configuratie hoeft te schrijven.

---

## Voor wie is dit systeem?

- **Data Engineers** die snel nieuwe bronnen willen ontsluiten
- **Analisten** die data nodig hebben maar geen pipeline-kennis hebben
- **Teams** die repetitieve data-integraties willen automatiseren

---

## Hoe gebruik je het?

### 1. Start de Interactieve Modus

```bash
python interact.py
```

### 2. Beschrijf je Opdracht

Voorbeelden:
- *"Ingest data from https://api.example.com/products"*
- *"Transform the Rechtspraak XML files to structured JSON"*
- *"Fetch weather data from KNMI and store it in the data lake"*

### 3. Review het Plan

Het systeem laat je zien:
- **Researcher Findings**: Wat het systeem heeft ontdekt over de bron
- **Architect Proposal**: Hoe de pipeline eruit gaat zien

Je keurt goed met `y` of wijst af met `n`.

### 4. Bekijk de Configuratie

Het systeem genereert een YAML manifest. Je controleert of het klopt.

### 5. Voer Uit

Het systeem voert de pipeline uit en rapporteert de resultaten.

---

## Wat gebeurt er achter de schermen?

### De Agents

Het systeem bestaat uit 5 gespecialiseerde AI agents:

#### Planning Fase
1. **Researcher** 🕵️
   - Analyseert de databron
   - Identificeert API structuur, formaat, paginatie
   - Output: "Bron X is een REST API met JSON en offset pagination"

2. **Architect** 🏗️
   - Ontwerpt de pipeline strategie
   - Bepaalt data flow (Landing → Silver → Gold)
   - Output: Een implementatieplan

3. **Engineer** 🛠️
   - Vertaalt het plan naar YAML configuratie
   - Gebruikt templates maar past ze aan
   - Output: Een executable manifest

#### Execution Fase
4. **Ingestion Specialist** 📥
   - Haalt data op van externe bronnen
   - Past zich aan aan API quirks (rate limits, errors)
   - Redeneert over paginatie en data extractie

5. **Transformation Specialist** 🔄
   - Transformeert ruwe data naar gestructureerde formats
   - Infereert schemas automatisch
   - Handelt data kwaliteit issues intelligent af

---

## Voorbeelden

### Voorbeeld 1: Nieuwe API Ontsluiten

**Jouw Input:**
```
Ingest data from https://dummyjson.com/recipes
```

**Wat er gebeurt:**

1. **Researcher**: *"DummyJSON biedt een REST API. Format: JSON. Paginatie: limit/skip parameters."*
2. **Architect**: *"Voorstel: GET request, opslaan in layer=landing/source=dummyjson/dataset=recipes"*
3. **Jij**: `y` (goedkeuren)
4. **Engineer**: Genereert `manifests/mas_ingest_data_from_dum.yaml`
5. **Jij**: `y` (uitvoeren)
6. **Ingestion Specialist**: *"Analyzing response... Found 'recipes' key. Fetching 30 records..."*
7. **Systeem**: ✅ Data opgeslagen in S3

**Resultaat:**
- 30 recepten opgeslagen in `s3://splendid-bethe/layer=landing/source=dummyjson/dataset=recipes/...`

---

### Voorbeeld 2: Data Transformeren

**Jouw Input:**
```
Transform Rechtspraak XML to structured JSON with ECLI, date, and summary
```

**Wat er gebeurt:**

1. **Researcher**: *"Rechtspraak data is in XML format, bevat juridische uitspraken"*
2. **Architect**: *"Voorstel: Lees van Landing, transformeer met AI, schrijf naar Silver"*
3. **Jij**: `y`
4. **Engineer**: Genereert transformatie manifest
5. **Jij**: `y`
6. **Transformation Specialist**: *"Analyzing XML structure... Extracting ECLI, date, summary fields... Processing 3 files..."*
7. **Systeem**: ✅ Gestructureerde JSON opgeslagen

**Resultaat:**
- Parquet files in `s3://splendid-bethe/layer=silver/source=rechtspraak/...`

---

## Belangrijke Concepten

### Hive Partitioning
Data wordt opgeslagen in een gestructureerde folder structuur:
```
layer=landing/source=rechtspraak/dataset=uitspraken/year=2026/month=02/day=15/
```

Dit maakt queries efficiënter.

### Layers
- **Landing**: Ruwe data, zoals opgehaald van de bron
- **Silver**: Getransformeerde, gestructureerde data
- **Gold**: Business-ready data (toekomstig)

### Manifests
YAML configuraties die pipelines beschrijven. Het systeem genereert deze automatisch, maar je kunt ze ook handmatig aanpassen.

---

## Beperkingen

- **LLM Afhankelijkheid**: Het systeem gebruikt OpenAI. Zonder API key werkt het niet.
- **Kosten**: Elke agent call kost tokens. Complexe pipelines kunnen duur worden.
- **Experimenteel**: Dit is een prototype. Productie-gebruik vereist extra validatie.

---

## Volgende Stappen

1. **Probeer het uit**: `python interact.py`
2. **Review de code**: Bekijk `src/agents/mas/` voor agent implementaties
3. **Pas aan**: Voeg nieuwe agent rollen toe of wijzig prompts
4. **Deploy**: Zet het systeem op een VPS voor geautomatiseerde runs
