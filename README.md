# Intellisure DWH - Agentic Data Engineering

A scalable Data Lakehouse architecture on OVH Cloud, driven by a **Manifest Driven Pipeline Runner** and **AI Agents**.

## Architecture

*   **Infrastructure**: OVH S3 (Storage) + OVH VPS (Compute)
*   **Orchestration**: Manifest Driven (YAML configuration)
*   **Agents**:
    *   `SmartIngestionAgent`: Generic REST/File fetcher.
    *   `TransformationAgent`: Schema validation & cleaning.
*   **CI/CD**: GitHub Actions (Linting, Testing).

## Getting Started

1.  Clone the repository.
2.  Copy `.env.example` to `.env` and fill in credentials.
3.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
4.  Run a pipeline:
    ```bash
    python main.py --manifest manifests/rechtspraak.yaml --env dev
    ```

## Project Structure

*   `manifests/`: YAML configurations for data pipelines.
*   `src/agents/`: Python logic for AI Agents.
*   `src/core/`: Shared logic (Config, S3Manager, Runner).
*   `tests/`: Unit tests.
