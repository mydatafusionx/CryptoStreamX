# CryptoStreamX - Data Pipeline with Databricks

A modern data pipeline project that demonstrates end-to-end data engineering using Databricks, Delta Lake, and Unity Catalog. This project ingests cryptocurrency data from CoinGecko API, processes it through multiple layers, and makes it available for analytics.

## Project Structure

```
CryptoStreamX/
├── notebooks/               # Databricks notebooks
│   ├── bronze/             # Data ingestion layer
│   ├── silver/             # Data transformation layer
│   └── gold/               # Business metrics layer
├── workflows/              # Pipeline and job configurations
├── .github/workflows/      # CI/CD configurations
└── README.md               # This file
```

## Prerequisites

- Databricks workspace (Free/Community Edition)
- Databricks CLI configured with access token
- GitHub account
- Python 3.8+

## Setup

1. Clone this repository
2. Install dependencies:
   ```bash
   pip install databricks-cli requests pandas
   ```
3. Configure Databricks CLI:
   ```bash
   databricks configure --token
   ```
4. Set up the following secrets in your GitHub repository:
   - `DATABRICKS_HOST`: Your Databricks workspace URL
   - `DATABRICKS_TOKEN`: Your Databricks access token

## Usage

1. **Bronze Layer (Ingestion)**
   - Notebook: `notebooks/bronze/ingest_coingecko.py`
   - Ingests raw data from CoinGecko API

2. **Silver Layer (Transformation)**
   - Notebook: `notebooks/silver/transform_coingecko.py`
   - Cleans and transforms the raw data

3. **Gold Layer (Aggregation)**
   - Notebook: `notebooks/gold/aggregate_metrics.py`
   - Creates business-ready metrics and KPIs

## CI/CD Pipeline

The project includes GitHub Actions workflow (`.github/workflows/deploy.yml`) that:
- Lints Python code
- Tests notebook execution
- Deploys to Databricks workspace

## License

MIT License
