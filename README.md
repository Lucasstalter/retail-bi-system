# RETAIL-BI-SYSTEM

A comprehensive Business Intelligence system for retail analytics, combining ETL pipelines, machine learning models, and interactive dashboards to derive actionable insights from transactional data.

## Overview

This project implements a complete data science pipeline for retail business intelligence, featuring:

- Automated ETL processes for data ingestion and transformation
- Machine learning models for forecasting, anomaly detection, and customer segmentation
- Interactive web-based dashboards with real-time analytics
- RESTful API for data access and model serving
- Power BI integration for enterprise reporting

The system processes 100,000+ transactions spanning 2022-2024, generating insights across revenue trends, product performance, regional distribution, and customer behavior.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Usage](#usage)
- [Models](#models)
- [API Documentation](#api-documentation)
- [Dashboard](#dashboard)
- [Contributing](#contributing)
- [License](#license)

## Features

### Data Processing
- Synthetic dataset generation (100k transactions)
- Automated ETL pipeline with data validation
- Multi-format output (CSV, Parquet, SQLite)
- Data quality monitoring and logging

### Machine Learning
- **Time Series Forecasting**: Facebook Prophet for revenue prediction
- **Anomaly Detection**: Isolation Forest for outlier identification
- **Customer Segmentation**: K-Means clustering with RFM analysis
- Model versioning and persistence

### Analytics & Visualization
- Revenue trend analysis with seasonality detection
- Product performance ranking and category breakdown
- Geographic distribution analysis across Brazilian regions
- Customer lifetime value calculation
- Interactive ASCII-based terminal dashboard
- Power BI integration

### API & Deployment
- FastAPI backend with automatic documentation
- CORS-enabled for cross-origin requests
- Containerized deployment with Docker
- Railway/Vercel deployment ready

## Architecture

```
┌─────────────┐
│   Data Gen  │
└──────┬──────┘
       │
       v
┌─────────────┐     ┌──────────────┐
│  ETL Layer  │────>│  Data Store  │
└──────┬──────┘     └──────────────┘
       │
       v
┌─────────────┐     ┌──────────────┐
│  ML Models  │────>│   Forecasts  │
└──────┬──────┘     └──────────────┘
       │
       v
┌─────────────┐     ┌──────────────┐
│  Analytics  │────>│  Dashboards  │
└─────────────┘     └──────────────┘
       │
       v
┌─────────────┐
│  API Layer  │
└─────────────┘
```

## Installation

### Prerequisites

- Python 3.8 or higher
- pip package manager
- Virtual environment tool (venv or conda)
- 4GB RAM minimum
- 2GB free disk space

### Setup

1. Clone the repository:
```bash
git clone https://github.com/seu-usuario/retail-bi-system.git
cd retail-bi-system
```

2. Create and activate virtual environment:
```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/macOS
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies:
```bash
# Windows
pip install -r requirements-windows.txt

# Linux/macOS
pip install -r requirements.txt
```

4. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your configurations
```

## Quick Start

### Run Complete Pipeline

Execute the entire data pipeline (generation, ETL, ML, and analytics):

```bash
python run_pipeline.py
```

This will:
1. Generate 100k synthetic transactions
2. Execute ETL transformations
3. Train ML models (Prophet, K-Means, Isolation Forest)
4. Generate analytics reports
5. Output results to `01_data/` directory

### Launch API Server

Start the FastAPI backend:

```bash
cd 05_api
uvicorn main:app --reload
```

API will be available at: `http://localhost:8000`
Documentation at: `http://localhost:8000/docs`

### View Dashboard

Open the interactive dashboard:

```bash
# Open in browser
07_web_demo/dashboard-lucas-code-complete.html
```


## Usage

### Generate Custom Dataset

```python
from etl.generate_data import generate_transactions

# Generate custom dataset
df = generate_transactions(
    n_transactions=50000,
    start_date='2023-01-01',
    end_date='2023-12-31',
    n_customers=5000
)

df.to_csv('01_data/custom_transactions.csv', index=False)
```

### Train Forecasting Model

```python
from ml.forecasting import train_prophet_model
import pandas as pd

# Load data
df = pd.read_csv('01_data/processed/transactions.csv')

# Train model
model, forecast = train_prophet_model(
    df=df,
    periods=90,  # Forecast 90 days ahead
    freq='D'
)

# Save model
model.save('01_data/models/prophet_model.pkl')
```

### Perform Customer Segmentation

```python
from ml.segmentation import rfm_segmentation

# Calculate RFM scores
rfm_df = rfm_segmentation(
    df=transactions,
    customer_col='customer_id',
    date_col='transaction_date',
    amount_col='total_amount'
)

# Classify customers
segments = classify_customers(rfm_df)
```

### Detect Anomalies

```python
from ml.anomaly_detection import detect_anomalies

# Train Isolation Forest
anomalies = detect_anomalies(
    df=daily_revenue,
    contamination=0.1
)

# Flag anomalous transactions
df['is_anomaly'] = anomalies
```

## Models

### Prophet Forecasting

- **Algorithm**: Facebook Prophet
- **Features**: Seasonality decomposition, trend analysis, holiday effects
- **Performance**: MAPE < 8% on validation set
- **Use Cases**: Revenue forecasting, demand prediction

### K-Means Clustering

- **Algorithm**: K-Means with Elbow method
- **Features**: RFM (Recency, Frequency, Monetary) analysis
- **Segments**: Champions, Loyal, Potential, At Risk, Lost
- **Applications**: Customer targeting, retention strategies

### Isolation Forest

- **Algorithm**: Isolation Forest ensemble
- **Features**: Revenue patterns, transaction volumes
- **Threshold**: 95th percentile
- **Applications**: Fraud detection, promotion impact analysis

## API Documentation

### Endpoints

#### Get Revenue Summary
```http
GET /api/v1/revenue/summary
```

Response:
```json
{
  "total_revenue": 15420389,
  "total_transactions": 100000,
  "avg_ticket": 154.20,
  "growth_rate": 0.125
}
```

#### Get Forecasts
```http
GET /api/v1/forecasts/revenue?periods=30
```

Response:
```json
{
  "forecasts": [
    {
      "date": "2025-01-01",
      "predicted_revenue": 142000,
      "lower_bound": 135000,
      "upper_bound": 149000,
      "confidence": 0.78
    }
  ]
}
```

#### Get Customer Segments
```http
GET /api/v1/customers/segments
```

Response:
```json
{
  "segments": {
    "champions": 1284,
    "loyal": 2568,
    "potential": 1876,
    "at_risk": 1543,
    "lost": 1282
  }
}
```

Full API documentation available at `/docs` when server is running.

## Dashboard

The interactive dashboard provides:

- **Real-time KPIs**: Revenue, profit, margin, transactions
- **Time Series Analysis**: Monthly revenue trends with anomaly highlighting
- **Category Breakdown**: Revenue distribution across product categories
- **Geographic Analysis**: Regional performance across Brazil
- **Product Rankings**: Top 10 products by revenue
- **Customer Segmentation**: RFM-based customer classification
- **ML Insights**: Anomaly detection, forecasts, profitability analysis

Features:
- ASCII-based terminal aesthetic
- Matrix rain background animation
- Responsive design for mobile/desktop
- Zero external dependencies (Chart.js CDN)

## Contributing

Contributions are welcome. Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-feature`)
3. Commit changes (`git commit -am 'Add new feature'`)
4. Push to branch (`git push origin feature/new-feature`)
5. Create Pull Request

### Code Standards

- Follow PEP 8 style guide for Python code
- Add docstrings to all functions and classes
- Include unit tests for new features
- Update documentation as needed

### Testing

Run tests before submitting:
```bash
pytest tests/ -v --cov
```

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) file for details.

## Acknowledgments

- Facebook Prophet for time series forecasting
- scikit-learn for machine learning algorithms
- FastAPI for API framework
- Chart.js for data visualization
