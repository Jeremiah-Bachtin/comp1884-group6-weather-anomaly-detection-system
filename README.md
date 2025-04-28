# COMP1884 Group 6: Weather Anomaly Detection System 🌦️

This repository houses the full codebase for our COMP1884 group project on weather anomaly detection, using machine learning and explainable AI.

---

## 📌 Overview

The goal of this project is to develop a robust MVP capable of identifying weather anomalies in hourly data for the Heathrow borough using a hybrid ML pipeline. Outputs are fed into an interactive dashboard and evaluated using community feedback.

---

## 👥 Team & Responsibilities

| Name        | Sub-topic               | Responsibilities                         |
|-------------|-------------------------|------------------------------------------|
| Jeremy      | ETL & ML                | Ingestion, preprocessing, ML training    |
| Marie       | XAI                     | Model explanation, SHAP/LIME/NLG         |
| Nad         | Dashboard Design        | Interactive visualisation of results     |
| Dipo        | Community Engagement    | Feedback collection, light NLP analysis  |

---

## 🗂️ Folder Structure

```
COMP1884-Group6-Codebase-Complete/
├── config/                 # Central settings (rolling window, coords etc.)
├── data/
│   ├── raw/               # Raw forecast/historical from Open-Meteo
│   └── processed/         # Rolling means, normalised inputs, ML-ready
├── outputs/
│   ├── anomaly_flags/     # IF + LSTM output
│   ├── xai_input/         # RF outputs + metadata
│   └── dashboard-ready/   # Final combined files for dashboard
├── scripts/
│   ├── etl/               # Ingestion, preprocessing, upload to Drive
│   ├── modelling/         # ML models: IF, LSTM-AE, RF, Transformer
│   ├── xai/               # SHAP, LIME, NLG (Marie)
│   ├── dashboard/         # Charting + formatting helpers (Nad)
│   └── community/         # Feedback pipelines and NLP (Dipo)
├── notebooks/             # Colab notebooks for each team member
├── utils/                 # Shared tools, e.g. plotting, time utilities
├── requirements.txt       # Python dependencies
├── run_pipeline.py        # Entrypoint for local runs
├── CONTRIBUTING.md        # How to collaborate
└── README.md              # This file
```

---

## Currently Implemented Key Features

### **Historical Data Collection**
- Retrieves historical weather data starting from January 2017, broken down monthly.
- Provides hourly weather variables such as temperature, precipitation, and wind speed based on configured coordinates (default: Greater London).
- **Important Note**: The raw historical data **does not handle missing timestamps or duplicate entries**. These are addressed later in the merging process.

### **Merging Historical Data**
- Combines monthly historical files into a single, merged dataset for the entire period.
- Handles **Daylight Savings Time (DST) anomalies**:
    - **Duplicated timestamps** (e.g., from October DST changes) are logged and retained.
    - **Missing timestamps** (e.g., from March DST changes) are identified and logged.

- Produces a clean, consistent historical dataset ready for analysis or model training.

### **Forecast Data Collection**
- Fetches forecast data to supplement recent missing data:
    - Covers **past 3 days** and **next 5 days** from the current date.
    - Ensures gaps for the last 2 days are backfilled when historical data is unavailable.

- Saves trimmed **72-hour forecasts** starting from a configurable anchor time.


## 🚀 Quickstart (Local)

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt

# Run main ingestion pipeline
python run_pipeline.py
```

---

## 📄 Key Dependencies

- `pandas`, `numpy`, `scikit-learn`
- `tensorflow` or `torch` (for LSTM-AE / Transformer AE)
- `shap`, `lime`, `matplotlib`, `seaborn`
- `pydrive`, `python-dotenv` (Google Drive integration)

---

## 📤 Collaboration Workflow

- Each team member should **work primarily in their own notebook** (under `/notebooks`) until logic is stable.
- Once tested, move your final functions to the appropriate `scripts/` subfolder.
- If you're unsure where something goes, ping Jeremy in the chat.
- GitHub is version-controlled — **avoid pushing large data files**.

---

## 📍 Current MVP Focus

- Geographical Scope: Heathrow
- Variables:  
  - `temperature_2m`  
  - `surface_pressure`  
  - `precipitation`  
  - `wind_speed_10m`
- Rolling Window: 60 days (baseline normality)
- Output: Anomaly scores → Explainable output → Dashboard integration

---

## 🧠 Want to Contribute?

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for coding guidelines, naming conventions, and branch management tips.

---

> *Built with love, grit and Colab-friendly code — Group 6, 2025* 💡
