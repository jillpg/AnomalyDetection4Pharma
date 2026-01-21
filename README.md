# AnomalyDetection4Pharma

![CI Status](https://github.com/jillpg/AnomalyDetection4Pharma/actions/workflows/ci.yml/badge.svg)

[![Coverage Status](https://coveralls.io/repos/github/jillpg/AnomalyDetection4Pharma/badge.svg?branch=main)](https://coveralls.io/github/jillpg/AnomalyDetection4Pharma?branch=main)

Proyecto de detección temprana de anomalías en producción farmacéutica.

## Estructura del repositorio

AnomalyDetection4Pharma/
├── .github/
│ └── workflows/ci.yml
├── src/
│ ├── data_ingest.py
│ ├── etl_pyspark.py
│ ├── synth_data.py
│ ├── models/
│ └── streaming_sim.py
├── tests/
│ └── test_etl.py
├── notebooks/
│ ├── 01_EDA_and_feature_gen.ipynb
│ ├── 02_baseline_classic.ipynb
│ └── 03_lstm_autoencoder.ipynb
├── data/
│ ├── raw/
│ └── processed/
├── dashboard/
│ └── streamlit_app.py
├── docs/
│ └── architecture_diagram.png
├── docker/
│ ├── Dockerfile.base
│ └── docker-compose.yml
└── README.md

## Convenciones de ramas

- **main**: rama estable, protegida
- **dev**: rama de integración
- **feature/<nombre>**: nuevas funcionalidades
- **hotfix/<descripción>**: correcciones urgentes
