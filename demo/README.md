---
title: PharmaGuard Stress-Test Simulator
emoji: 💊
colorFrom: blue
colorTo: red
sdk: docker
pinned: false
license: mit
---

# 💊 PharmaGuard Stress-Test Simulator

**Interactive Anomaly Detection for Pharmaceutical Manufacturing**

This demo simulates a tablet manufacturing process and showcases a Hybrid AI approach to quality control, combining:
- **LSTM Autoencoder** (Deep Learning for temporal patterns)
- **PCA** (Multivariate correlation analysis)
- **Isolation Forest** (Statistical outlier detection)

## 🚀 Key Features

- **Real-time streaming**: 10Hz simulation of sensor telemetry.
- **Anomaly Injection**: Inject synthetic faults (Spike, Drift, Freeze) to test robustness.
- **Root Cause Analysis**: Visualize which sensors contribute to the anomaly score.
- **Model Consensus**: Compare reaction times of different algorithms.

## 🛠️ Tech Stack

- **Frontend**: Streamlit
- **Backend**: PyTorch, Scikit-learn
- **Data**: Hugging Face Datasets (streaming Gold layer subset)
- **Deployment**: Docker container on HF Spaces

## 🏃‍♂️ How to Run Locally

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run the app:
   ```bash
   streamlit run app.py
   ```

## 📚 Data Source

Based on the dataset: *Zagar, L., et al. (2022). Big data collection in pharmaceutical manufacturing.*
