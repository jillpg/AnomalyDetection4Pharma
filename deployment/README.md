---
title: PharmaGuard Stress-Test Simulator
emoji: 💊
colorFrom: blue
colorTo: red
sdk: docker
pinned: false
license: mit
---

# PharmaGuard: Stress-Test Simulator

> **Advanced Anomaly Detection for Pharmaceutical Manufacturing**

This interactive demo simulates a real-time data stream from a pharmaceutical tablet press machine. It showcases the robustness of an **LSTM Autoencoder** against traditional methods (**Isolation Forest**, **PCA**) when facing complex mechanical failures.

## 🧪 Simulation Scenarios

You can inject synthetic anomalies to test model performance:

1.  **⚡ Spike**: Sudden jump in Tensile Strength (+50%). Tests instantaneous reaction.
2.  **📈 Drift**: Gradual increase in Ejection Force. Tests early detection of degradation.
3.  **❄️ Freeze**: Sensor signal flatline. Tests detection of data loss.

## 🛠️ Tech Stack

- **Frontend**: Streamlit
- **ML Engine**: PyTorch (LSTM), Scikit-Learn (Baselines)
- **Data**: Hugging Face Datasets (Gold Layer Sample)
- **Viz**: Plotly Interactive Charts

---
*Part of the AnomalyDetection4Pharma Project by [Jill Palma Garro](https://github.com/jillpg)*
