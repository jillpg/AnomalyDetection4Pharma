import streamlit as st
import time
import numpy as np
import os
import sys

# Add current directory to path so imports work
sys.path.append(os.path.dirname(__file__))

from ui.layout import setup_page
from ui.components import render_kpi_row, render_model_consensus
from ui.visualizations import create_sensor_plot, create_error_chart, create_attribution_chart
from models.lstm_loader import load_lstm_model
from models.baseline_loader import load_baseline_models
from models.inference import TripleDetector
from simulation.data_stream import RealTimeStream
from simulation.anomaly_injector import AnomalyInjector
from simulation.threshold import calculate_threshold

# --- 1. Setup ---
from dotenv import load_dotenv
load_dotenv() # Load HF_TOKEN from .env

setup_page()

# --- 2. Resource Loading (Cached) ---
@st.cache_resource
def get_stream():
    return RealTimeStream()

@st.cache_resource
def get_injector():
    return AnomalyInjector()

# Initialize stream FIRST to get valid feature cols
stream = get_stream()
injector = get_injector()

# --- 2. Resource Loading (Cached) ---
@st.cache_resource
def load_resources(feature_cols):
    # Paths (Try local first, then download from Hub)
    repo_id = "jillpg/pharma-models"
    
    import os
    from huggingface_hub import hf_hub_download

    def get_model_path(filename):
        # 1. Check deployment dir (if using bundle with models)
        local_path = os.path.join("models", filename)
        if os.path.exists(local_path):
            return local_path
            
        # 2. Check production dir (dev mode)
        dev_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "models", "production", filename))
        if os.path.exists(dev_path):
             # Map filenames if they differ
             if filename == "lstm_ae_champion.pth": dev_path = dev_path.replace("lstm_ae_champion.pth", "lstm_ae.pth")
             
        # 3. Download from Hub
        try:
            print(f"📥 Downloading {filename} from {repo_id}...")
            return hf_hub_download(repo_id=repo_id, filename=filename)
        except Exception as e:
            print(f"❌ Failed to download {filename}: {e}")
            return None

    # Model Filenames in Hub
    lstm_path = get_model_path("lstm_ae_champion.pth")
    pca_path = get_model_path("pca_model.pkl")
    iso_path = get_model_path("iso_forest.pkl")
    
    # Load Models
    try:
        if lstm_path:
            lstm, config = load_lstm_model(lstm_path)
        else:
            lstm = None
    except Exception as e:
        print(f"⚠️ LSTM load failed: {e}")
        lstm = None
        
    try:
        if pca_path and iso_path:
             pca, iso = load_baseline_models(pca_path, iso_path)
        else:
             pca, iso = None, None
    except Exception as e:
        print(f"⚠️ Baseline load failed: {e}")
        pca, iso = None, None
        
    # Calculate Threshold
    if lstm:
        threshold = calculate_threshold(lstm)
    else:
        threshold = 0.15
        
    # Initialize Detector with Feature Names
    detector = TripleDetector(
        lstm, pca, iso, 
        thresholds={'lstm': threshold, 'pca': 0.1, 'iso': 0.0},
        feature_names=feature_cols
    )
    
    return detector, threshold

detector, threshold = load_resources(stream.feature_cols)

# --- 3. Session State ---
if 'history' not in st.session_state:
    st.session_state.history = {col: [] for col in stream.feature_cols}
    st.session_state.errors = []
    st.session_state.playing = False
    st.session_state.anomaly = None # {type, sensor, start_step}
    
# --- 4. Sidebar Controls ---
with st.sidebar:
    st.header("⚙️ Simulation Controls")
    
    col_play, col_reset = st.columns(2)
    with col_play:
        if st.button("⏸️ Pause" if st.session_state.playing else "▶️ Play"):
            st.session_state.playing = not st.session_state.playing
            # No rerun needed here, fragment will pick it up or we force a rerender of controls
            
    with col_reset:
        if st.button("🔄 Reset"):
            stream.reset()
            st.session_state.history = {col: [] for col in stream.feature_cols}
            st.session_state.errors = []
            st.session_state.anomaly = None
            st.rerun() # Force full reset
            
    st.divider()
    st.header("💉 Inject Anomaly")
    
    # Buttons to trigger anomalies
    if st.button("⚡ Spike (Tensile)"):
        st.session_state.anomaly = {
            'type': 'spike',
            'sensor': 0, # dynamic_tensile_strength
            'start': stream.current_idx
        }
    
    if st.button("📈 Drift (Ejection)"):
         st.session_state.anomaly = {
            'type': 'drift',
            'sensor': 1, # ejection
            'start': stream.current_idx
        }
        
    if st.button("❄️ Freeze (Speed)"):
         st.session_state.anomaly = {
            'type': 'freeze',
            'sensor': 2, # tbl_speed
            'start': stream.current_idx
        }

    st.divider()
    st.markdown("### 📊 Debug Info")
    st.write(f"Stream Idx: {stream.current_idx}")
    if st.session_state.anomaly:
        st.warning(f"Anomaly Active: {st.session_state.anomaly['type']}")

# --- 5. Main Layout ---
st.title("PharmaGuard Control Center")

# --- 6. The FRAGMENT (Auto-refreshing dashboard) ---
@st.fragment(run_every=0.5) # Updates every 500ms for balance
def run_dashboard():
    # Only update simulation if playing
    if st.session_state.playing:
        # 1. Injection Logic
        def inject_logic(values):
            if st.session_state.anomaly:
                return injector.apply(values, st.session_state.anomaly, stream.current_idx)
            return values
        
        # 2. Get Data
        sample = stream.next_sample(injection_callback=inject_logic)
        
        # 3. Update History
        for col, val in sample.items():
            st.session_state.history[col].append(val)
            if len(st.session_state.history[col]) > 100:
                 st.session_state.history[col].pop(0)
                 
        # 4. Inference
        window = stream.get_window()
        result = detector.predict(window)
        
        st.session_state.errors.append(result['lstm_error'])
        if len(st.session_state.errors) > 100:
            st.session_state.errors.pop(0)
            
        # Check Anomaly Duration
        if st.session_state.anomaly:
            elapsed = stream.current_idx - st.session_state.anomaly['start']
            if elapsed > 40:
                st.session_state.anomaly = None
        
        # --- RENDER ---
        
        # KPI Row
        status = "ANOMALY" if result['lstm_alert'] else "HEALTHY"
        health = max(0, 100 - (result['lstm_error'] * 1000))
        render_kpi_row(status, result['lstm_error'], health)
        
        # Charts Container
        with st.container():
            # Row 1: Sensors
            row1 = st.columns(3)
            sensors = stream.feature_cols[:3]
            for i, sensor in enumerate(sensors):
                with row1[i]:
                    st.plotly_chart(
                        create_sensor_plot(st.session_state.history, sensor), 
                        width="stretch",
                        key=f"chart_{sensor}"
                    )
                    
            row2 = st.columns(3)
            sensors_2 = stream.feature_cols[3:6]
            for i, sensor in enumerate(sensors_2):
                if i < len(row2):
                    with row2[i]:
                        st.plotly_chart(
                            create_sensor_plot(st.session_state.history, sensor), 
                            width="stretch",
                            key=f"chart_{sensor}"
                        )
                
            st.divider()
            
            # Row 3: Analysis
            st.markdown("### 🔍 Real-Time Analysis")
            col_err, col_consensus = st.columns([2, 1])
            
            with col_err:
                st.plotly_chart(
                    create_error_chart(st.session_state.errors, threshold), 
                    width="stretch",
                    key="chart_error"
                )
                
            with col_consensus:
                render_model_consensus(result['lstm_alert'], result['pca_alert'], result['iso_alert'])
                if 'attribution' in result:
                     st.plotly_chart(
                         create_attribution_chart(result['attribution'], stream.feature_cols), 
                         width="stretch",
                         key="chart_attrib"
                     )

    else:
        # Paused State (Static Render)
        render_kpi_row("PAUSED", 0.0, 100.0)
        st.info("⏸️ Simulation Paused. Press Play in sidebar to start.")

# Run the fragment
run_dashboard()
