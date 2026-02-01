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
    st.session_state.system_active = False # Controls visibility (Stop vs Pause)
    st.session_state.anomaly = None # {type, sensor, start_step}
    st.session_state.last_result = None # Persistence for Pause state

# --- 4. Sidebar Controls (Fragmented) ---
@st.fragment
def render_sidebar_controls():
    st.header("⚙️ Simulation Controls")
    
    # Row 1: Play/Pause (Important action, full width or larger)
    label = "⏸️ PAUSE" if st.session_state.playing else "▶️ PLAY"
    if st.button(label, use_container_width=True):
        st.session_state.playing = not st.session_state.playing
        if st.session_state.playing:
            st.session_state.system_active = True 
        st.rerun()

    # Row 2: Stop | Reset (Secondary actions)
    col_stop, col_reset = st.columns(2)
    with col_stop:
        if st.button("⏹️ STOP", use_container_width=True):
            st.session_state.playing = False
            st.session_state.system_active = False
            st.session_state.last_result = None
            stream.reset()
            st.session_state.history = {col: [] for col in stream.feature_cols}
            st.session_state.errors = []
            st.session_state.anomaly = None
            st.rerun()

    with col_reset:
        if st.button("🔄 RESET", use_container_width=True):
            stream.reset()
            st.session_state.history = {col: [] for col in stream.feature_cols}
            st.session_state.errors = []
            st.session_state.anomaly = None
            st.session_state.last_result = None
            st.session_state.system_active = True 
            st.rerun()
            
    st.divider()
    st.header("💉 Inject Anomaly")
    
    # Buttons to trigger anomalies - All full width
    if st.button("⚡ SPIKE (TENSILE)", use_container_width=True):
        st.session_state.anomaly = {
            'type': 'spike',
            'sensor': 0, # dynamic_tensile_strength
            'start': stream.current_idx
        }
    
    if st.button("📈 DRIFT (EJECTION)", use_container_width=True):
         st.session_state.anomaly = {
            'type': 'drift',
            'sensor': 1, # ejection
            'start': stream.current_idx
        }
        
    if st.button("❄️ FREEZE (SPEED)", use_container_width=True):
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

with st.sidebar:
    render_sidebar_controls()

# --- 5. Main Layout ---
st.markdown("""
<div style="display: flex; align-items: center; gap: 10px; margin-bottom: 20px;">
    <h1 style="margin: 0; color: #f9fafb;">💊 PharmaGuard <span style="color: #3b82f6; font-size: 0.6em; vertical-align: middle;">AI CORE</span></h1>
</div>
""", unsafe_allow_html=True)

# --- 6. The FRAGMENT (Auto-refreshing dashboard) ---
@st.fragment(run_every=0.5) # Updates every 500ms for balance
def run_dashboard():
    
    # LOGIC UPDATE: We only UPDATE data if playing, but we RENDER if system_active
    
    if st.session_state.playing:
        # 1. Injection Logic
        def inject_logic(values):
            if st.session_state.anomaly:
                return injector.apply(values, st.session_state.anomaly, stream.current_idx)
            return values
        
        # 2. Get Data (Advance Time)
        sample = stream.next_sample(injection_callback=inject_logic)
        
        # 3. Update History
        for col, val in sample.items():
            st.session_state.history[col].append(val)
            if len(st.session_state.history[col]) > 100:
                 st.session_state.history[col].pop(0)
                 
        # 4. Inference
        window = stream.get_window()
        result = detector.predict(window)
        st.session_state.last_result = result # Persist specific inference result
        
        st.session_state.errors.append(result['lstm_error'])
        if len(st.session_state.errors) > 100:
            st.session_state.errors.pop(0)
            
        # Check Anomaly Duration
        if st.session_state.anomaly:
            elapsed = stream.current_idx - st.session_state.anomaly['start']
            if elapsed > 40:
                st.session_state.anomaly = None
    
    # --- RENDER LOGIC ---
    if st.session_state.system_active:
        # Use persisted result if available (covers Pause state)
        current_result = st.session_state.last_result
        
        if current_result:
            # LIVE or PAUSED state (Persisted)
            status = "ANOMALY" if current_result['lstm_alert'] else "HEALTHY"
            last_error = current_result['lstm_error']
        else:
            # RESET/EMPTY state
            last_error = 0.0
            status = "READY"
            
        health = max(0, 100 - (last_error * 1000))
        render_kpi_row(status, last_error, health)
        
        # ... (rest of render code)
        
        st.markdown("### 📡 Sensor Telemetry")
        # Charts Container
        with st.container(border=True):
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
                
        # Row 3: Analysis
        st.markdown("### 🔍 Real-Time Analysis")
        with st.container(border=True):
            col_err, col_consensus = st.columns([2, 1])
            
            with col_err:
                st.plotly_chart(
                    create_error_chart(st.session_state.errors, threshold), 
                    width="stretch",
                    key="chart_error"
                )
                
            with col_consensus:
                # Need alerts for consensus. If playing, use current_result.
                # If paused, we need to persist state or just show Safe?
                # For simplicity, if not playing, show all 'ok' or last state?
                # We didn't save alert history. Let's assume OK when paused for now or improve later.
                if current_result:
                     render_model_consensus(current_result['lstm_alert'], current_result['pca_alert'], current_result['iso_alert'])
                     if 'attribution' in current_result:
                         st.plotly_chart(
                             create_attribution_chart(current_result['attribution'], stream.feature_cols), 
                             width="stretch",
                             key="chart_attrib"
                         )
                else:
                     # Paused/Reset State
                     render_model_consensus(False, False, False)
                     if st.session_state.errors: # If we have data but paused, try to show something? 
                         pass 

    else:
        # STOP STATE (System Active = False)
        # Show specific "System Offline" UI
        st.info("🛑 SYSTEM OFFLINE. Press play to initialize PharmaGuard Core.")
        # Or a nice logo placeholder
        st.markdown("""
        <div style="text-align: center; padding: 50px; opacity: 0.5;">
            <h1>SYSTEM OFFLINE</h1>
            <p>Waiting for initialization...</p>
        </div>
        """, unsafe_allow_html=True)

# Run the fragment
run_dashboard()
