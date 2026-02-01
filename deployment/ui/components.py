import streamlit as st

def render_kpi_row(system_status, current_mse, sensor_health):
    """
    Top row: 3-column metrics display
    """
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**SYSTEM STATUS**")
        if system_status == "ANOMALY":
            st.markdown('<div class="alert-badge">🚨 ANOMALY DETECTED</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="ok-badge">✅ SYSTEM HEALTHY</div>', unsafe_allow_html=True)
            
    with col2:
        st.metric("MSE Score (Signal Quality)", f"{current_mse:.4f}")
        
    with col3:
        st.metric("Sensor Health Score", f"{sensor_health:.1f}%")

def render_model_consensus(lstm_alert, pca_alert, iso_alert):
    """
    Three LED indicators showing which models triggered.
    """
    st.markdown("### 🤖 Model Consensus")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        color = "🔴" if lstm_alert else "🟢"
        st.markdown(f"#### LSTM-AE {color}")
        st.caption("Temporal Patterns")
    
    with col2:
        color = "🔴" if pca_alert else "🟢"
        st.markdown(f"#### PCA {color}")
        st.caption("Linear Correlations")
    
    with col3:
        color = "🔴" if iso_alert else "🟢"
        st.markdown(f"#### IsoForest {color}")
        st.caption("Statistical Outliers")
