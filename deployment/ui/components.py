import streamlit as st

def render_kpi_row(system_status, current_mse, sensor_health):
    """
    Top row: 3-column metrics display with styling.
    """
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**SYSTEM STATUS**")
        if system_status == "ANOMALY":
            st.markdown('<div class="status-badge status-alert">🚨 ANOMALY ACTIVE</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="status-badge status-ok">✅ SYSTEM NORMAL</div>', unsafe_allow_html=True)
            
    with col2:
        st.metric("MSE Reconstruction", f"{current_mse:.4f}", delta_color="inverse")
        
    with col3:
        st.metric("Sensor Confidence", f"{sensor_health:.1f}%")

def render_model_consensus(lstm_alert, pca_alert, iso_alert):
    """
    Dashboard LED indicators for model consensus (Industrial Style).
    """
    st.markdown("### 🛡️ SYSTEM DIAGNOSTICS")
    col1, col2, col3 = st.columns(3)
    
    def led_html(label, sub, status):
        """
        Status: 'ok' (Cyan), 'alert' (Red), 'warning' (Amber)
        """
        if status == 'alert':
            color_class = "led-red"
        elif status == 'warning':
            color_class = "led-amber"
        else:
            color_class = "led-green" # Actually maps to Cyan in CSS now if we updated styles.css correctly, or we should update class name
            
        return f"""
        <div class="led-box">
            <div class="led {color_class}"></div>
            <div>
                <div class="led-label">{label}</div>
                <div class="led-sub">{sub}</div>
            </div>
        </div>
        """
    
    with col1:
        # LSTM is the critical path -> Red if alert
        status = "alert" if lstm_alert else "ok"
        st.markdown(led_html("LSTM-AE", "TEMPORAL SCAN", status), unsafe_allow_html=True)
    
    with col2:
        # PCA -> Red/Amber
        status = "alert" if pca_alert else "ok"
        st.markdown(led_html("PCA", "LINEAR CORRELATION", status), unsafe_allow_html=True)
    
    with col3:
        # IsoForest -> Amber (Warning) as per spec
        status = "warning" if iso_alert else "ok"
        st.markdown(led_html("ISO FOREST", "STATISTICAL OUTLIER", status), unsafe_allow_html=True)
