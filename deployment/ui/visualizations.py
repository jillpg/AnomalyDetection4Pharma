import plotly.graph_objects as go
import pandas as pd
import numpy as np

def create_sensor_plot(history_dict, sensor_name, anomaly_region=None):
    """
    Creates Plotly line chart for a single sensor.
    Args:
        history_dict: dict {sensor: [values]}
        sensor_name: e.g., "dynamic_tensile_strength"
        anomaly_region: tuple (start_idx, end_idx) for red shading (Not implemented for MVP)
    """
    values = history_dict.get(sensor_name, [])
    # Determine X axis (relative time)
    x_axis = list(range(len(values)))
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=x_axis, 
        y=values, 
        mode='lines', 
        name=sensor_name,
        line=dict(color='#00d9ff', width=2)
    ))
    
    fig.update_layout(
        title=dict(text=sensor_name.replace("_", " ").title(), font=dict(size=14)),
        margin=dict(l=20, r=20, t=30, b=20),
        height=200,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(showgrid=False, visible=False),
        yaxis=dict(showgrid=True, gridcolor='#333'),
        font=dict(color='#e0e6ff'),
        uirevision='static', # Prevent reset on update
    )
    
    return fig

def create_error_chart(error_history, threshold):
    """
    Line chart showing MSE over time with persistent red threshold line.
    """
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        y=error_history, 
        mode='lines', 
        name='Reconstruction Error',
        line=dict(color='#00ff88', width=2),
        fill='tozeroy',
        fillcolor='rgba(0, 255, 136, 0.1)'
    ))
    
    # Threshold line
    fig.add_hline(
        y=threshold, 
        line_dash="dash", 
        line_color="#ff4444", 
        annotation_text="99.9% Threshold",
        annotation_font=dict(color="#ff4444"),
        annotation_position="bottom right"
    )
    
    fig.update_layout(
        title="Anomaly Score (MSE)",
        margin=dict(l=20, r=20, t=40, b=20),
        height=250,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(showgrid=False, title="Time"),
        yaxis=dict(showgrid=True, gridcolor='#333'),
        font=dict(color='#e0e6ff'),
        uirevision='static', # Prevent reset on update
    )
    
    return fig

def create_attribution_chart(attribution_vector, sensor_names):
    """
    Horizontal bar chart showing per-sensor reconstruction error.
    Args:
        attribution_vector: np.array of error values
        sensor_names: list of sensor strings
    """
    # Create DF for sorting
    df = pd.DataFrame({
        'sensor': sensor_names,
        'error': attribution_vector
    }).sort_values('error', ascending=True)
    
    fig = go.Figure(go.Bar(
        x=df['error'],
        y=df['sensor'],
        orientation='h',
        marker=dict(color='#ff4444', line=dict(width=1, color='#ffebea')) # Red bars with white border
    ))
    
    fig.update_layout(
        title="Root Cause Analysis (Contribution)",
        margin=dict(l=20, r=20, t=40, b=20),
        height=250,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(showgrid=True, gridcolor='#333', title="Reconstruction Error"),
        yaxis=dict(showgrid=False, tickfont=dict(color='#e0e6ff')),
        font=dict(color='#e0e6ff'),
        uirevision='static', # Stabilize updates
    )
    
    return fig
