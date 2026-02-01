import plotly.graph_objects as go
import pandas as pd
import numpy as np

# Neon Industrial Theme Constants
COLOR_CYAN = '#00F0FF'       # Normal State
COLOR_RED = '#FF3838'        # Alert State (Neon Crimson)
COLOR_AMBER = '#FFB300'      # Warning State
COLOR_GRID = '#2D3748'       # Subtle Blue-Grey
FONT_FAMILY = "JetBrains Mono, monospace"

def create_sensor_plot(history_dict, sensor_name, anomaly_region=None):
    """
    Creates Plotly line chart with 'Laser Glow' effect.
    """
    values = history_dict.get(sensor_name, [])
    x_axis = list(range(len(values)))
    
    fig = go.Figure()
    
    # 1. The Glow Trace (Wide, Transparent)
    fig.add_trace(go.Scatter(
        x=x_axis, y=values, mode='lines', 
        name=sensor_name + '_glow',
        line=dict(color=COLOR_CYAN, width=6),
        opacity=0.2,
        hoverinfo='skip' # No tooltip for glow
    ))
    
    # 2. The Core Laser Trace (Thin, Bright)
    fig.add_trace(go.Scatter(
        x=x_axis, y=values, mode='lines', 
        name=sensor_name,
        line=dict(color=COLOR_CYAN, width=2),
        hovertemplate='<b>%{y:.4f}</b><extra></extra>' # Clean tooltip
    ))
    
    fig.update_layout(
        title=dict(text=sensor_name.replace("_", " ").upper(), font=dict(size=12, color='#A0AEC0')),
        margin=dict(l=10, r=10, t=30, b=10),
        height=180,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(showgrid=False, visible=False),
        yaxis=dict(showgrid=True, gridcolor=COLOR_GRID, zeroline=False),
        font=dict(color='#E2E8F0', family=FONT_FAMILY),
        uirevision='static', 
        showlegend=False,
        hoverlabel=dict(
            bgcolor=COLOR_GRID,
            bordercolor=COLOR_CYAN,
            font=dict(color=COLOR_CYAN, family=FONT_FAMILY)
        )
    )
    
    return fig

def create_error_chart(error_history, threshold):
    """
    MSE chart with Neon Red threshold and filled area.
    """
    fig = go.Figure()
    
    # Area Glow
    fig.add_trace(go.Scatter(
        y=error_history, 
        mode='lines', 
        name='MSE',
        line=dict(color='#10B981', width=2), # Emerald Green for base error
        fill='tozeroy',
        fillcolor='rgba(16, 185, 129, 0.1)',
        hovertemplate='MSE: <b>%{y:.5f}</b><extra></extra>'
    ))
    
    # Threshold Line (Neon Red)
    fig.add_hline(
        y=threshold, 
        line_dash="dot", 
        line_width=3,
        line_color=COLOR_RED,
        annotation_text="THRESHOLD (99.9%)",
        annotation_font=dict(color=COLOR_RED, family=FONT_FAMILY),
        annotation_position="top right"
    )
    
    fig.update_layout(
        title="RECONSTRUCTION ERROR (MSE)",
        margin=dict(l=20, r=20, t=30, b=20),
        height=220,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(showgrid=False, title="TIME WINDOW"),
        yaxis=dict(showgrid=True, gridcolor=COLOR_GRID),
        font=dict(color='#E2E8F0', family=FONT_FAMILY),
        uirevision='static',
        showlegend=False,
        hoverlabel=dict(
            bgcolor=COLOR_GRID,
            bordercolor='#10B981',
            font=dict(color='#10B981', family=FONT_FAMILY)
        )
    )
    
    return fig

def create_attribution_chart(attribution_vector, sensor_names):
    """
    Root Cause Analysis (RCA) - Horizontal Bars
    """
    df = pd.DataFrame({
        'sensor': [s.replace("_", " ").upper() for s in sensor_names],
        'error': attribution_vector
    }).sort_values('error', ascending=True)
    
    fig = go.Figure(go.Bar(
        x=df['error'],
        y=df['sensor'],
        orientation='h',
        marker=dict(
            color=df['error'],
            colorscale=[[0, COLOR_CYAN], [1, COLOR_RED]], # Gradient from Cyan to Red based on severity
            line=dict(width=0)
        ),
        hovertemplate='Contrib: <b>%{x:.4f}</b><extra></extra>'
    ))
    
    fig.update_layout(
        title="ROOT CAUSE ANALYSIS",
        margin=dict(l=20, r=20, t=30, b=20),
        height=220,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(showgrid=True, gridcolor=COLOR_GRID, title="CONTRIBUTION"),
        yaxis=dict(showgrid=False, tickfont=dict(color='#A0AEC0', size=10)),
        font=dict(color='#E2E8F0', family=FONT_FAMILY),
        uirevision='static',
        hoverlabel=dict(
            bgcolor=COLOR_GRID,
            bordercolor=COLOR_RED,
            font=dict(color='#E2E8F0', family=FONT_FAMILY)
        )
    )
    
    return fig
