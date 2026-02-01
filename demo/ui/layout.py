import streamlit as st
import os

def setup_page():
    """Configure Streamlit page settings and load CSS."""
    st.set_page_config(
        page_title="PharmaGuard Stress-Test",
        page_icon="💊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Load custom CSS
    css_path = os.path.join(os.path.dirname(__file__), "../assets/styles.css")
    if os.path.exists(css_path):
        with open(css_path) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    else:
        st.warning("⚠️ CSS file not found.")
