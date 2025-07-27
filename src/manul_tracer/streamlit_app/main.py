"""Main Streamlit app for Manul Tracer visualization."""

import os
import streamlit as st
from pathlib import Path

# Set page config
st.set_page_config(
    page_title="Manul Tracer",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Get database path from environment variable
DATABASE_PATH = os.getenv('MANUL_DATABASE_PATH')
DEBUG_MODE = os.getenv('MANUL_DEBUG', 'false').lower() == 'true'

def main():
    """Main app entry point."""
    st.title("🔍 Manul Tracer")
    st.markdown("OpenAI API Call Tracing and Analytics Dashboard")
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    
    # Database info in sidebar
    if DATABASE_PATH:
        st.sidebar.info(f"📊 Database: {Path(DATABASE_PATH).name}")
    else:
        st.sidebar.error("❌ No database configured")
        st.error("Database path not configured. Please use the CLI to start the app.")
        return
    
    # Check if database exists
    if not Path(DATABASE_PATH).exists():
        st.sidebar.error("❌ Database file not found")
        st.error(f"Database file not found: {DATABASE_PATH}")
        return
    
    # Debug info
    if DEBUG_MODE:
        st.sidebar.write("🐛 Debug mode enabled")
        with st.sidebar.expander("Debug Info"):
            st.write(f"Database: {DATABASE_PATH}")
            st.write(f"File exists: {Path(DATABASE_PATH).exists()}")
    
    # Main content
    st.markdown("""
    ## Welcome to Manul Tracer Dashboard
    
    Use the navigation in the sidebar to explore your OpenAI API traces:
    
    - **📊 Overview** - Key metrics and recent activity
    - **⚡ Performance** - Latency and token usage analysis  
    - **💬 Sessions and Traces** - Session-based trace exploration and analysis
    
    ---
    """)
    
    # Quick stats preview
    try:
        # Import here to avoid circular imports
        from manul_tracer.analytics import AnalyticsService
        
        analytics = AnalyticsService(DATABASE_PATH)
        overview = analytics.get_dashboard_overview()
        stats = overview['overview_stats']
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Traces", stats['total_traces'])
        
        with col2:
            st.metric("Success Rate", f"{stats['success_rate']:.1f}%")
        
        with col3:
            st.metric("Total Tokens", f"{stats['total_tokens']:,}")
        
        with col4:
            st.metric("Avg Latency", f"{stats['avg_latency_ms']:.0f}ms")
        
    except Exception as e:
        if DEBUG_MODE:
            st.error(f"Error loading stats: {e}")
        else:
            st.warning("Unable to load quick stats. Check the database connection.")


if __name__ == "__main__":
    main()