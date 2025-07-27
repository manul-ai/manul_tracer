"""Performance analysis page for Manul Tracer dashboard."""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# Import analytics service
from manul_tracer.analytics import AnalyticsService

st.set_page_config(page_title="Performance - Manul Tracer", page_icon="⚡", layout="wide")

def main():
    st.title("⚡ Performance Analysis")
    
    # Get database path
    DATABASE_PATH = os.getenv('MANUL_DATABASE_PATH')
    DEBUG_MODE = os.getenv('MANUL_DEBUG', 'false').lower() == 'true'
    
    if not DATABASE_PATH or not Path(DATABASE_PATH).exists():
        st.error("Database not configured or not found.")
        return
    
    try:
        analytics = AnalyticsService(DATABASE_PATH)
        performance_data = analytics.get_performance_data()
        
        # Model Performance Comparison
        st.subheader("🤖 Model Performance Comparison")
        
        token_usage = performance_data['token_usage_by_model']
        if token_usage:
            df_models = pd.DataFrame(token_usage)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Token Usage by Model**")
                fig = px.bar(
                    df_models, 
                    x='model', 
                    y='total_tokens',
                    color='model',
                    title="Total Tokens by Model"
                )
                fig.update_layout(showlegend=False, height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.write("**Average Tokens per Request**")
                fig = px.bar(
                    df_models, 
                    x='model', 
                    y='avg_tokens_per_trace',
                    color='model',
                    title="Avg Tokens per Request"
                )
                fig.update_layout(showlegend=False, height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            # Model performance table
            st.write("**Detailed Model Statistics**")
            st.dataframe(
                df_models,
                column_config={
                    'model': 'Model',
                    'trace_count': st.column_config.NumberColumn('Requests'),
                    'total_tokens': st.column_config.NumberColumn('Total Tokens'),
                    'prompt_tokens': st.column_config.NumberColumn('Prompt Tokens'),
                    'completion_tokens': st.column_config.NumberColumn('Completion Tokens'),
                    'avg_tokens_per_trace': st.column_config.NumberColumn('Avg Tokens/Request', format="%.1f")
                },
                use_container_width=True
            )
        else:
            st.info("No model performance data available.")
        
        st.divider()
        
        # Daily Trends
        st.subheader("📈 Usage Trends (Last 7 Days)")
        
        daily_trends = performance_data['daily_trends']
        if daily_trends:
            df_trends = pd.DataFrame(daily_trends)
            df_trends['date'] = pd.to_datetime(df_trends['date'])
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.line(
                    df_trends, 
                    x='date', 
                    y='total_requests',
                    title="Daily Request Volume",
                    markers=True
                )
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.line(
                    df_trends, 
                    x='date', 
                    y='total_tokens',
                    title="Daily Token Usage",
                    markers=True,
                    color_discrete_sequence=['#FF6B6B']
                )
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
            
            # Latency trend
            if 'avg_latency' in df_trends.columns:
                fig = px.line(
                    df_trends, 
                    x='date', 
                    y='avg_latency',
                    title="Daily Average Latency",
                    markers=True,
                    color_discrete_sequence=['#8B5CF6']
                )
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No daily trend data available.")
        
        st.divider()
        
        # Latency Analysis
        st.subheader("🕐 Latency Analysis")
        
        latency_stats = performance_data['latency_distribution']
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Average", f"{latency_stats['avg_latency']:.0f}ms")
        
        with col2:
            st.metric("Median", f"{latency_stats['median_latency']:.0f}ms")
        
        with col3:
            st.metric("95th Percentile", f"{latency_stats['p95_latency']:.0f}ms")
        
        with col4:
            st.metric("Slow Requests", f"{latency_stats['slow_requests']}")
        
        # Latency distribution chart (simulated since we don't have individual data points)
        if latency_stats['avg_latency'] > 0:
            st.write("**Latency Statistics**")
            
            # Create a simple metrics display
            metrics_data = {
                'Metric': ['Min', 'Average', 'Median', '95th Percentile', 'Max'],
                'Latency (ms)': [
                    latency_stats['min_latency'],
                    latency_stats['avg_latency'],
                    latency_stats['median_latency'],
                    latency_stats['p95_latency'],
                    latency_stats['max_latency']
                ]
            }
            
            fig = px.bar(
                x=metrics_data['Metric'],
                y=metrics_data['Latency (ms)'],
                title="Latency Distribution",
                color=metrics_data['Latency (ms)'],
                color_continuous_scale='viridis'
            )
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        
    except Exception as e:
        st.error(f"Error loading performance data: {e}")
        if DEBUG_MODE:
            st.exception(e)


if __name__ == "__main__":
    main()