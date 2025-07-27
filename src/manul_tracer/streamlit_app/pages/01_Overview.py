"""Overview page for Manul Tracer dashboard."""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from pathlib import Path

# Import analytics service
from manul_tracer.analytics import AnalyticsService

st.set_page_config(page_title="Overview - Manul Tracer", page_icon="📊", layout="wide")

def main():
    st.title("📊 Overview Dashboard")
    
    # Get database path
    DATABASE_PATH = os.getenv('MANUL_DATABASE_PATH')
    DEBUG_MODE = os.getenv('MANUL_DEBUG', 'false').lower() == 'true'
    
    if not DATABASE_PATH or not Path(DATABASE_PATH).exists():
        st.error("Database not configured or not found.")
        return
    
    try:
        analytics = AnalyticsService(DATABASE_PATH)
        overview_data = analytics.get_dashboard_overview()
        
        # Key metrics
        st.subheader("📈 Key Metrics")
        stats = overview_data['overview_stats']
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                "Total Traces", 
                stats['total_traces'],
                help="Total number of API calls traced"
            )
        
        with col2:
            st.metric(
                "Success Rate", 
                f"{stats['success_rate']:.1f}%",
                delta=f"+{stats['successful_traces']} success",
                help="Percentage of successful API calls"
            )
        
        with col3:
            st.metric(
                "Total Tokens", 
                f"{stats['total_tokens']:,}",
                help="Total tokens used across all traces"
            )
        
        with col4:
            st.metric(
                "Avg Latency", 
                f"{stats['avg_latency_ms']:.0f}ms",
                help="Average response time"
            )
        
        with col5:
            st.metric(
                "Unique Models", 
                stats['unique_models'],
                help="Number of different models used"
            )
        
        # Row 2 - Latency and Session metrics
        col1, col2, col3 = st.columns(3)
        
        latency_stats = overview_data['latency_stats']
        session_counts = overview_data['session_counts']
        
        with col1:
            st.metric(
                "P95 Latency",
                f"{latency_stats['p95_latency']:.0f}ms",
                help="95th percentile latency"
            )
        
        with col2:
            st.metric(
                "Total Sessions",
                session_counts['total_sessions'],
                help="Total number of traced sessions"
            )
        
        with col3:
            st.metric(
                "Active Sessions",
                session_counts['active_sessions'],
                help="Sessions that haven't been closed"
            )
        
        st.divider()
        
        # Recent Activity
        st.subheader("🕒 Recent Activity")
        
        recent_traces = overview_data['recent_traces']
        if recent_traces:
            df = pd.DataFrame(recent_traces)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Show recent traces table
            st.dataframe(
                df[['timestamp', 'model', 'total_tokens', 'latency_ms', 'success']],
                column_config={
                    'timestamp': st.column_config.DatetimeColumn('Time'),
                    'model': 'Model',
                    'total_tokens': st.column_config.NumberColumn('Tokens'),
                    'latency_ms': st.column_config.NumberColumn('Latency (ms)'),
                    'success': st.column_config.CheckboxColumn('Success')
                },
                use_container_width=True
            )
        else:
            st.info("No recent traces found.")
        
        st.divider()
        
        # Quick charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Success vs Failed Requests")
            if stats['total_traces'] > 0:
                success_data = {
                    'Status': ['Successful', 'Failed'],
                    'Count': [stats['successful_traces'], stats['failed_traces']]
                }
                fig = px.pie(
                    values=success_data['Count'], 
                    names=success_data['Status'],
                    color_discrete_map={'Successful': '#00CC88', 'Failed': '#FF6B6B'}
                )
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data available for chart.")
        
        with col2:
            st.subheader("🔍 Token Distribution")
            if stats['total_tokens'] > 0:
                token_data = {
                    'Type': ['Prompt Tokens', 'Completion Tokens'],
                    'Count': [stats['total_prompt_tokens'], stats['total_completion_tokens']]
                }
                fig = px.bar(
                    x=token_data['Type'],
                    y=token_data['Count'],
                    color=token_data['Type'],
                    color_discrete_map={
                        'Prompt Tokens': '#3B82F6', 
                        'Completion Tokens': '#8B5CF6'
                    }
                )
                fig.update_layout(height=300, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No token data available.")
                
        # System Health section
        st.divider()
        st.subheader("🚀 System Health")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if latency_stats['slow_requests'] == 0:
                st.success("✅ No slow requests (>5s)")
            else:
                st.warning(f"⚠️ {latency_stats['slow_requests']} slow requests detected")
        
        with col2:
            if stats['success_rate'] >= 95:
                st.success(f"✅ Excellent success rate ({stats['success_rate']:.1f}%)")
            elif stats['success_rate'] >= 90:
                st.warning(f"⚠️ Good success rate ({stats['success_rate']:.1f}%)")
            else:
                st.error(f"❌ Low success rate ({stats['success_rate']:.1f}%)")
        
        with col3:
            avg_tokens_per_second = stats.get('avg_tokens_per_second', 0)
            if avg_tokens_per_second > 0:
                st.info(f"⚡ Avg speed: {avg_tokens_per_second:.1f} tokens/sec")
            else:
                st.info("⚡ No speed data available")
        
    except Exception as e:
        st.error(f"Error loading overview data: {e}")
        if DEBUG_MODE:
            st.exception(e)

if __name__ == "__main__":
    main()