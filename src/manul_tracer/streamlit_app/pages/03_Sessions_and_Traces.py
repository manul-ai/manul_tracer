"""Sessions and Traces page for Manul Tracer dashboard."""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# Import analytics service
from manul_tracer.analytics import AnalyticsService
from manul_tracer.database.repositories.trace_repository import TraceRepository

st.set_page_config(page_title="Sessions & Traces - Manul Tracer", page_icon="💬", layout="wide")

def main():
    st.title("💬 Sessions and Traces")
    
    # Get database path
    DATABASE_PATH = os.getenv('MANUL_DATABASE_PATH')
    DEBUG_MODE = os.getenv('MANUL_DEBUG', 'false').lower() == 'true'
    
    if not DATABASE_PATH or not Path(DATABASE_PATH).exists():
        st.error("Database not configured or not found.")
        return
    
    try:
        analytics = AnalyticsService(DATABASE_PATH)
        trace_repo = TraceRepository(DATABASE_PATH)
        session_data = analytics.get_session_analytics()
        
        # Session Summary
        st.subheader("📊 Session Summary")
        
        summary = session_data['summary']
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Sessions", summary['total_sessions'])
        
        with col2:
            avg_duration = summary['avg_duration']
            if avg_duration > 0:
                if avg_duration > 3600:
                    duration_str = f"{avg_duration/3600:.1f}h"
                elif avg_duration > 60:
                    duration_str = f"{avg_duration/60:.1f}m"
                else:
                    duration_str = f"{avg_duration:.0f}s"
                st.metric("Avg Duration", duration_str)
            else:
                st.metric("Avg Duration", "N/A")
        
        with col3:
            st.metric("Avg Traces/Session", f"{summary['avg_traces_per_session']:.1f}")
        
        st.divider()
        
        # Session Selection
        st.subheader("🔍 Select Session")
        
        sessions = session_data['sessions']
        if not sessions:
            st.info("No sessions found.")
            return
        
        df_sessions = pd.DataFrame(sessions)
        
        # Session filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            session_types = df_sessions['session_type'].dropna().unique()
            selected_type = st.selectbox(
                "Filter by Session Type", 
                options=['All'] + list(session_types),
                index=0
            )
        
        with col2:
            min_traces = st.number_input(
                "Minimum Traces", 
                min_value=0, 
                value=0, 
                help="Show only sessions with at least this many traces"
            )
        
        with col3:
            # Search by session ID
            session_search = st.text_input("Search Session ID", help="Enter part of session ID")
        
        # Apply filters
        filtered_df = df_sessions.copy()
        if selected_type != 'All':
            filtered_df = filtered_df[filtered_df['session_type'] == selected_type]
        if min_traces > 0:
            filtered_df = filtered_df[filtered_df['trace_count'] >= min_traces]
        if session_search:
            filtered_df = filtered_df[filtered_df['session_id'].str.contains(session_search, case=False, na=False)]
        
        # Session selection
        if not filtered_df.empty:
            selected_session = st.selectbox(
                "Choose a session to explore:",
                options=filtered_df['session_id'].tolist(),
                format_func=lambda x: f"{x[:8]}... ({filtered_df[filtered_df['session_id']==x]['trace_count'].iloc[0]} traces, {filtered_df[filtered_df['session_id']==x]['total_tokens'].iloc[0]:,} tokens)"
            )
            
            if selected_session:
                st.divider()
                
                # Session Details
                session_info = filtered_df[filtered_df['session_id'] == selected_session].iloc[0]
                
                st.subheader(f"📋 Session Details: {selected_session[:12]}...")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**Session Information**")
                    st.write(f"**ID:** {session_info['session_id']}")
                    st.write(f"**Type:** {session_info.get('session_type', 'N/A')}")
                    st.write(f"**Created:** {session_info.get('created_at', 'N/A')}")
                    st.write(f"**Ended:** {session_info.get('ended_at', 'Active')}")
                
                with col2:
                    st.write("**Performance Metrics**")
                    st.write(f"**Traces:** {session_info['trace_count']}")
                    st.write(f"**Total Tokens:** {session_info['total_tokens']:,}")
                    st.write(f"**Success Rate:** {session_info['success_rate']:.1f}%")
                    if session_info.get('duration_seconds'):
                        duration = session_info['duration_seconds']
                        if duration > 3600:
                            duration_str = f"{duration/3600:.1f} hours"
                        elif duration > 60:
                            duration_str = f"{duration/60:.1f} minutes"
                        else:
                            duration_str = f"{duration:.0f} seconds"
                        st.write(f"**Duration:** {duration_str}")
                
                st.divider()
                
                # Traces in Session
                st.subheader("🔍 Traces in Session")
                
                # Additional trace filters
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    model_filter = st.selectbox("Filter by Model", options=['All', 'gpt-4', 'gpt-3.5-turbo', 'gpt-4o', 'o1-preview'], index=0, key="model_filter")
                
                with col2:
                    success_filter = st.selectbox("Filter by Status", options=['All', 'Success', 'Failed'], index=0, key="success_filter")
                
                with col3:
                    sort_order = st.selectbox("Sort by", options=['Newest First', 'Oldest First'], index=0)
                
                # Get traces for this session
                try:
                    traces = analytics.search_traces(
                        session_id=selected_session,
                        model=model_filter if model_filter != 'All' else None,
                        success=True if success_filter == 'Success' else False if success_filter == 'Failed' else None
                    )
                    
                    if traces:
                        df_traces = pd.DataFrame(traces)
                        df_traces['timestamp'] = pd.to_datetime(df_traces['timestamp'])
                        
                        # Sort traces
                        if sort_order == 'Newest First':
                            df_traces = df_traces.sort_values('timestamp', ascending=False)
                        else:
                            df_traces = df_traces.sort_values('timestamp', ascending=True)
                        
                        # Summary metrics for filtered traces
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            st.metric("Traces", len(traces))
                        
                        with col2:
                            success_rate = (sum(1 for t in traces if t['success']) / len(traces)) * 100
                            st.metric("Success Rate", f"{success_rate:.1f}%")
                        
                        with col3:
                            total_tokens = sum(t['total_tokens'] for t in traces if t['total_tokens'])
                            st.metric("Total Tokens", f"{total_tokens:,}")
                        
                        with col4:
                            avg_latency = sum(t['latency_ms'] for t in traces if t['latency_ms']) / len([t for t in traces if t['latency_ms']])
                            st.metric("Avg Latency", f"{avg_latency:.0f}ms")
                        
                        # Traces table with selection
                        st.write("**Select a trace to view details:**")
                        selected_indices = st.dataframe(
                            df_traces[['trace_id', 'timestamp', 'model_id', 'total_tokens', 'latency_ms', 'success', 'error_message']],
                            column_config={
                                'trace_id': st.column_config.TextColumn('Trace ID', width='small'),
                                'timestamp': st.column_config.DatetimeColumn('Time'),
                                'model_id': 'Model ID',
                                'total_tokens': st.column_config.NumberColumn('Tokens'),
                                'latency_ms': st.column_config.NumberColumn('Latency (ms)'),
                                'success': st.column_config.CheckboxColumn('Success'),
                                'error_message': st.column_config.TextColumn('Error Message', width='medium')
                            },
                            use_container_width=True,
                            on_select="rerun",
                            selection_mode="single-row"
                        )
                        
                        st.divider()
                        
                        # Trace Detail View
                        st.subheader("🔍 Trace Details")
                        
                        if selected_indices.selection.rows:
                            selected_idx = selected_indices.selection.rows[0]
                            selected_trace_id = df_traces.iloc[selected_idx]['trace_id']
                            
                            # Get full trace details
                            try:
                                full_trace = trace_repo.read(selected_trace_id)
                                
                                if full_trace:
                                    # Trace metadata
                                    col1, col2 = st.columns(2)
                                    
                                    with col1:
                                        st.write("**Trace Information**")
                                        st.write(f"**ID:** {full_trace.trace_id}")
                                        st.write(f"**Session:** {full_trace.session_id}")
                                        st.write(f"**Model ID:** {full_trace.model_id}")
                                        if hasattr(full_trace, 'provider'):
                                            st.write(f"**Provider:** {full_trace.provider}")
                                        st.write(f"**Success:** {'✅' if full_trace.success else '❌'}")
                                        if not full_trace.success and full_trace.error_message:
                                            st.error(f"**Error:** {full_trace.error_message}")
                                    
                                    with col2:
                                        st.write("**Performance Metrics**")
                                        st.write(f"**Total Tokens:** {full_trace.total_tokens or 0:,}")
                                        st.write(f"**Prompt Tokens:** {full_trace.prompt_tokens or 0:,}")
                                        st.write(f"**Completion Tokens:** {full_trace.completion_tokens or 0:,}")
                                        st.write(f"**Latency:** {full_trace.total_latency_ms or 0:.0f}ms")
                                        if full_trace.tokens_per_second:
                                            st.write(f"**Speed:** {full_trace.tokens_per_second:.1f} tokens/sec")
                                    
                                    # API Parameters
                                    if any([full_trace.temperature, full_trace.max_tokens, full_trace.top_p]):
                                        st.write("**API Parameters**")
                                        params_col1, params_col2, params_col3 = st.columns(3)
                                        
                                        with params_col1:
                                            if full_trace.temperature is not None:
                                                st.write(f"**Temperature:** {full_trace.temperature}")
                                        
                                        with params_col2:
                                            if full_trace.max_tokens is not None:
                                                st.write(f"**Max Tokens:** {full_trace.max_tokens}")
                                        
                                        with params_col3:
                                            if full_trace.top_p is not None:
                                                st.write(f"**Top P:** {full_trace.top_p}")
                                    
                                    # Conversation
                                    if full_trace.full_conversation:
                                        # Check if trace contains images using has_images field
                                        has_images = any(
                                            getattr(msg, 'has_images', False)
                                            for msg in full_trace.full_conversation
                                        )
                                        
                                        conversation_title = "**Conversation**"
                                        if has_images:
                                            conversation_title += " 🖼️"  # Add image indicator
                                        
                                        st.write(conversation_title)
                                        
                                        for i, message in enumerate(full_trace.full_conversation):
                                            role_emoji = {
                                                'system': '⚙️',
                                                'user': '👤', 
                                                'assistant': '🤖',
                                                'tool': '🔧'
                                            }.get(message.role, '💬')
                                            
                                            # Check if this message contains images using has_images field
                                            message_has_images = getattr(message, 'has_images', False)
                                            
                                            message_title = f"{role_emoji} {message.role.title()} Message {i+1}"
                                            if message_has_images:
                                                message_title += " 🖼️"
                                            
                                            with st.expander(message_title):
                                                # Display message content
                                                if isinstance(message.content, list):
                                                    # Handle vision API format with content array
                                                    for content_item in message.content:
                                                        if isinstance(content_item, dict):
                                                            if content_item.get('type') == 'text':
                                                                st.write(content_item.get('text', ''))
                                                            elif content_item.get('type') == 'image_url':
                                                                st.write("📷 **Image attached**")
                                                                
                                                                # Display image metadata if available
                                                                image_url = content_item.get('image_url', {})
                                                                if isinstance(image_url, dict) and 'image_id' in image_url:
                                                                    # New format with metadata
                                                                    st.write("**Image Metadata:**")
                                                                    
                                                                    # Create metadata table
                                                                    metadata_data = []
                                                                    if image_url.get('format'):
                                                                        metadata_data.append(["Format", image_url['format']])
                                                                    if image_url.get('size_mb'):
                                                                        metadata_data.append(["Size", f"{image_url['size_mb']:.2f} MB"])
                                                                    if image_url.get('width') and image_url.get('height'):
                                                                        metadata_data.append(["Dimensions", f"{image_url['width']} × {image_url['height']}"])
                                                                    if image_url.get('hash'):
                                                                        metadata_data.append(["Hash", image_url['hash'][:12] + "..."])
                                                                    if image_url.get('image_id'):
                                                                        metadata_data.append(["Image ID", image_url['image_id'][:8] + "..."])
                                                                    
                                                                    if metadata_data:
                                                                        df_metadata = pd.DataFrame(metadata_data, columns=["Property", "Value"])
                                                                        st.table(df_metadata)
                                                                else:
                                                                    # Old format or unprocessed image
                                                                    st.caption("Image metadata not available")
                                                        else:
                                                            # Handle non-dict content items
                                                            st.write(str(content_item))
                                                elif isinstance(message.content, str):
                                                    # Simple text message
                                                    st.write(message.content)
                                                else:
                                                    # Handle other content types (should not happen)
                                                    st.write(str(message.content) if message.content else "No content")
                                                
                                                if message.token_count:
                                                    st.caption(f"Tokens: {message.token_count}")
                                    
                                    # Raw data (debug)
                                    if DEBUG_MODE:
                                        with st.expander("🔍 Raw Trace Data (Debug)"):
                                            st.json(full_trace.to_dict())
                                
                                else:
                                    st.error("Trace not found in database.")
                            
                            except Exception as e:
                                st.error(f"Error loading trace details: {e}")
                                if DEBUG_MODE:
                                    st.exception(e)
                        
                        else:
                            st.info("Select a trace from the table above to view details.")
                    
                    else:
                        st.info("No traces found for this session with the selected filters.")
                
                except Exception as e:
                    st.error(f"Error loading session traces: {e}")
                    if DEBUG_MODE:
                        st.exception(e)
        
        else:
            st.info("No sessions match the selected filters.")
        
    except Exception as e:
        st.error(f"Error loading session data: {e}")
        if DEBUG_MODE:
            st.exception(e)


if __name__ == "__main__":
    main()