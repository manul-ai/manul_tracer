import streamlit as st
import openai
import os
import logging

from datetime import datetime
from dotenv import load_dotenv
from manul_tracer import ManulTracer

logging.basicConfig(level=logging.INFO)
app_logger = logging.getLogger('streamlit_demo')

load_dotenv()

def initialize_session_state():
    if "messages" not in st.session_state:
        st.session_state.messages = []
        app_logger.info("Initialized empty messages list")
        
    if "session_id" not in st.session_state:
        st.session_state.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        app_logger.info(f"Created new Streamlit session ID: {st.session_state.session_id}")
        
    if "tracer" not in st.session_state:
        kwargs = {"session_id": st.session_state.session_id}

        app_logger.info(f"Creating ManulTracer with session_id: {st.session_state.session_id}")
        st.session_state.tracer = ManulTracer(**kwargs)
        app_logger.info("ManulTracer created and stored in session state")

def get_openai_response(messages: list[dict[str, str]], api_key: str) -> str:
    client = openai.OpenAI(api_key=api_key, http_client=st.session_state.tracer.http_client)
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=messages,
        max_tokens=1000,
        temperature=0.7
    )
    return response.choices[0].message.content



def main():
    st.set_page_config(page_title="LLM Chat App", page_icon="🤖", layout="wide")
    
    st.title("🤖 LLM Chat Application")
    st.markdown("A simple chat interface with OpenAI API and session management")
    
    initialize_session_state()
    
    api_key = os.getenv("OPENAI_API_KEY")
    
    with st.sidebar:
        st.header("Session Info")
        st.write(f"Session ID: {st.session_state.session_id}")
        st.write(f"Messages: {len(st.session_state.messages)}")
        
        # Display tracer session information
        if "tracer" in st.session_state:
            session_info = st.session_state.tracer.get_session_info()
            st.subheader("Tracer Stats")
            st.write(f"Total Requests: {session_info['total_requests']}")
            st.write(f"Total Tokens: {session_info['total_tokens']}")
            st.write(f"Successful: {session_info['successful_requests']}")
            st.write(f"Failed: {session_info['failed_requests']}")
            if session_info['session_created_at']:
                st.write(f"Session Started: {session_info['session_created_at']}")
        
        if st.button("Clear Session"):
            old_session_id = st.session_state.session_id
            app_logger.info(f"Clearing session {old_session_id}")
            
            st.session_state.messages = []
            st.session_state.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            app_logger.info(f"New session ID: {st.session_state.session_id}")
            
            # Clear tracer so it gets recreated with new session_id
            if "tracer" in st.session_state:
                app_logger.info(f"Removing old tracer for session {old_session_id}")
                del st.session_state.tracer
            st.rerun()
        
        if st.button("Refresh Stats"):
            app_logger.info(f"Refreshing stats for session {st.session_state.session_id}")
            st.rerun()
    
    if not api_key:
        st.warning("OPENAI_API_KEY not found. Please set it in your .env file.")
        st.stop()
    
    chat_container = st.container()
    
    with chat_container:
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
    
    if prompt := st.chat_input("Type your message here..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        with st.chat_message("user"):
            st.markdown(prompt)
        
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                response = get_openai_response(st.session_state.messages, api_key)
                st.markdown(response)
        
        st.session_state.messages.append({"role": "assistant", "content": response})

if __name__ == "__main__":
    main()