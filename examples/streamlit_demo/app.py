import streamlit as st
import openai
import os

from datetime import datetime
from dotenv import load_dotenv
from manul_tracer import TracedClient

load_dotenv()

# Global traced client for statistics tracking
traced_client = TracedClient(proxies=os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY"))

def initialize_session_state():
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "session_id" not in st.session_state:
        st.session_state.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")

def get_openai_response(messages: list[dict[str, str]], api_key: str) -> str:
    client = openai.OpenAI(api_key=api_key, http_client=traced_client)
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
        
        st.header("API Tracing Stats")
        stats = traced_client.get_stats()
        st.write(f"Total Calls: {stats['total_calls']}")
        st.write(f"Successful Calls: {stats['successful_calls']}")
        st.write(f"Total Tokens: {stats['total_tokens']}")
        st.write(f"Avg Duration: {stats['average_duration']:.2f}s")
        
        if st.button("Clear Session"):
            st.session_state.messages = []
            st.session_state.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
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