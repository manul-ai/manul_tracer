import streamlit as st
import openai
import os
import logging
import base64
from io import BytesIO
from PIL import Image

from datetime import datetime
from dotenv import load_dotenv
from manul_tracer import ManulTracer

logging.basicConfig(level=logging.INFO)
app_logger = logging.getLogger('streamlit_demo')

load_dotenv()

# Mock users for demo
MOCK_USERS = [
    {"user_id": "user_001", "username": "Fox", "email": "fox@example.com"},
    {"user_id": "user_002", "username": "Eagle", "email": "eagle@example.com"},
    {"user_id": "user_003", "username": "Bear", "email": "bear@example.com"},
    {"user_id": "user_004", "username": "Wolf", "email": "wolf@example.com"},
    {"user_id": "user_005", "username": "Owl", "email": "owl@example.com"},
]

def initialize_session_state():
    if "messages" not in st.session_state:
        st.session_state.messages = []
        app_logger.info("Initialized empty messages list")
        
    if "session_id" not in st.session_state:
        st.session_state.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        app_logger.info(f"Created new Streamlit session ID: {st.session_state.session_id}")
        
    if "current_user" not in st.session_state:
        # Set default user to the first one
        st.session_state.current_user = MOCK_USERS[0]
        app_logger.info(f"Set default user: {st.session_state.current_user['username']}")
    
    # Initialize selected_user if not exists (should match current_user)
    if "selected_user" not in st.session_state:
        st.session_state.selected_user = st.session_state.current_user
        
    if "tracer" not in st.session_state:
        user = st.session_state.current_user
        kwargs = {
            "session_id": st.session_state.session_id,
            "user_id": user["user_id"],
            "username": user["username"],
            "email": user["email"]
        }

        app_logger.info(f"Creating ManulTracer with session_id: {st.session_state.session_id} and user: {user['username']}")
        st.session_state.tracer = ManulTracer(**kwargs, database_file="databases/traces.db", auto_save=True)
        app_logger.info("ManulTracer created and stored in session state")
    
    if "uploaded_images" not in st.session_state:
        st.session_state.uploaded_images = []
    
    if "uploader_key" not in st.session_state:
        st.session_state.uploader_key = 0

def encode_image(image_file):
    """Encode image to base64 string."""
    return base64.b64encode(image_file.read()).decode('utf-8')

def get_openai_response(messages: list[dict], api_key: str, model: str = "gpt-4o") -> str:
    """Get response from OpenAI API with support for vision models."""
    client = openai.OpenAI(api_key=api_key, http_client=st.session_state.tracer.http_client)
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        max_tokens=1000,
        temperature=0.7
    )
    return response.choices[0].message.content

def format_message_for_api(text: str, images: list = None):
    """Format message with text and images for OpenAI API."""
    if not images:
        return {"role": "user", "content": text}
    
    content = [{"type": "text", "text": text}]
    
    for img_data in images:
        content.append({
            "type": "image_url",
            "image_url": {
                "url": f"data:image/{img_data['format']};base64,{img_data['base64']}"
            }
        })
    
    return {"role": "user", "content": content}

def display_message_with_images(message):
    """Display message with text and any associated images."""
    if isinstance(message["content"], str):
        st.markdown(message["content"])
    elif isinstance(message["content"], list):
        for item in message["content"]:
            if item["type"] == "text":
                st.markdown(item["text"])
            elif item["type"] == "image_url":
                # Extract base64 from data URL
                base64_str = item["image_url"]["url"].split(",")[1]
                image_data = base64.b64decode(base64_str)
                image = Image.open(BytesIO(image_data))
                st.image(image, width=300)

def main():
    st.set_page_config(page_title="LLM Chat App", page_icon="🤖", layout="wide")
    
    st.title("🤖 LLM Chat Application with Vision")
    st.markdown("Chat interface with OpenAI API supporting text and images")
    
    initialize_session_state()
    
    api_key = os.getenv("OPENAI_API_KEY")
    
    with st.sidebar:
        st.header("Session Info")
        st.write(f"Session ID: {st.session_state.session_id}")
        st.write(f"Messages: {len(st.session_state.messages)}")
        
        # User selection
        st.subheader("User")
        st.write(f"**Current User:** {st.session_state.current_user['username']}")
        
        user_options = {f"{u['username']} ({u['user_id']})": u for u in MOCK_USERS}
        current_user_label = f"{st.session_state.current_user['username']} ({st.session_state.current_user['user_id']})"
        selected_user_label = st.selectbox(
            "Select User",
            options=list(user_options.keys()),
            index=list(user_options.keys()).index(current_user_label),
            help="Select a user to associate with the tracing session"
        )
        selected_user = user_options[selected_user_label]
        
        # Model selection
        model = st.selectbox(
            "Model",
            ["gpt-4o", "gpt-4o-mini", "gpt-3.5-turbo"],
            help="Select a model. Vision models (gpt-4o) support image inputs."
        )
        
        # Display tracer session information
        if "tracer" in st.session_state:
            session_info = st.session_state.tracer.get_session_info()
            st.subheader("Tracer Stats")
            st.write(f"Total Requests: {session_info['total_requests']}")
            st.write(f"Total Tokens: {session_info['total_tokens']}")
            st.write(f"Successful: {session_info['successful_requests']}")
            st.write(f"Failed: {session_info['failed_requests']}")
            if session_info['created_at']:
                st.write(f"Session Started: {session_info['created_at']}")
        
        if st.button("Clear Session"):
            old_session_id = st.session_state.session_id
            app_logger.info(f"Clearing session {old_session_id}")
            
            # Update current user to selected user before clearing session
            if selected_user['user_id'] != st.session_state.current_user['user_id']:
                app_logger.info(f"Updating user from {st.session_state.current_user['username']} to {selected_user['username']} for new session")
                st.session_state.current_user = selected_user
            
            st.session_state.messages = []
            st.session_state.uploaded_images = []
            st.session_state.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            app_logger.info(f"New session ID: {st.session_state.session_id}")
            
            # Clear tracer so it gets recreated with new session_id and current user
            if "tracer" in st.session_state:
                app_logger.info(f"Removing old tracer for session {old_session_id}")
                del st.session_state.tracer
            st.rerun()
        
        if st.button("Refresh Stats"):
            app_logger.info(f"Refreshing stats for session {st.session_state.session_id}")
            
            # Check if user changed by comparing user_id
            if selected_user['user_id'] != st.session_state.current_user['user_id']:
                app_logger.info(f"User changed from {st.session_state.current_user['username']} to {selected_user['username']}")
                st.session_state.current_user = selected_user
                
                # Close old tracer
                if "tracer" in st.session_state and st.session_state.tracer:
                    st.session_state.tracer.close()
                    del st.session_state.tracer
                
                # Create new tracer with updated user
                user = st.session_state.current_user
                kwargs = {
                    "session_id": st.session_state.session_id,
                    "user_id": user["user_id"],
                    "username": user["username"],
                    "email": user["email"]
                }
                
                app_logger.info(f"Creating new ManulTracer with user: {user['username']}")
                st.session_state.tracer = ManulTracer(**kwargs, database_file="databases/traces.db", auto_save=True)
            
            st.rerun()
        
        st.divider()
        
        # Image upload in sidebar
        uploaded_files = st.file_uploader(
            "Upload images (optional)", 
            type=["png", "jpg", "jpeg", "gif", "webp"],
            accept_multiple_files=True,
            key=f"uploader_{st.session_state.uploader_key}"
        )
        
        # Process uploaded images
        if uploaded_files:
            st.session_state.uploaded_images = []
            for uploaded_file in uploaded_files:
                image = Image.open(uploaded_file)
                buffered = BytesIO()
                image_format = uploaded_file.type.split('/')[-1].upper()
                if image_format == 'JPG':
                    image_format = 'JPEG'
                image.save(buffered, format=image_format)
                img_str = base64.b64encode(buffered.getvalue()).decode()
                
                st.session_state.uploaded_images.append({
                    'base64': img_str,
                    'format': uploaded_file.type.split('/')[-1],
                    'name': uploaded_file.name,
                    'size_mb': len(buffered.getvalue()) / (1024 * 1024),
                    'width': image.width,
                    'height': image.height
                })
            
            # Show preview of uploaded images in sidebar
            if st.session_state.uploaded_images:
                st.write("**Images to send:**")
                for img_data in st.session_state.uploaded_images:
                    image_bytes = base64.b64decode(img_data['base64'])
                    image = Image.open(BytesIO(image_bytes))
                    st.image(image, caption=img_data['name'], width=150)
    
    if not api_key:
        st.warning("OPENAI_API_KEY not found. Please set it in your .env file.")
        st.stop()
    
    # Chat container
    chat_container = st.container()
    
    with chat_container:
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                display_message_with_images(message)
    
    # Chat input
    user_input = st.chat_input("Type your message here...")
    
    # Send message with images
    if user_input:
        # Format message with images
        user_message = format_message_for_api(user_input, st.session_state.uploaded_images)
        st.session_state.messages.append(user_message)
        
        # Display user message
        with st.chat_message("user"):
            display_message_with_images(user_message)
        
        # Get and display assistant response
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                response = get_openai_response(st.session_state.messages, api_key, model)
                st.markdown(response)
        
        st.session_state.messages.append({"role": "assistant", "content": response})
        
        # Clear uploaded images after sending and reset uploader
        st.session_state.uploaded_images = []
        st.session_state.uploader_key += 1
        st.rerun()

if __name__ == "__main__":
    main()