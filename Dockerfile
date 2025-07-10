FROM python:3.11-slim

WORKDIR /app

# Install tracer library first
COPY src /tmp/src
COPY pyproject.toml /tmp/
RUN pip install --no-cache-dir /tmp/

# Install app dependencies
COPY examples/streamlit_demo/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code and environment file
COPY examples/streamlit_demo .
COPY examples/streamlit_demo/.env .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]