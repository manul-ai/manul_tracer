FROM python:3.11-slim

WORKDIR /app

# Install tracer library first
COPY tracer /tmp/tracer
RUN pip install --no-cache-dir /tmp/tracer

# Install app dependencies
COPY sample_app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code and environment file
COPY sample_app .
COPY sample_app/.env .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]