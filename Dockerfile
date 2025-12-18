FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY pyproject.toml ./
RUN pip install --no-cache-dir -e .

# Copy application code
COPY *.py ./

# Expose port for web interface
EXPOSE 5000

# Run the Streamlit application
CMD ["streamlit", "run", "web_app.py", "--server.port=5000", "--server.address=0.0.0.0"]
