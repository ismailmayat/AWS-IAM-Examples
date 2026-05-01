FROM python:3.14-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY producer.py .

# Run as non-root user
RUN useradd -m -u 1000 producer && \
    chown -R producer:producer /app

USER producer

CMD ["python", "producer.py"]
