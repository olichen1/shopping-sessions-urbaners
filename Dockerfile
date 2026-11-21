FROM --platform=linux/amd64 python:3.12-slim
RUN pip install --no-cache-dir duckdb==1.3.2 numpy==2.2.6 scipy==1.15.3
WORKDIR /app
COPY main.py /app/main.py
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/app/main.py"]
