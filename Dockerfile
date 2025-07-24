# --- Base Image ---
FROM python:3.11-slim

# --- Environment Variables ---
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# --- Install Python Dependencies ---
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY otwin8_api.py .
COPY run_pipeline_pq.py .
COPY workbench_retrieval.py .
COPY gene_pathways_kegg.json .
COPY ecrf_extract_pq.py .
COPY twin_algo_pq.py .
COPY weights.json .
COPY column_subsets.json .
COPY .env .


# Expose the port the API runs on
EXPOSE 8001

# --- Command ---
CMD ["uvicorn", "otwin8_api:app", "--host", "0.0.0.0", "--port", "8001"] 