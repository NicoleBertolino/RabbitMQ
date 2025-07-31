FROM python:3.10-slim

# Configurações essenciais
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Instala dependências do sistema para pika
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copia requirements do contexto raiz
COPY ./cluster_sync/requirements.txt .

# Instala dependências
RUN pip install --no-cache-dir -r requirements.txt pika

# Copia o código fonte
COPY ./cluster_sync/src ./src
COPY ./cluster_sync/config ./config

WORKDIR /app/src

CMD ["python", "cluster_sync.py"]