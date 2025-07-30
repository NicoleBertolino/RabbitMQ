# Usa uma imagem leve com Python 3.11
FROM python:3.11-slim

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copia as pastas de código e configuração para dentro da imagem
COPY config/ config/
COPY src/ src/
COPY utils/ utils/

# Copia o arquivo de dependências
COPY requirements.txt .

# Instala as dependências necessárias
RUN pip install --no-cache-dir -r requirements.txt

# Define uma variável de ambiente para o arquivo de configuração
ENV CONFIG_FILE=config/cluster_config.json

# Comando padrão para executar o processo do Cluster Sync
CMD ["python", "src/cluster_sync.py"]
