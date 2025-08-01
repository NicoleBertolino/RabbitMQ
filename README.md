# RabbitMQ
Trabalho de Sistemas Distribuidos

# 1. Limpeza prévia
docker compose down -v
docker system prune -a --volumes

# 2. Build com novo contexto
docker compose build --no-cache --progress plain

# 3. Execução
docker compose up -d

# 4. Comando para rodar custer_sync
docker logs -f cluster_sync
# 5. Comando para rodar client
docker logs -f client_1 