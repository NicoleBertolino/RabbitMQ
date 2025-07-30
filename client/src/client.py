import pika
import json
import uuid
import random
import time
import os
from datetime import datetime

# Carregar configuração
CONFIG_FILE = os.getenv("CONFIG_FILE", "../config/client_config.json")
with open(CONFIG_FILE, "r") as f:
    config = json.load(f)

CLIENT_ID = config["client_id"]
CLUSTER_SYNC = config["cluster_sync_host"]
ACCESS_COUNT = config["access_count"]
SLEEP_MIN = config["sleep_min"]
SLEEP_MAX = config["sleep_max"]

RABBITMQ_CONF = config["rabbitmq"]

# Estabelece conexão com RabbitMQ
credentials = pika.PlainCredentials(
    RABBITMQ_CONF["username"], RABBITMQ_CONF["password"]
)
parameters = pika.ConnectionParameters(
    host=RABBITMQ_CONF["host"],
    port=RABBITMQ_CONF["port"],
    credentials=credentials,
    heartbeat=600,
    blocked_connection_timeout=300,
)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Garante que a fila exista (caso o Cluster ainda não tenha criado)
channel.queue_declare(queue=RABBITMQ_CONF["queue"], durable=True)

# Cria uma fila exclusiva para receber COMMITTED (respostas)
reply_queue = channel.queue_declare(queue='', exclusive=True)
reply_queue_name = reply_queue.method.queue

# Callback que trata respostas COMMITTED
response_received = False
def on_response(ch, method, props, body):
    global response_received
    if props.correlation_id == current_correlation_id:
        print(f"[{CLIENT_ID}] COMMITTED recebido: {body.decode()}")
        response_received = True

channel.basic_consume(
    queue=reply_queue_name,
    on_message_callback=on_response,
    auto_ack=True
)

# Envia pedidos de acesso
for i in range(ACCESS_COUNT):
    request_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().isoformat()
    message = {
        "client_id": CLIENT_ID,
        "timestamp": timestamp,
        "request_id": request_id,
        "primitiva": "ACQUIRE"
    }

    current_correlation_id = str(uuid.uuid4())

    # Publica pedido para a fila
    channel.basic_publish(
        exchange=RABBITMQ_CONF["exchange"],
        routing_key=RABBITMQ_CONF["queue"],
        properties=pika.BasicProperties(
            reply_to=reply_queue_name,
            correlation_id=current_correlation_id,
            content_type='application/json'
        ),
        body=json.dumps(message)
    )

    print(f"[{CLIENT_ID}] Pedido {i+1}/{ACCESS_COUNT} enviado: {message['timestamp']}")

    # Aguarda resposta COMMITTED
    response_received = False
    while not response_received:
        connection.process_data_events(time_limit=1)

    # Dorme entre 1 e 5 segundos
    wait_time = random.randint(SLEEP_MIN, SLEEP_MAX)
    print(f"[{CLIENT_ID}] Dormindo por {wait_time}s\n")
    time.sleep(wait_time)

print(f"[{CLIENT_ID}] Finalizou os {ACCESS_COUNT} pedidos.")
connection.close()
