import pika
import json
import time
import uuid
import os
import random
from threading import Thread

# Identificação do processo
SYNC_ID = os.getenv("SYNC_ID", "sync_1")

# Configurações do RabbitMQ
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "Nicole")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "nicole123")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "r_queue")

# Conexão com RabbitMQ
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

# Estado local
acquire_list = []
release_list = []
pending_requests = []

print(f"[{SYNC_ID}] Iniciado e ouvindo a fila '{RABBITMQ_QUEUE}'.")

def posso_entrar_na_secao_critica(meu_pedido):
    for acq in acquire_list:
        if acq["request_id"] == meu_pedido["request_id"]:
            return True
        if acq["request_id"] not in release_list:
            return False
    return False

def processar_fila():
    while True:
        time.sleep(0.1)
        for req in list(pending_requests):
            if posso_entrar_na_secao_critica(req):
                entrar_secao_critica(req)
                pending_requests.remove(req)

def entrar_secao_critica(req):
    print(f"[{SYNC_ID}] Entrando na seção crítica para {req['client_id']} ({req['request_id']})")
    time.sleep(random.uniform(0.2, 1.0))  # Simula escrita
    release_msg = {
        "client_id": req["client_id"],
        "timestamp": req["timestamp"],
        "request_id": req["request_id"],
        "primitiva": "RELEASE"
    }
    channel.basic_publish(
        exchange='',
        routing_key=RABBITMQ_QUEUE,
        body=json.dumps(release_msg)
    )
    release_list.append(req["request_id"])
    print(f"[{SYNC_ID}] RELEASE publicado para {req['request_id']}")

    # Envia COMMITTED de volta ao cliente
    if "reply_to" in req and "correlation_id" in req:
        committed = {"status": "COMMITTED", "request_id": req["request_id"]}
        channel.basic_publish(
            exchange='',
            routing_key=req["reply_to"],
            properties=pika.BasicProperties(
                correlation_id=req["correlation_id"],
                content_type='application/json'
            ),
            body=json.dumps(committed)
        )
        print(f"[{SYNC_ID}] COMMITTED enviado a {req['client_id']}")

# Inicia thread paralela que processa a fila local
Thread(target=processar_fila, daemon=True).start()

# Callback que trata todas as mensagens recebidas da fila compartilhada
def on_message(ch, method, properties, body):
    msg = json.loads(body.decode())
    primitiva = msg.get("primitiva")
    request_id = msg.get("request_id")

    if primitiva == "ACQUIRE":
        acquire_list.append(msg)
        msg["reply_to"] = properties.reply_to
        msg["correlation_id"] = properties.correlation_id
        pending_requests.append(msg)
        print(f"[{SYNC_ID}] ACQUIRE recebido de {msg['client_id']}")
    elif primitiva == "RELEASE":
        release_list.append(request_id)
        print(f"[{SYNC_ID}] RELEASE registrado de {msg['client_id']}")

channel.basic_consume(
    queue=RABBITMQ_QUEUE,
    on_message_callback=on_message,
    auto_ack=True
)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    print("Encerrando processo...")
    connection.close()
