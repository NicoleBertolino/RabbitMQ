import pika
import json
import time
import random
import os
from threading import Thread
from functools import partial

class ClusterSync:
    def __init__(self):
        self.SYNC_ID = os.getenv("SYNC_ID", "sync_1")
        self.setup_rabbitmq()
        self.state = {
            'acquire_list': [],
            'release_list': [],
            'pending_requests': []
        }
        print(f"[{self.SYNC_ID}] Iniciado e ouvindo a fila 'r_queue'")

    def setup_rabbitmq(self):
        self.rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
        self.rabbitmq_user = os.getenv("RABBITMQ_USER", "Nicole")
        self.rabbitmq_pass = os.getenv("RABBITMQ_PASS", "nicole123")
        self.rabbitmq_queue = os.getenv("RABBITMQ_QUEUE", "r_queue")
        
        self.connection = None
        self.channel = None
        self.connect()

    def connect(self):
        max_retries = 5
        retry_delay = 3
        
        for attempt in range(max_retries):
            try:
                credentials = pika.PlainCredentials(self.rabbitmq_user, self.rabbitmq_pass)
                parameters = pika.ConnectionParameters(
                    host=self.rabbitmq_host,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300,
                    connection_attempts=3,
                    retry_delay=3
                )
                
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                
                # Configura a fila como durável
                self.channel.queue_declare(
                    queue=self.rabbitmq_queue,
                    durable=True,
                    arguments={
                        'x-message-ttl': 60000,
                        'x-dead-letter-exchange': ''
                    }
                )
                
                self.channel.basic_qos(prefetch_count=1)
                return True
                
            except Exception as e:
                print(f"[{self.SYNC_ID}] Tentativa {attempt + 1}/{max_retries} - Erro ao conectar: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        
        raise Exception("Não foi possível conectar ao RabbitMQ após várias tentativas")

    def reconnect(self):
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except:
            pass
            
        print(f"[{self.SYNC_ID}] Tentando reconectar...")
        self.connect()
        self.start_consuming()

    def start_consuming(self):
        try:
            self.channel.basic_consume(
                queue=self.rabbitmq_queue,
                on_message_callback=self.on_message,
                auto_ack=False
            )
            print(f"[{self.SYNC_ID}] Aguardando mensagens...")
            self.channel.start_consuming()
        except pika.exceptions.AMQPError as e:
            print(f"[{self.SYNC_ID}] Erro no consumo: {str(e)}")
            self.reconnect()
        except Exception as e:
            print(f"[{self.SYNC_ID}] Erro inesperado: {str(e)}")
            raise

    def on_message(self, ch, method, properties, body):
        try:
            if not body:
                ch.basic_nack(delivery_tag=method.delivery_tag)
                return

            msg = json.loads(body.decode())
            primitiva = msg.get("primitiva")
            request_id = msg.get("request_id")

            if primitiva == "ACQUIRE":
                self.state['acquire_list'].append(msg)
                msg["reply_to"] = properties.reply_to
                msg["correlation_id"] = properties.correlation_id
                self.state['pending_requests'].append(msg)
                print(f"[{self.SYNC_ID}] ACQUIRE recebido de {msg['client_id']}")
                
                # Processa em thread separada para não bloquear
                Thread(target=self.process_request, args=(msg,)).start()
                
            elif primitiva == "RELEASE":
                self.state['release_list'].append(request_id)
                print(f"[{self.SYNC_ID}] RELEASE registrado de {msg['client_id']}")

            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except json.JSONDecodeError:
            print(f"[{self.SYNC_ID}] Mensagem inválida (não JSON)")
            ch.basic_nack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"[{self.SYNC_ID}] Erro ao processar mensagem: {str(e)}")
            ch.basic_nack(delivery_tag=method.delivery_tag)

    def process_request(self, request):
        try:
            if self.can_enter_critical_section(request):
                self.enter_critical_section(request)
        except Exception as e:
            print(f"[{self.SYNC_ID}] Erro no processamento: {str(e)}")

    def can_enter_critical_section(self, request):
        for acq in self.state['acquire_list']:
            if acq["request_id"] == request["request_id"]:
                return True
            if acq["request_id"] not in self.state['release_list']:
                return False
        return True

    def enter_critical_section(self, request):
        try:
            print(f"[{self.SYNC_ID}] Entrando na seção crítica para {request['client_id']} ({request['request_id']})")
            time.sleep(random.uniform(0.2, 1.0))  # Simula processamento

            release_msg = {
                "client_id": request["client_id"],
                "timestamp": request["timestamp"],
                "request_id": request["request_id"],
                "primitiva": "RELEASE"
            }
            
            self.channel.basic_publish(
                exchange='',
                routing_key=self.rabbitmq_queue,
                body=json.dumps(release_msg).encode(),
                properties=pika.BasicProperties(delivery_mode=2)
            )

            self.state['release_list'].append(request["request_id"])
            print(f"[{self.SYNC_ID}] RELEASE publicado para {request['request_id']}")

            # Envia COMMITTED de volta ao cliente
            if "reply_to" in request and "correlation_id" in request:
                committed = {"status": "COMMITTED", "request_id": request["request_id"]}
                self.channel.basic_publish(
                    exchange='',
                    routing_key=request["reply_to"],
                    properties=pika.BasicProperties(
                        correlation_id=request["correlation_id"],
                        content_type='application/json',
                        delivery_mode=2
                    ),
                    body=json.dumps(committed).encode()
                )
                print(f"[{self.SYNC_ID}] COMMITTED enviado a {request['client_id']}")
                
        except pika.exceptions.AMQPError as e:
            print(f"[{self.SYNC_ID}] Erro ao publicar mensagem: {str(e)}")
            self.reconnect()

if __name__ == "__main__":
    try:
        sync = ClusterSync()
        sync.start_consuming()
    except KeyboardInterrupt:
        print("\nEncerrando processo...")
    except Exception as e:
        print(f"Erro fatal: {str(e)}")