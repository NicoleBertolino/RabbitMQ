import pika
import json
import uuid
import random
import time
import os
from datetime import datetime

class RabbitMQClient:
    def __init__(self):
        self.load_config()
        self.setup_connection()
        self.setup_queues()
        
    def load_config(self):
        CONFIG_FILE = os.getenv("CONFIG_FILE", "../config_client_3/client_config_3.json")
        with open(CONFIG_FILE, "r") as f:
            config = json.load(f)
        
        self.CLIENT_ID = config["client_id"]
        self.ACCESS_COUNT = config["access_count"]
        self.SLEEP_MIN = config["sleep_min"]
        self.SLEEP_MAX = config["sleep_max"]
        self.RABBITMQ_CONF = config["rabbitmq"]
        self.RESPONSE_TIMEOUT = 30  # segundos

    def setup_connection(self):
        credentials = pika.PlainCredentials(
            self.RABBITMQ_CONF["username"], 
            self.RABBITMQ_CONF["password"]
        )
        
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.RABBITMQ_CONF["host"],
                port=self.RABBITMQ_CONF["port"],
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300,
                connection_attempts=5,
                retry_delay=3
            )
        )
        self.channel = self.connection.channel()

    def setup_queues(self):
        """Configura as filas de forma segura, sem modificar existentes"""
        try:
            # Verifica se a fila principal existe sem modificar
            self.channel.queue_declare(
                queue=self.RABBITMQ_CONF["queue"],
                durable=True,
                passive=True
            )
        except pika.exceptions.ChannelClosedByBroker as e:
            if e.reply_code == 404:  # Se a fila não existir
                self.channel.queue_declare(
                    queue=self.RABBITMQ_CONF["queue"],
                    durable=True,
                    arguments={
                        'x-message-ttl': 60000,
                        'x-dead-letter-exchange': ''
                    }
                )
            else:
                raise
        
        # Fila de respostas (exclusiva para este cliente)
        self.reply_queue = self.channel.queue_declare(
            queue='',
            exclusive=True,
            auto_delete=True
        )
        self.reply_queue_name = self.reply_queue.method.queue
        
        self.channel.basic_consume(
            queue=self.reply_queue_name,
            on_message_callback=self.on_response,
            auto_ack=True
        )

    def on_response(self, ch, method, props, body):
        """Callback para processar respostas COMMITTED"""
        if hasattr(self, 'current_correlation_id') and props.correlation_id == self.current_correlation_id:
            try:
                response = json.loads(body)
                if response.get("status") == "COMMITTED":
                    print(f"[{self.CLIENT_ID}] COMMITTED recebido para {props.correlation_id}")
                    self.response_received = True
            except json.JSONDecodeError:
                print(f"[{self.CLIENT_ID}] Resposta inválida")

    def send_request(self, attempt):
        """Envia um pedido ACQUIRE para a fila"""
        request_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat()
        message = {
            "client_id": self.CLIENT_ID,
            "timestamp": timestamp,
            "request_id": request_id,
            "primitiva": "ACQUIRE"
        }

        self.current_correlation_id = str(uuid.uuid4())
        self.response_received = False

        try:
            self.channel.basic_publish(
                exchange=self.RABBITMQ_CONF.get("exchange", ""),
                routing_key=self.RABBITMQ_CONF["queue"],
                properties=pika.BasicProperties(
                    reply_to=self.reply_queue_name,
                    correlation_id=self.current_correlation_id,
                    content_type='application/json',
                    delivery_mode=2,  # Persistente
                ),
                body=json.dumps(message)
            )
            print(f"[{self.CLIENT_ID}] Pedido {attempt} enviado: {request_id}")
            return True
        except pika.exceptions.AMQPError as e:
            print(f"[{self.CLIENT_ID}] Falha ao enviar pedido {attempt}: {str(e)}")
            return False

    def wait_for_response(self):
        """Aguarda a resposta COMMITTED com timeout"""
        start_time = time.time()
        while not self.response_received:
            if time.time() - start_time > self.RESPONSE_TIMEOUT:
                print(f"[{self.CLIENT_ID}] Timeout esperando resposta")
                return False
            self.connection.process_data_events()
            time.sleep(0.1)
        return True

    def run(self):
        """Executa o loop principal do cliente"""
        try:
            for i in range(1, self.ACCESS_COUNT + 1):
                if not self.send_request(i):
                    time.sleep(3)  # Espera antes de tentar novamente
                    continue
                    
                if not self.wait_for_response():
                    continue
                    
                wait_time = random.randint(self.SLEEP_MIN, self.SLEEP_MAX)
                print(f"[{self.CLIENT_ID}] Dormindo por {wait_time}s\n")
                time.sleep(wait_time)

            print(f"[{self.CLIENT_ID}] Finalizou os {self.ACCESS_COUNT} pedidos.")
        finally:
            self.connection.close()

if __name__ == "__main__":
    try:
        client = RabbitMQClient()
        client.run()
    except KeyboardInterrupt:
        print("\nClient encerrado pelo usuário")
    except Exception as e:
        print(f"Erro inesperado: {str(e)}")