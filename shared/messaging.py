import pika  # Biblioteca para interação com o RabbitMQ
import json  # Para serializar e desserializar mensagens no formato JSON

# Classe que encapsula a comunicação com o RabbitMQ
class MessageBroker:
    def __init__(self, host='rabbitmq', username='Nicole', password='nicole123'):
        credentials = pika.PlainCredentials(username, password)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, credentials=credentials)
        )
        self.channel = self.connection.channel()

    # Declara uma fila no servidor RabbitMQ (se já existir, não tem problema)
    def declare_queue(self, queue_name):
        self.channel.queue_declare(queue=queue_name, durable=True)  # durable=True garante que a fila persista mesmo após reinicialização do RabbitMQ

    # Publica uma mensagem na fila indicada
    def publish(self, queue_name, message):
        self.channel.basic_publish(
            exchange='',                 # Exchange padrão (direta, fila com o mesmo nome do routing_key)
            routing_key=queue_name,     # Nome da fila de destino
            body=json.dumps(message),   # Converte o dicionário Python para uma string JSON
            properties=pika.BasicProperties(delivery_mode=2)  # delivery_mode=2 torna a mensagem persistente (não é perdida em caso de crash do servidor)
        )

    # Inicia o consumo de mensagens da fila com o callback fornecido
    def consume(self, queue_name, callback):
        # Função interna para adaptar o callback do pika (padrão) ao formato do usuário
        def wrapped_callback(ch, method, properties, body):
            message = json.loads(body.decode())  # Decodifica JSON recebido
            callback(message)                    # Executa o callback definido pelo usuário com a mensagem
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Confirma recebimento da mensagem (evita redelivery)

        # Configura para o consumidor receber apenas uma mensagem por vez (boa prática)
        self.channel.basic_qos(prefetch_count=1)

        # Inscreve o consumidor com o callback tratado
        self.channel.basic_consume(queue=queue_name, on_message_callback=wrapped_callback)

        # Inicia o consumo de mensagens (bloqueia o processo até KeyboardInterrupt)
        self.channel.start_consuming()
        
    def close(self):
        self.connection.close()
