import pika

def publicar_mensagem(mensagem, canal, fila='minha_fila_B'):
    canal.basic_publish(exchange='',
                        routing_key=fila,
                        body=mensagem)
    print(f"Mensagem enviada para a fila '{fila}': {mensagem}")

def main():
    credenciais = pika.PlainCredentials('Nicole', 'nicole123')
    parametros = pika.ConnectionParameters('localhost', credentials=credenciais)
    conexao = pika.BlockingConnection(parametros)
    canal = conexao.channel()
    canal.queue_declare(queue='minha_fila')

    print("Digite as mensagens para enviar à fila RabbitMQ.")
    print("Digite 'sair' para encerrar.")

    while True:
        mensagem = input("Mensagem: ")
        if mensagem.lower() == 'sair':
            break
        publicar_mensagem(mensagem, canal)

    conexao.close()
    print("Conexão encerrada.")

if __name__ == "__main__":
    main()
