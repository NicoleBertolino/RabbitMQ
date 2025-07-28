import pika

def callback(ch, method, properties, body):
    print(f"[x] Fila: {method.routing_key} | Mensagem: {body.decode()}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def consumir_varias_filas(filas):
    credenciais = pika.PlainCredentials('Nicole', 'nicole123')
    parametros = pika.ConnectionParameters('localhost', credentials=credenciais)
    conexao = pika.BlockingConnection(parametros)
    canal = conexao.channel()

    for fila in filas:
        canal.queue_declare(queue=fila)
        canal.basic_consume(queue=fila, on_message_callback=callback, auto_ack=False)
        print(f"[*] Escutando fila: {fila}")

    print(" [*] Aguardando mensagens. Para sair, pressione CTRL+C")

    try:
        canal.start_consuming()
    except KeyboardInterrupt:
        print(" [!] Interrompido pelo usuário.")
        canal.stop_consuming()
        conexao.close()

if __name__ == "__main__":
    filas = input("Digite os nomes das filas separadas por vírgula: ").split(",")
    filas = [f.strip() for f in filas if f.strip()]
    consumir_varias_filas(filas)
