# Tipos de mensagens possíveis (valores fixos para padronizar a comunicação)
ACQUIRE = "ACQUIRE"    # Pedido para entrar na seção crítica
RELEASE = "RELEASE"    # Liberação da seção crítica
COMMITTED = "COMMITTED"  # Confirmação de que a operação foi concluída com sucesso

# Classe que representa uma mensagem trocada entre processos
class Message:
    def __init__(self, msg_type, sender_id, timestamp=None):
        # Inicializa uma nova mensagem com tipo, identificador do remetente e um timestamp opcional
        self.type = msg_type      # Tipo da mensagem: ACQUIRE, RELEASE ou COMMITTED
        self.sender = sender_id   # ID de quem enviou a mensagem
        self.timestamp = timestamp  # Marca de tempo associada à mensagem (pode ser usada para ordenação)

    # Converte o objeto Message em um dicionário Python para envio como JSON
    def to_dict(self):
        return {
            "type": self.type,
            "sender": self.sender,
            "timestamp": self.timestamp
        }

    # Método estático para criar um objeto Message a partir de um dicionário
    @staticmethod
    def from_dict(data):
        # Pega os campos do dicionário recebido e cria uma nova instância de Message
        return Message(data['type'], data['sender'], data.get('timestamp'))
