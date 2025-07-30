# Tipos de mensagens possíveis
ACQUIRE = "ACQUIRE"
RELEASE = "RELEASE"
COMMITTED = "COMMITTED"

class Message:
    def __init__(self, msg_type, sender_id, timestamp=None):
        self.type = msg_type  # ACQUIRE, RELEASE ou COMMITTED
        self.sender = sender_id
        self.timestamp = timestamp

    def to_dict(self):
        return {
            "type": self.type,
            "sender": self.sender,
            "timestamp": self.timestamp
        }

    @staticmethod
    def from_dict(data):
        return Message(data['type'], data['sender'], data.get('timestamp'))
