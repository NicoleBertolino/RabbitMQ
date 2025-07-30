import random
import time

def enter_critical_section(sync_id, client_id):
    """
    Simula entrada na seção crítica pelo Cluster Sync.
    """
    duration = round(random.uniform(0.2, 1.0), 2)
    print(f"[{sync_id}] >> Cliente {client_id} está na seção crítica por {duration}s")
    time.sleep(duration)
    print(f"[{sync_id}] << Cliente {client_id} saiu da seção crítica")