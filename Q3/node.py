import socket
import threading
import time
import sys

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"Uso: python {sys.argv[0]} <host_servidor> <porta_servidor>")
        sys.exit(1)
    server_host = sys.argv[1]
    server_port = int(sys.argv[2])
    # Conecta ao scheduler
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((server_host, server_port))
    send_lock = threading.Lock()
    running = True
    node_status = "idle"
    # Thread de heartbeat: envia status periodicamente
    def heartbeat():
        while running:
            msg = f"HEARTBEAT {node_status}\n".encode()
            send_lock.acquire()
            try:
                sock.sendall(msg)
            finally:
                send_lock.release()
            time.sleep(5)  # envia a cada 5 segundos

    hb_thread = threading.Thread(target=heartbeat, daemon=True)
    hb_thread.start()
    # Informa prontidão ao scheduler
    send_lock.acquire()
    try:
        sock.sendall(b"READY\n")
    finally:
        send_lock.release()
    # Loop principal para receber comandos do scheduler
    conn_file = sock.makefile('rb')
    try:
        while True:
            line = conn_file.readline()
            if not line:
                # Conexão fechada pelo scheduler
                break
            line = line.decode().strip()
            if line.startswith("TASK"):
                # Recebeu uma tarefa para processar
                _, filename = line.split(" ", 1)
                node_status = "busy"
                # Processa o arquivo (ler e normalizar conteúdo para minúsculas)
                result_data = b""
                try:
                    with open(filename, 'rb') as f:
                        content = f.read()
                        result_data = content.lower()
                except Exception as e:
                    print(f"Nó: Erro ao ler {filename}: {e}")
                # Envia o resultado de volta para o scheduler
                header = f"RESULT {filename} {len(result_data)}\n".encode()
                send_lock.acquire()
                try:
                    sock.sendall(header)
                    if result_data:
                        sock.sendall(result_data)
                finally:
                    send_lock.release()
                node_status = "idle"
                # loop continua esperando próximo comando
                continue
            elif line == "SHUTDOWN":
                # Pedido para encerrar
                print("Nó: Recebido comando de desligamento do servidor.")
                running = False
                break
            elif line == "READY":
                # O scheduler não deveria mandar READY, então apenas ignoramos
                continue
            else:
                # Mensagem desconhecida, ignora
                continue
    finally:
        sock.close()
        print("Nó: Desconectado do servidor.")
