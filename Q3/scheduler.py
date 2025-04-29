import socket
import threading
import os
import sys

HOST = '0.0.0.0'  # Escuta em todas as interfaces (adequado para servidor)
PORT = 5000       # Porta padrão (pode ser sobreposta via argumento)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Uso: python {sys.argv[0]} <diretorio_entrada> [<diretorio_saida>] [<porta>]")
        sys.exit(1)
    input_dir = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "output"
    if len(sys.argv) > 3:
        PORT = int(sys.argv[3])
    # Lista de tarefas (arquivos texto a processar)
    tasks = []
    for fname in os.listdir(input_dir):
        if fname.endswith(".txt"):
            tasks.append(os.path.join(input_dir, fname))
    total_tasks = len(tasks)
    if total_tasks == 0:
        print("Nenhum arquivo .txt encontrado no diretório de entrada.")
        sys.exit(1)
    print(f"Scheduler: {total_tasks} arquivos a processar.")
    # Variáveis de estado
    tasks_lock = threading.Lock()
    tasks_index = 0             # índice da próxima tarefa a distribuir
    tasks_completed = 0         # contador de tarefas já concluídas
    node_conns = []             # lista de conexões de nós ativos
    node_conns_lock = threading.Lock()

    def handle_node(conn, addr):
        """Thread que lida com a comunicação com um nó específico."""
        nonlocal tasks_index, tasks_completed
        print(f"Scheduler: Conectado a nó {addr}")
        conn_file = conn.makefile('rb')  # arquivo bufferizado para leitura de linhas
        try:
            while True:
                # Envia tarefa ao nó se houver disponível
                task_to_send = None
                tasks_lock.acquire()
                if tasks_index < total_tasks:
                    task_to_send = tasks[tasks_index]
                    tasks_index += 1
                tasks_lock.release()
                if task_to_send:
                    try:
                        msg = f"TASK {task_to_send}\n".encode()
                        conn.sendall(msg)
                        print(f"Scheduler: Tarefa '{task_to_send}' enviada para {addr}")
                    except Exception as e:
                        print(f"Scheduler: Erro ao enviar tarefa para {addr}: {e}")
                        break
                # Aguarda mensagem do nó
                line = conn_file.readline()
                if not line:
                    print(f"Scheduler: Conexão fechada por {addr}")
                    break
                line = line.decode().strip()
                if line.startswith("HEARTBEAT"):
                    # Mensagem de heartbeat do nó (podemos atualizar status se necessário)
                    continue
                elif line.startswith("RESULT"):
                    # Nó concluiu uma tarefa e está enviando resultado
                    parts = line.split(" ")
                    if len(parts) < 3:
                        print(f"Scheduler: Resultado malformatado de {addr}")
                        continue
                    filename = parts[1]
                    length = int(parts[2])
                    # Lê exatamente <length> bytes do conteúdo do resultado
                    content = conn_file.read(length)
                    if content is None:
                        print(f"Scheduler: Conexão encerrada durante recebimento de resultado de {addr}")
                        break
                    # Salva o conteúdo recebido em um arquivo de saída
                    base_name = os.path.basename(filename)
                    out_path = os.path.join(output_dir, base_name + ".out")
                    os.makedirs(output_dir, exist_ok=True)
                    try:
                        with open(out_path, 'wb') as f_out:
                            f_out.write(content)
                        print(f"Scheduler: Resultado de '{filename}' salvo em '{out_path}'")
                    except Exception as e:
                        print(f"Scheduler: Erro ao salvar resultado de {filename}: {e}")
                    # Atualiza contador de tarefas concluídas
                    tasks_lock.acquire()
                    tasks_completed += 1
                    all_done = (tasks_completed >= total_tasks)
                    tasks_lock.release()
                    if all_done:
                        print("Scheduler: Todas as tarefas concluídas. Enviando sinal de encerramento.")
                        # Envia comando de desligamento para todos os nós conectados
                        node_conns_lock.acquire()
                        for c in list(node_conns):
                            try:
                                c.sendall(b"SHUTDOWN\n")
                            except:
                                pass
                        node_conns_lock.release()
                        break  # Sai do loop de comunicação com este nó
                    else:
                        # Caso ainda haja tarefas pendentes ou em andamento, continua loop 
                        # (na próxima iteração, tentará enviar outra tarefa se disponível)
                        continue
                elif line == "READY":
                    # Nó indicou que está pronto (já consideramos isso implicitamente ao enviar tarefa no connect)
                    continue
                elif line == "SHUTDOWN":
                    # Nó confirmando desligamento (não obrigatório no protocolo, mas tratado)
                    print(f"Scheduler: Nó {addr} desligado.")
                    break
                else:
                    # Mensagem desconhecida
                    print(f"Scheduler: Mensagem desconhecida de {addr}: {line}")
                    continue
        finally:
            # Limpeza ao sair (fechamento da conexão)
            node_conns_lock.acquire()
            if conn in node_conns:
                node_conns.remove(conn)
            node_conns_lock.release()
            conn.close()
            print(f"Scheduler: Conexão com {addr} encerrada.")

    # Inicia o socket servidor para aceitar conexões dos nós
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind((HOST, PORT))
    server_sock.listen(5)
    print(f"Scheduler: Aguardando conexões em {HOST}:{PORT}...")
    try:
        while True:
            conn, addr = server_sock.accept()
            # Adiciona a nova conexão de nó na lista e inicia thread de atendimento
            node_conns_lock.acquire()
            node_conns.append(conn)
            node_conns_lock.release()
            th = threading.Thread(target=handle_node, args=(conn, addr), daemon=True)
            th.start()
            # Opcional: poderíamos parar de aceitar novas conexões se todas as tarefas já foram distribuídas
            # if tasks_index >= total_tasks: ...
    except KeyboardInterrupt:
        print("Scheduler: Interrompido manualmente (Ctrl+C).")
    finally:
        server_sock.close()
        print("Scheduler: Encerrando servidor.")
