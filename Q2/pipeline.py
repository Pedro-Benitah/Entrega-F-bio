import threading
import queue
import time
import os
import sys

# Funções de worker para cada estágio do pipeline:
def stage1_worker(q1, q2):
    """Lê arquivo da fila q1 e envia conteúdo bruto para q2."""
    while True:
        fname = q1.get()
        if fname is None:
            q1.task_done()
            break
        start_time = time.time()
        try:
            with open(fname, 'rb') as f:
                content = f.read()
        except Exception:
            content = b""
        # Coloca conteúdo lido (bytes) junto com timestamp de início na fila do estágio 2
        q2.put((fname, content, start_time))
        q1.task_done()

def stage2_worker(q2, q3):
    """Normaliza o texto (para minúsculas) e envia para q3."""
    while True:
        item = q2.get()
        if item is None:
            q2.task_done()
            break
        fname, content, start_time = item
        # Normalização: converte todo o conteúdo para minúsculas
        norm_content = content.lower()
        q3.put((fname, norm_content, start_time))
        q2.task_done()

def stage3_worker(q3, output_dir, latencies, lat_lock):
    """Escreve o conteúdo normalizado em arquivo de saída e mede latência."""
    while True:
        item = q3.get()
        if item is None:
            q3.task_done()
            break
        fname, norm_content, start_time = item
        # Escreve o conteúdo normalizado no arquivo de saída correspondente
        rel_name = os.path.basename(fname)
        out_path = os.path.join(output_dir, rel_name + ".out")
        os.makedirs(output_dir, exist_ok=True)
        try:
            with open(out_path, 'wb') as out_f:
                out_f.write(norm_content)
        except Exception:
            pass
        # Calcula latência (tempo desde início da leitura até escrita)
        end_time = time.time()
        latency = end_time - start_time
        # Armazena latência em uma lista (com lock para evitar condições de corrida)
        lat_lock.acquire()
        latencies.append(latency)
        lat_lock.release()
        q3.task_done()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"Uso: python {sys.argv[0]} <diretorio_entrada> <diretorio_saida> "
              "[<threads_por_estagio> ou <t1> <t2> <t3>]")
        sys.exit(1)
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    # Define quantidade de threads por estágio conforme argumentos
    t1 = t2 = t3 = 1
    if len(sys.argv) == 4:
        # Um valor para todos estágios
        t1 = t2 = t3 = int(sys.argv[3])
    elif len(sys.argv) >= 6:
        # Três valores específicos para cada estágio
        t1 = int(sys.argv[3]); t2 = int(sys.argv[4]); t3 = int(sys.argv[5])
    # Prepara lista de arquivos de entrada
    file_list = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith(".txt")]
    total_files = len(file_list)
    if total_files == 0:
        print("Nenhum arquivo .txt encontrado no diretório de entrada.")
        sys.exit(1)
    # Cria filas intermediárias
    q1 = queue.Queue()
    q2 = queue.Queue()
    q3 = queue.Queue()
    # Lista e lock para armazenar latências medidas
    latencies = []
    lat_lock = threading.Lock()
    # Cria e inicia threads de cada estágio
    stage1_threads = [threading.Thread(target=stage1_worker, args=(q1, q2)) for _ in range(t1)]
    stage2_threads = [threading.Thread(target=stage2_worker, args=(q2, q3)) for _ in range(t2)]
    stage3_threads = [threading.Thread(target=stage3_worker, args=(q3, output_dir, latencies, lat_lock)) for _ in range(t3)]
    for th in stage1_threads + stage2_threads + stage3_threads:
        th.start()
    # Enfileira todos os arquivos de entrada na fila do estágio 1
    start_time = time.time()
    for f in file_list:
        q1.put(f)
    # Insere sentinelas para indicar fim em cada fila, de acordo com número de threads do próximo estágio
    for _ in stage1_threads:
        q1.put(None)
    # Espera estágio 1 esvaziar sua fila
    q1.join()
    for _ in stage2_threads:
        q2.put(None)
    q2.join()
    for _ in stage3_threads:
        q3.put(None)
    q3.join()
    end_time = time.time()
    # Calcula métricas de desempenho
    total_time = end_time - start_time
    throughput = total_files / total_time if total_time > 0 else 0.0
    avg_latency = sum(latencies)/len(latencies) if latencies else 0.0
    print(f"Tempo total de processamento: {total_time:.2f} segundos")
    print(f"Throughput: {throughput:.2f} arquivos/segundo")
    print(f"Latência média por arquivo: {avg_latency:.4f} segundos")
    # Aguarda todos os threads terminarem (após os sentinelas, todos deverão encerrar)
    for th in stage1_threads + stage2_threads + stage3_threads:
        th.join()
