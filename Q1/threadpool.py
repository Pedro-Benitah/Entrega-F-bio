import threading
import queue
import time
import os
import sys
import re
from collections import Counter

def worker_thread(task_queue, output_dir):
    """Função executada por cada thread worker para processar arquivos da fila."""
    while True:
        fname = task_queue.get()
        if fname is None:
            # Sinal de término
            task_queue.task_done()
            break
        # Leitura do arquivo de texto
        try:
            with open(fname, 'r', encoding='utf-8', errors='ignore') as f:
                text = f.read()
        except Exception as e:
            print(f"Erro ao ler arquivo {fname}: {e}")
            text = ""
        # Contagem de ocorrências de cada palavra (ignorando pontuação e maiúsculas/minúsculas)
        clean_text = text.lower()
        clean_text = re.sub(r'[^\w\s]', ' ', clean_text)  # substitui pontuação por espaço
        words = clean_text.split()
        counts = Counter(words)
        # Escreve o resultado em arquivo .out
        rel_name = os.path.basename(fname)
        out_path = os.path.join(output_dir, rel_name + ".out")
        os.makedirs(output_dir, exist_ok=True)
        try:
            with open(out_path, 'w', encoding='utf-8') as out_f:
                for word, count in counts.items():
                    out_f.write(f"{word}: {count}\n")
        except Exception as e:
            print(f"Erro ao escrever arquivo de resultado {out_path}: {e}")
        task_queue.task_done()

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(f"Uso: python {sys.argv[0]} <diretorio_entrada> <diretorio_saida> <num_threads>")
        sys.exit(1)
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    try:
        num_threads = int(sys.argv[3])
    except:
        print("Número de threads inválido.")
        sys.exit(1)
    # Lista todos os arquivos .txt no diretório de entrada
    files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.txt')]
    total_files = len(files)
    if total_files == 0:
        print("Nenhum arquivo .txt encontrado no diretório de entrada.")
        sys.exit(1)
    # Cria a fila de tarefas e inicia os threads workers
    task_queue = queue.Queue()
    workers = []
    for i in range(num_threads):
        th = threading.Thread(target=worker_thread, args=(task_queue, output_dir))
        th.start()
        workers.append(th)
    # Enfileira todas as tarefas (arquivos de texto)
    start_time = time.time()
    for f in files:
        task_queue.put(f)
    # Aguarda processamento de todas as tarefas
    task_queue.join()
    # Envia sinal de término (None) para todos os workers e aguarda encerramento
    for i in range(num_threads):
        task_queue.put(None)
    for th in workers:
        th.join()
    end_time = time.time()
    # Cálculo do tempo total e throughput
    total_time = end_time - start_time
    throughput = total_files / total_time if total_time > 0 else 0.0
    print(f"Tempo total: {total_time:.2f} segundos")
    print(f"Throughput: {throughput:.2f} arquivos/segundo")
