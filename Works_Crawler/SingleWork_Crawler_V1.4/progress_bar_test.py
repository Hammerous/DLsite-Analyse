import time
import multiprocessing
from tqdm import tqdm

def worker(num):
    """模拟耗时任务"""
    for _ in tqdm(range(100), desc=f'Worker {num}', position=num, leave=True, delay=1):
        time.sleep(0.5)

if __name__ == '__main__':
    # 创建3个进程
    processes = []
    for i in range(3):
        p = multiprocessing.Process(target=worker, args=(i,))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()
