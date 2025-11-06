import time
import os

class Timer:
    def __enter__(self):
        self._enter_time = time.time()

    def __exit__(self, *exc_args):
        self._exit_time = time.time()
        print(f"{self._exit_time - self._enter_time:.2f} seconds elapsed")

def get_dataset_dir():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    datasets = os.path.join(current_dir, '..', 'datasets')
    return os.path.normpath(datasets)

