import time, os
from contextlib import contextmanager
import sys, os

class Timer:
    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end = time.perf_counter()
        self.elapsed = self.end - self.start

def get_dataset_dir():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    datasets = os.path.join(current_dir, '..', 'datasets')
    return os.path.normpath(datasets)

@contextmanager
def suppress_stdout():
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout

