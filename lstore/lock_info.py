import threading
from collections import defaultdict



class Lock_Manager:

    def __init__(self)->None:
        self.locks:defaultdict[int,RWL] = defaultdict(RWL)

    def acquire_read(self, page_range_index:int, search_key=None)->bool:
        return self.locks[page_range_index].acquire_read()

    def acquire_write(self, page_range_index:int)->bool:
        return self.locks[page_range_index].acquire_write()
    
    def release_read(self, page_range_index:int)->None:
        self.locks[page_range_index].release_read()

    def release_write(self, page_range_index:int)->None:
        self.locks[page_range_index].release_write()


class RWL:

    def __init__(self)->None:
        self.num_readers:int = 0
        self.is_writer:bool  = False
        self.lock            = threading.RLock()

    def acquire_read(self)->bool:
        with self.lock:
            if self.is_writer:
                return False
            self.num_readers += 1
        return True

    def acquire_write(self)->bool:
        with self.lock:
            if self.is_writer or self.num_readers:
                return False
            self.is_writer = True
        return True

    def release_read(self)->None:
        with self.lock:
            self.num_readers -= 1

    def release_write(self)->None:
        with self.lock:
            self.is_writer = False
