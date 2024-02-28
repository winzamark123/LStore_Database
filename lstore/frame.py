import lstore.page_range as Page_Range
from lstore.disk import DISK
import lstore.config as Config
import os 
from lstore.physical_page import Physical_Page
from datetime import datetime 

class Frame:
    def __init__(self, path_to_page: str):
        self.is_dirty = 0 # Boolean to check if the physical_page has been modified
        self.pin_count = 0
        self.is_pin = False
        self.physical_pages = []
        self.time_in_buffer = datetime.now()
        self.is_tail = False 
        
        self.path_to_page = path_to_page 

    def get_pin_count(self) -> int:
        return self.pin_count
    
    def is_dirty(self) -> bool:
        return self.is_dirty
    
    def set_dirty(self):
        self.dirty = True
    
    def set_clean(self):
        self.dirty = False
    
    def pin_frame(self):
        self.pin_count += 1
        self.is_pin = True
        self.time_in_buffer = datetime.now()
    
    def unpin_frame(self):
        self.is_pin = False

    def has_capacity(self) -> bool:
        return self.physical_pages[0].has_capacity()
    
    def load_data(self, num_columns: int, path_to_page: str):
        self.pin_frame() #pin itself
        self.pin_count += 1 

        for i in range(num_columns):
            self.physical_pages.append(Physical_Page(i))

            path_to_physical_page = f"{path_to_page}{i}.bin"
            os.makedirs(path_to_physical_page, exist_ok=True)

            # Check if the file exists to decide whether to read from it or initialize a new one
            if os.path.exists(path_to_physical_page):
                DISK.read_physical_page_from_disk(path_to_physical_page)

            else:
                # If the file does not exist, you may need to create and initialize it
                # Example: initializing an empty file
                with open(path_to_physical_page, 'wb') as f:
                    # Initialize the file if needed; for example, writing empty bytes:
                    f.write(b'\x00' * Config.DATA_ENTRY_SIZE)  # Adjust this according to your data structure needs

