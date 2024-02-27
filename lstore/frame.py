import lstore.page_range as Page_Range
import lstore.physical_page as Physical_Page
from datetime import datetime 

class Frame:
    def __init__(self, path_to_page: str, table_name: str):
        self.is_dirty = 0 # Boolean to check if the physical_page has been modified
        self.pin_count = 0
        self.is_pin = False
        self.physical_pages = []
        self.table_name = table_name
        self.time_in_buffer = datetime.now()
        
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

    


    

    
