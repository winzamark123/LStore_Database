import lstore.page_range as Page_Range
from lstore.config import *
from lstore.disk import DISK
import lstore.config as Config
from lstore.record import Record
import os 
from lstore.physical_page import Physical_Page
from datetime import datetime 

class Frame:
    def __init__(self, path_to_page: str):
        self.is_dirty = 0 # Boolean to check if the physical_page has been modified
        self.pin_count = 0
        self.is_pin = False
        self.physical_pages:list[Physical_Page] = []
        self.time_in_buffer = datetime.now()
        self.is_tail = False 
        
        self.path_to_page = path_to_page 

    def get_pin_count(self) -> int:
        return self.pin_count
    
    def check_dirty_status(self) -> bool:
        return self.is_dirty
    
    def set_dirty(self):
        self.is_dirty = True
    
    def set_clean(self):
        self.is_dirty = False
    
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
            # self.physical_pages.append(Physical_Page())

            path_to_physical_page = f"{path_to_page}/{i}.bin"

            # Check if the file exists to decide whether to read from it or initialize a new one
            if os.path.exists(path_to_physical_page):
                self.physical_pages.append(DISK.read_physical_page_from_disk(path_to_physical_page))

            else:
                # If the file does not exist, you may need to create and initialize it
                # Example: initializing an empty file
                with open(path_to_physical_page, 'wb') as f:
                    # Initialize the file if needed; for example, writing empty bytes:
                    f.write(b'\x00' * Config.PHYSICAL_PAGE_SIZE)  # Adjust this according to your data structure needs
                self.physical_pages.append(DISK.read_physical_page_from_disk(path_to_physical_page))

    def insert_record(self, key_index:int, record:Record) -> None:
        rid = record.get_rid()

        for i , pp in enumerate(self.physical_pages):
            print("I", i)
            if i == RID_COLUMN:
                pp.edit_byte_array(value=rid, rid=rid)
            elif i == INDIRECTION_COLUMN:
                pp.edit_byte_array(value=rid, rid=rid)
            elif i == BASE_RID_COLUMN:
                pp.edit_byte_array(value=0, rid=rid)
            elif i == SCHEMA_ENCODING_COLUMN:
                pp.edit_byte_array(value=0, rid=rid)
            elif i == key_index:
                pp.edit_byte_array(value=record.key, rid=rid)
            else:
                print("INDEX", i - META_DATA_NUM_COLUMNS)
                print(len(record.columns))
                pp.edit_byte_array(record.columns[i - META_DATA_NUM_COLUMNS], rid)

    def get_record(self, rid:int) -> Record:
        indir = self.physical_page[0].value_exists_at_bytes(rid=rid)
        # temp_rid = self.physical_page[1].value_exists_at_bytes(rid=rid)
        print("Indir", indir)
        # print("temp_rid", temp_rid)

        temp_list = []
        if indir == rid:
            for pp in self.physical_pages[META_DATA_NUM_COLUMNS:]:
                temp_list.append(pp.value_exists_at_bytes(rid=rid))
                #TODO 
            return Record(rid=rid, columns=tuple(temp_list))
                






        




        pass 

    def update_record(self, record:Record):
        pass
