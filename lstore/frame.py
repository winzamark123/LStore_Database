from lstore.disk import DISK
import lstore.config as Config
from lstore.record import Record, RID
import os
from lstore.physical_page import Physical_Page
from datetime import datetime

class Frame:
    def __init__(self, path_to_page: str):
        self.is_dirty = 0 # Boolean to check if the physical_page has been modified
        self.pin_count = 0
        self.is_pin = False
        self.physical_pages:list[Physical_Page] = []
        # self.path_to_page = path_to_page

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
        self.last_time_used = datetime.now()

    def unpin_frame(self):
        self.is_pin = False
        self.pin_count -= 1

    def has_capacity(self)->bool:
        return self.physical_pages[0].has_capacity()

    def load_data(self, num_columns:int, path_to_page:str)->None:
        self.pin_frame()

        for i in range(num_columns):
            # self.physical_pages.append(Physical_Page()) path_to_physical_page = f"{path_to_page}/{i}.bin"

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
                
        self.unpin_frame()

    def insert_record(self, record:Record) -> None:
        self.pin_frame()
        rid = record.get_rid()        
        for i , pp in enumerate(self.physical_pages):
            # print("I", i)
            if i == Config.RID_COLUMN:
                pp.edit_byte_array(value=rid, rid=rid)
            elif i == Config.INDIRECTION_COLUMN:
                pp.edit_byte_array(value=rid, rid=rid)
            elif i == Config.BASE_RID_COLUMN:
                pp.edit_byte_array(value=0, rid=rid)
            elif i == Config.SCHEMA_ENCODING_COLUMN:
                pp.edit_byte_array(value=0, rid=rid)
            else:
                pp.edit_byte_array(record.columns[i - Config.META_DATA_NUM_COLUMNS], rid)

        if not self.check_dirty_status():
            print("frame set to dirty")
            # sets frame to be dirty
            self.set_dirty()

        # print("Record inserted into frame")
        # print(rid)
        self.unpin_frame()
    
    def delete_record(self, rid:RID):
        self.pin_frame()

        for i, physical_page in enumerate(self.physical_pages):
            if i == Config.RID_COLUMN:
                physical_page.edit_byte_array(value=0, rid=rid)
        
        if not self.check_dirty_status():
            self.set_dirty()

        self.unpin_frame()


    def update_record(self, rid:RID, new_record:Record):
        self.pin_frame()
        old_record_columns = list()
        for i, physical_page in enumerate(self.physical_pages):
            if i == Config.RID_COLUMN: continue
            elif i == Config.INDIRECTION_COLUMN:
                record_tail_page_path = physical_page.get_byte_array()

        #   old_record_columns.append(physical_page.get_byte_array())
        # old_record_columns = tuple(old_record_columns)
        self.unpin_frame()

    def get_data(self, rid:RID) -> tuple:
        self.pin_frame()
        data_columns = list()
        for i, physical_page in enumerate(self.physical_pages):
            if i == Config.RID_COLUMN:
                continue
            elif i == Config.INDIRECTION_COLUMN:
                # record_tail_page_path = physical_page.get_data(rid)
                continue
            elif i == Config.BASE_RID_COLUMN:
                continue
            elif i == Config.SCHEMA_ENCODING_COLUMN:
                continue
            else:
                # data_columns.append(physical_page.get_byte_array(rid))
                data_columns.append(physical_page.get_data(rid))
        data_columns = tuple(data_columns)
        self.unpin_frame()
        return data_columns

    def update_record(self, record:Record):
        pass
