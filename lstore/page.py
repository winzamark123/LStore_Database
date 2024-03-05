import os 
from lstore.disk import DISK
from lstore.bufferpool import BUFFERPOOL
from lstore.config import *
from lstore.record import Record, RID
from lstore.physical_page import Physical_Page

class Page: 
    pass 
class Base_Page:
    def __init__(self, base_page_dir_path:str, base_page_index:int)->None:
        self.base_page_index = base_page_index
        self.path_to_page = base_page_dir_path

        self.meta_data = self.__read_metadata()

        self.num_columns = self.meta_data["num_columns"] + META_DATA_NUM_COLUMNS 
        self.key_index = self.meta_data["key_index"]
        
    def __read_metadata(self)->dict:
        table_path = os.path.dirname(os.path.dirname(self.path_to_page))
        print(table_path)
        return DISK.read_metadata_from_disk(table_path)

    def insert_record(self, record:Record)->None:
        # self.insert_new_record(record)
        record_info = {
            "page_range_num": record.get_page_range_index(),
            "page_type": "base",
            "page_num": record.get_base_page_index()
        } 

        #META = RID, IC, SCHEMA, BASE_RID
        frame_index = BUFFERPOOL.import_frame(path_to_page=self.path_to_page, num_columns=self.num_columns, record_info=record_info)
        BUFFERPOOL.insert_record(key_index=self.key_index, frame_index=frame_index, record=record)

    def get_record(self, rid:RID)->Record:
        if not BUFFERPOOL.is_record_in_buffer(rid=rid, page_type="base", page_num=rid.get_base_page_index()):
            return None
        else:
            frame_index = BUFFERPOOL.get_record_from_buffer(rid, "base", rid.get_base_page_index()) 


    def update_record(self, rid:RID, new_record:Record)->None:
        frame_index = BUFFERPOOL.get_record_from_buffer(rid, "base", rid.get_base_page_index())
        pass
        

    def delete_record(self, rid:RID)->None:
        pass

class Tail_Page:

    def __init__(self, tail_page_dir_path:str, tail_page_index:int)->None:
        self.tail_page_index = tail_page_index
        self.path_to_page = tail_page_dir_path

        self.meta_data = self.__read_metadata()

        self.num_columns = self.meta_data["num_columns"] + META_DATA_NUM_COLUMNS 
        self.key_index = self.meta_data["key_index"]

    def __read_metadata(self)->dict:
        table_path = os.path.dirname(os.path.dirname(self.path_to_page))
        return DISK.read_metadata_from_disk(table_path)

    def insert_record(self, record:Record)->None:
        self.insert_new_record(record)

        record_info = {
            "page_range_num": record.get_page_range_index(),
            "page_type": "tail",
            "page_num": record.get_base_page_index()
        } 

        #META = RID, IC, SCHEMA, BASE_RID

        frame_index = BUFFERPOOL.import_frame(path_to_page=self.path_to_page, num_columns=self.num_columns, record_info=record_info)
        BUFFERPOOL.insert_record(key_index=self.key_index, frame_index=frame_index, record=record)

    def get_record(self, rid:RID)->Record:
        pass

    def update_record(self, rid:RID, new_record:Record)->None:
        pass

    def delete_record(self, rid:RID)->None:
        pass