import os
from lstore.disk import DISK
from lstore.bufferpool import BUFFERPOOL
from lstore.record import Record, RID
from lstore.physical_page import Physical_Page

class Base_Page:
    def __init__(self, base_page_dir_path:str, base_page_index:int)->None:
        self.base_page_index = base_page_index
        self.base_page_path = base_page_dir_path
        # nums of columns plus the META_DATA columns
        self.num_columns = self.__get_num_columns()

    def __get_num_columns(self)->int:
        table_path = os.path.dirname(os.path.dirname(self.base_page_path))
        return DISK.read_metadata_from_disk(table_path)["num_columns"]

    def insert_record(self, record:Record)->None:


        # for j in range(self.num_columns):
        #     print(f'Base page {self.base_page_index} physical page ({j})')
        #     for i in range(0, 4096, 8):
        #         print(int.from_bytes(BUFFERPOOL.frames[frame_index].physical_pages[j].data[i:i+8], byteorder='big')

        BUFFERPOOL.insert_record(page_path=self.base_page_path, record=record, num_columns=self.num_columns)

    #get data from bufferpool
    def get_data(self, rid:RID)->tuple:
        return BUFFERPOOL.get_data_from_buffer(rid=rid, page_path=self.base_page_path, num_columns=self.num_columns)

    def update_record(self, rid:RID, updated_columns:tuple)->None:
        pass


    def delete_record(self, rid:RID)->None:
        pass

class Tail_Page:

    def __init__(self, tail_page_dir_path:str, tail_page_index:int)->None:
        self.tail_page_index = tail_page_index
        self.tail_page_path = tail_page_dir_path
        self.num_columns = self.__get_num_columns()

    def __get_num_columns(self)->int:
        table_path = os.path.dirname(os.path.dirname(self.base_page_path))
        return DISK.read_metadata_from_disk(table_path)["num_columns"]

    def insert_record(self, record:Record)->None:
        self.insert_new_record(record)

        record_info = {
            "page_range_index": record.get_page_range_index(),
            "page_index": record.get_base_page_index()
        }

        #META = RID, IC, SCHEMA, BASE_RID

        frame_index = BUFFERPOOL.import_frame(path_to_page=self.tail_page_path, num_columns=self.num_columns, record_info=record_info)
        BUFFERPOOL.insert_record(key_index=self.key_index, frame_index=frame_index, record=record)

    def get_record(self, rid:RID)->Record:
        pass

    def update_record(self, rid:RID, new_record:Record)->None:
        pass

    def delete_record(self, rid:RID)->None:
        pass