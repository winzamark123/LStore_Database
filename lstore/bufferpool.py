import io
import os
from datetime import datetime
from bitarray import bitarray
from threading import RLock

import lstore.config as Config
from lstore.disk import Disk
from lstore.record_info import Record, RID, TID

INITIAL_SCHEMA_ENCODING = 0
INITIAL_INDIRECTION_VALUE = -1

class Bufferpool:

    def __init__(self)->None:
        self.frames:dict[str,Frame] = dict()
        self.latch:RLock             = RLock()

    def __del__(self)->None:
        del self.frames
        self.frames = None

    def __has_capacity(self)->bool:
        return len(self.frames) < Config.NUM_FRAMES_IN_BUFFERPOOL

    def __is_page_in_buffer(self, page_path:str)->bool:
        return page_path in self.frames

    def __evict_frame(self)->None:
        earliest_time = datetime.now()
        earliest_frame_path = None
        for frame_path, frame in self.frames.items():
            if earliest_time > frame.get_last_time_used():
                earliest_frame_path = frame_path
        del self.frames[earliest_frame_path]

    def __import_frame(self, page_path:str)->None:
        if not self.__has_capacity():
            self.__evict_frame()
        self.frames[page_path] = Frame(page_path)

    def __access_frame(self, page_path:str)->None:
        with self.latch:
            if not self.__is_page_in_buffer(page_path):
                self.__import_frame(page_path)

    def insert_record(self, record:Record, base_page_path:str)->None:
        self.__access_frame(base_page_path)
        self.frames[base_page_path].insert_record(record)

    def get_record_entry(self, id:RID, page_path:str, column_index:int)->int:
        self.__access_frame(page_path)
        return self.frames[page_path].get_record_entry(id, column_index)

    def get_schema_encoding(self, rid:RID, base_page_path:str)->bitarray:
        self.__access_frame(base_page_path)
        return self.frames[base_page_path].get_schema_encoding(rid)

    def set_schema_encoding(self, rid:RID, schema_encoding:bitarray, base_page_path:str)->None:
        self.__access_frame(base_page_path)
        self.frames[base_page_path].set_schema_encoding(rid, schema_encoding)

    def get_indirection_tid(self, id:RID, page_path:str)->TID:
        self.__access_frame(page_path)
        return self.frames[page_path].get_indirection_tid(id)

    def set_indirection_tid(self, id:RID, tid:TID, page_path:str)->None:
        self.__access_frame(page_path)
        self.frames[page_path].set_indirection_tid(id, tid)

    def delete_record(self, rid:RID, base_page_path:str)->None:
        self.__access_frame(base_page_path)
        self.frames[base_page_path].delete_record(rid)

    def commit_writes_to_disk(self)->None:
        with self.latch:
            for frame in self.frames.values():
                frame.write_frame_to_disk()

    def abort_writes_to_disk(self)->None:
        with self.latch:
            for frame in self.frames.values():
                frame.keep_original_frame_to_disk()


class Frame:

    def __init__(self, page_path:str)->None:
        self.page_path:str                      = page_path
        self.num_pins:int                       = 0
        self.is_dirty:bool                      = False
        self.physical_pages:list[Physical_Page] = list()
        self.last_time_used:datetime            = datetime.now()

        self.latch:RLock                         = RLock()

        # find number of columns from table metadata (in grandparent dir from base/tail page)
        self.num_columns:int                        = \
            Disk.read_from_path_metadata(os.path.dirname(os.path.dirname(page_path)))["num_columns"]

        # get physical pages
        self.__load_physical_pages()
        self.__assert_num_physical_pages()

    def __del__(self)->None:
        self.write_frame_to_disk()

    def __assert_num_physical_pages(self)->None:
        assert len(self.physical_pages) == Config.NUM_METADATA_COLUMNS + self.num_columns

    def __create_physical_pages(self)->None:
        for physical_page_index in range(self.num_columns + Config.NUM_METADATA_COLUMNS):
            physical_page_path = os.path.join(self.page_path, f"{physical_page_index}.bin")
            with open(physical_page_path, 'wb') as f:
                f.write(bytearray(Config.PHYSICAL_PAGE_SIZE))

    def __load_physical_pages(self)->None:
        """
        Reads data from disk and loads it into the physical pages
        """
        if not len(Disk.list_directories_in_path(self.page_path)):
            self.__create_physical_pages()
        physical_page_paths:list[str] = Disk.list_directories_in_path(self.page_path)
        physical_page_paths.sort(key=lambda x: int(x.split('/')[-1].split('.')[0]))
        for physical_page_path in physical_page_paths:
            if not os.path.isfile(physical_page_path): raise ValueError
            with open(physical_page_path, 'rb') as f:
                data = bytearray(f.read())
            self.physical_pages.append(Physical_Page(physical_page_path, data))

    def __pin_frame_decorator(func):
        def wrapper(self, *args, **kwargs):
            with self.latch:
                self.num_pins += 1
            result = func(self, *args, **kwargs)
            with self.latch:
                self.num_pins -= 1
            return result
        return wrapper

    def __set_dirty_bit(self)->None:
        self.is_dirty = True

    def get_last_time_used(self)->datetime:
        return self.last_time_used

    def write_frame_to_disk(self)->None:
        if self.is_dirty:
            for physical_page in self.physical_pages:
                physical_page.write_data_to_disk()

    def keep_original_frame_to_disk(self)->None:
        if self.is_dirty:
            for physical_page in self.physical_pages:
                physical_page.keep_original_data_to_disk()

    @__pin_frame_decorator
    def insert_record(self, record:Record)->None:
        rid = int(record.get_rid())
        # create metadata for data
        self.physical_pages[Config.INDIRECTION_COLUMN].write_record_info_to_data(INITIAL_INDIRECTION_VALUE, rid)
        self.physical_pages[Config.RID_COLUMN].write_record_info_to_data(rid, rid)
        self.physical_pages[Config.TIMESTAMP_COLUMN].write_record_info_to_data(datetime.now().timestamp(), rid)
        self.physical_pages[Config.SCHEMA_ENCODING_COLUMN].write_record_info_to_data(INITIAL_SCHEMA_ENCODING, rid)

        # write columns to data
        for i, entry_value in enumerate(record.get_columns()):
            self.physical_pages[i+Config.NUM_METADATA_COLUMNS].write_record_info_to_data(entry_value, rid)

        # set frame as dirty
        self.__set_dirty_bit()

    @__pin_frame_decorator
    def get_schema_encoding(self, rid:RID)->bitarray:
        rbarr = bitarray()
        rbarr.frombytes(self.physical_pages[Config.SCHEMA_ENCODING_COLUMN].read_record_info_from_data(int(rid)).to_bytes(Config.RECORD_FIELD_SIZE, "big"))
        rbarr = rbarr[-self.num_columns:]
        return rbarr

    @__pin_frame_decorator
    def set_schema_encoding(self, rid:RID, schema_encoding:bitarray)->None:
        schema_encoding = int(schema_encoding.to01(), 2)
        self.physical_pages[Config.SCHEMA_ENCODING_COLUMN].write_record_info_to_data(schema_encoding, int(rid))
        self.__set_dirty_bit()

    @__pin_frame_decorator
    def get_indirection_tid(self, rid:RID)->TID:
        return TID(self.physical_pages[Config.INDIRECTION_COLUMN].read_record_info_from_data(int(rid)))

    @__pin_frame_decorator
    def set_indirection_tid(self, id:RID, tid:TID)->None:
        self.physical_pages[Config.INDIRECTION_COLUMN].write_record_info_to_data(tid, int(id))
        self.__set_dirty_bit()

    @__pin_frame_decorator
    def get_record_entry(self, id:RID, column_index:int)->int:
        return self.physical_pages[column_index+Config.NUM_METADATA_COLUMNS].read_record_info_from_data(int(id))

    @__pin_frame_decorator
    def delete_record(self, rid:RID)->None:
        self.physical_pages[Config.RID_COLUMN].write_record_info_to_data(0, int(rid))
        self.__set_dirty_bit()


class Physical_Page:

    def __init__(self, physical_page_path:str, data:bytearray)->None:
        assert len(data) == Config.PHYSICAL_PAGE_SIZE
        self.physical_page_path:str  = physical_page_path
        self.data:bytearray          = data
        self.original_data:bytearray = data

    def __get_offset(self, rid:int)->int:
        return (rid - 1) * Config.RECORD_FIELD_SIZE % Config.PHYSICAL_PAGE_SIZE

    def keep_original_data_to_disk(self)->int:
        with io.open(self.physical_page_path, 'wb') as f:
            f.write(self.original_data)

    def write_data_to_disk(self)->int:
        with io.open(self.physical_page_path, 'wb') as f:
            f.write(self.data)

    def write_record_info_to_data(self, entry_value, id:int)->None:
        offset = self.__get_offset(id)
        self.data = self.data[:offset] + \
                    int(entry_value).to_bytes(Config.RECORD_FIELD_SIZE, "big", signed=True) + \
                    self.data[offset+Config.RECORD_FIELD_SIZE:]
        assert len(self.data) == Config.PHYSICAL_PAGE_SIZE

    def read_record_info_from_data(self, id:int)->int:
        offset = self.__get_offset(id)
        return int.from_bytes(self.data[offset:offset+Config.RECORD_FIELD_SIZE], "big", signed=True)

BUFFERPOOL = Bufferpool()
