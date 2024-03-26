import os
from bitarray import bitarray
from enum import Enum
from threading import RLock
from copy import deepcopy

import lstore.config as Config
from lstore.disk import Disk
from lstore.bufferpool import BUFFERPOOL
from lstore.record_info import Record, RID, TID

class Page_Type(Enum):
    ANY = 0
    BASE = 1
    TAIL = 2


class Page_Range:

    def __init__(self, page_range_path:str, page_range_index:int, latest_tid:int, tps_index:int)->None:
        self.page_range_path:str            = page_range_path
        self.page_range_index:int           = page_range_index
        self.latest_tid:int                 = latest_tid
        self.tps_index:int                  = tps_index

        self.latch:RLock                     = RLock()

        self.base_pages:dict[int,Base_Page] = dict()
        self.tail_pages:dict[int,Tail_Page] = dict()
        self.__load_pages()

    def __del__(self)->None:
        metadata = Disk.read_from_path_metadata(self.page_range_path)
        metadata["latest_tid"] = self.latest_tid
        Disk.write_to_path_metadata(self.page_range_path, metadata)
        del self.base_pages
        self.base_pages = None
        del self.tail_pages
        self.tail_pages = None

    def __get_pages(self, page_type:Page_Type)->tuple[list[str],int]:
        page_dirs:list[str] = Disk.list_directories_in_path(self.page_range_path)
        for page_dir in page_dirs:
            match page_type:
                case Page_Type.BASE:
                    if os.path.basename(page_dir)[:2] != "BP":
                        page_dirs.remove(page_dir)
                case Page_Type.TAIL:
                    if os.path.basename(page_dir)[:2] != "TP":
                        page_dirs.remove(page_dir)
                case _:
                    continue
        return (page_dirs, len(page_dirs))

    def __load_pages(self)->None:
        """
        Load base pages from disk.
        """
        with self.latch:
            page_paths, num_pages = self.__get_pages(Page_Type.ANY)
            if not num_pages: return
            for page_path in page_paths:
                page_index = int(os.path.basename(page_path)[2:])
                metadata = Disk.read_from_path_metadata(page_path)
                match os.path.basename(page_path)[:2]:
                    case "BP": self.base_pages[page_index] = Base_Page(
                            metadata["base_page_path"],
                            metadata["base_page_index"],
                        )
                    case "TP": self.tail_pages[page_index] = Tail_Page(
                            metadata["tail_page_path"],
                            metadata["tail_page_index"],
                        )
                    case _: raise FileNotFoundError

    def __create_base_page(self, base_page_index)->None:
        base_page_path = os.path.join(self.page_range_path, f"BP{base_page_index}")
        if os.path.exists(base_page_path): raise FileExistsError
        Disk.create_path_directory(base_page_path)
        metadata = {
            "base_page_path": base_page_path,
            "base_page_index": base_page_index,
        }
        Disk.write_to_path_metadata(base_page_path, metadata)
        self.base_pages[base_page_index] = Base_Page(
            metadata["base_page_path"],
            metadata["base_page_index"],
        )

    def __access_base_page(self, base_page_index:int)->None:
        with self.latch:
            if not base_page_index in self.base_pages:
                self.__create_base_page(base_page_index)

    def __create_tail_page(self, tail_page_index)->None:
        tail_page_path = os.path.join(self.page_range_path, f"TP{tail_page_index}")
        if os.path.exists(tail_page_path): raise FileExistsError
        Disk.create_path_directory(tail_page_path)
        metadata = {
            "tail_page_path": tail_page_path,
            "tail_page_index": tail_page_index,
        }
        Disk.write_to_path_metadata(tail_page_path, metadata)
        self.tail_pages[tail_page_index] = Tail_Page(
            metadata["tail_page_path"],
            metadata["tail_page_index"],
        )

    def __access_tail_page(self, tail_page_index:int)->None:
        with self.latch:
            if not tail_page_index in self.tail_pages:
                self.__create_tail_page(tail_page_index)

    def __merge(self)->None:
        if not self.latest_tid % Config.MERGE_THRESHOLD:
            return
        # print("merge is happening")
        # with self.latch:
        #     # copy base pages associated w/ page range
        #     base_pages = deepcopy(self.base_pages)

        #     # determine number of columns
        #     num_columns:dict = Disk.read_from_path_metadata(os.path.dirname(self.page_range_path))["num_columns"]

    def insert_record(self, record:Record)->None:
        """
        Insert record to a base page in a page range.
        """
        self.__access_base_page(record.get_base_page_index())
        self.base_pages[record.get_base_page_index()].insert_record(record)

    def get_record_columns(self, rid:RID, rollback_version:int)->tuple:
        """
        Get Record columns
        """
        columns = list()
        # print(f"GETTING COLUMNS FOR RID {rid} WITH {abs(rollback_version)} ROLLBACKS")
        tid = self.base_pages[rid.get_base_page_index()].get_indirection_tid(rid)
        schema_encoding = self.base_pages[rid.get_base_page_index()].get_schema_encoding(rid)
        while rollback_version < 0 and int(tid) != -1:
            tid = self.tail_pages[tid.get_tail_page_index()].get_indirection_tid(tid)
            rollback_version += 1
        # print(f"ACCESSING TID {tid}")
        for i, bit in enumerate(schema_encoding):
            if not bit or int(tid) == -1:
                columns.append(self.base_pages[rid.get_base_page_index()].select_record(rid, i))
            else:
                columns.append(self.tail_pages[tid.get_tail_page_index()].select_record(tid, i))
        # print(f"RID {rid} HAS COLUMNS {columns}")
        return tuple(columns)

    def update_record(self, rid:RID, old_columns:tuple, new_columns:tuple)->None:
        """
        Update Record
        """
        self.__access_base_page(rid.get_base_page_index())
        tid = self.base_pages[rid.get_base_page_index()].get_indirection_tid(rid)

        # convert columns to lists
        old_columns = list(old_columns)
        new_columns = list(new_columns)

        # adjust schema encoding
        schema_encoding = self.base_pages[rid.get_base_page_index()].get_schema_encoding(rid)
        for i in range(len(old_columns)):
            if new_columns[i] != None and old_columns[i] != new_columns[i]:
                schema_encoding[i] = True
                old_columns[i] = new_columns[i]
        self.base_pages[rid.get_base_page_index()].set_schema_encoding(rid, schema_encoding)

        # increment number of TIDs in a page range
        self.latest_tid += 1
        new_tid = TID(deepcopy(self.latest_tid))
        self.__access_tail_page(new_tid.get_tail_page_index())

        # write new data to new TID
        key_index = Disk.read_from_path_metadata(os.path.dirname(self.page_range_path))["key_index"]
        new_record = Record(new_tid, key_index, tuple(old_columns))
        self.tail_pages[new_tid.get_tail_page_index()].insert_record(new_record)

        # handle indirection
        self.base_pages[rid.get_base_page_index()].set_indirection_tid(rid, new_tid)
        if not int(tid) == -1:
            self.tail_pages[new_tid.get_tail_page_index()].set_indirection_tid(new_tid, tid)

        # print(f"UPDATED COLUMNS FOR RID {rid} TO {old_columns}")

        # perform merging if necessary
        # self.__merge()

    def delete_record(self, rid:RID)->None:
        """
        Delete Record
        """
        self.__access_base_page(rid.get_base_page_index())
        self.base_pages[rid.get_base_page_index()].delete_record(rid)


class Base_Page:

    def __init__(self, base_page_path:str, base_page_index:int)->None:
        self.base_page_path = base_page_path
        self.base_page_index = base_page_index

    def insert_record(self, record:Record)->None:
        """
        Insert Base Record
        """
        BUFFERPOOL.insert_record(record, self.base_page_path)

    def get_schema_encoding(self, rid:RID)->bitarray:
        """
        Get schema encoding for Base Record
        """
        return BUFFERPOOL.get_schema_encoding(rid, self.base_page_path)

    def set_schema_encoding(self, rid:RID, schema_encoding:bitarray)->None:
        """
        Set schema encoding for Base Record
        """
        BUFFERPOOL.set_schema_encoding(rid, schema_encoding, self.base_page_path)

    def get_indirection_tid(self, rid:RID)->TID:
        """
        Get indirection for Base Record
        """
        return BUFFERPOOL.get_indirection_tid(rid, self.base_page_path)

    def set_indirection_tid(self, rid:RID, tid:TID)->None:
        """
        Set indirection for Base Record
        """
        BUFFERPOOL.set_indirection_tid(rid, tid, self.base_page_path)

    def select_record(self, rid:RID, column_index:int)->int:
        """
        Select Base Record
        """
        return BUFFERPOOL.get_record_entry(rid, self.base_page_path, column_index)

    def delete_record(self, rid:RID)->None:
        """
        Delete Base Record
        """
        BUFFERPOOL.delete_record(rid, self.base_page_path)


class Tail_Page:

    def __init__(self, tail_page_path:str, tail_page_index:int)->None:
        self.tail_page_path = tail_page_path
        self.tail_page_index = tail_page_index

    def insert_record(self, record:Record)->None:
        """
        Insert Tail Record
        """
        BUFFERPOOL.insert_record(record, self.tail_page_path)

    def select_record(self, tid:TID, column_index:int)->int:
        """
        Select Tail Record
        """ 
        return BUFFERPOOL.get_record_entry(tid, self.tail_page_path, column_index)

    def get_indirection_tid(self, tid:TID)->TID:
        """
        Get indirection for Tail Record
        """
        return BUFFERPOOL.get_indirection_tid(tid, self.tail_page_path)

    def set_indirection_tid(self, tid:TID, indirection_tid:TID)->None:
        """
        Set indirection for Tail Record
        """
        BUFFERPOOL.set_indirection_tid(tid, indirection_tid, self.tail_page_path)
