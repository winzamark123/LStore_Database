import lstore.config as Config
from lstore.disk import DISK
from lstore.page import Base_Page, Tail_Page
from lstore.record import Record, RID
import os

class Page_Range:
    def __init__(self, page_range_dir_path:str, page_range_index:int, tps_index:int)->None:
        self.page_range_dir_path:str        = page_range_dir_path
        self.page_range_index:int           = page_range_index
        self.tps_index:int                  = tps_index

        self.base_pages:dict[int,Base_Page] = dict()
        if self.__get_num_base_pages():
            self.base_pages = self.__load_base_pages()

        self.tail_pages:dict[int,Tail_Page] = dict()
        if self.__get_num_tail_pages():
            self.tail_pages = self.__load_tail_pages()

    def __get_num_base_pages(self):
        return len([
            os.path.join(self.page_range_dir_path, _) for _ in os.listdir(self.page_range_dir_path)
            if os.path.isdir(os.path.join(self.page_range_dir_path, _)) and _.startswith("BP")
        ])

    def __get_num_tail_pages(self):
        return len([
            os.path.join(self.page_range_dir_path, _) for _ in os.listdir(self.page_range_dir_path)
            if os.path.isdir(os.path.join(self.page_range_dir_path, _)) and _.startswith("TP")
        ])

    def __load_base_pages(self):
        base_page_dirs = [
            os.path.join(self.page_range_dir_path, _) for _ in os.listdir(self.page_range_dir_path)
            if os.path.isdir(os.path.join(self.page_range_dir_path, _)) and _.startswith("BP")
        ]
        for base_page_dir in base_page_dirs:
            base_page_index = int(base_page_dir.removeprefix("BP"))
            metadata = DISK.read_metadata_from_disk(base_page_dir)
            self.base_pages[base_page_index] = \
                Base_Page(
                    base_page_dir_path=metadata["base_page_dir_path"],
                    base_page_index=metadata["base_page_index"],
                )

    def __load_tail_pages(self):
        tail_page_dirs = [
            os.path.join(self.page_range_dir_path, _) for _ in os.listdir(self.page_range_dir_path)
            if os.path.isdir(os.path.join(self.page_range_dir_path, _)) and _.startswith("TP")
        ]
        for tail_page_dir in tail_page_dirs:
            tail_page_index = int(tail_page_dir.removeprefix("TP"))
            metadata = DISK.read_metadata_from_disk(tail_page_dir)
            self.tail_pages[tail_page_index] = \
                Tail_Page(
                    metadata["tail_page_dir_path"],
                    metadata["tail_page_index"]
                )

    def create_base_page(self, base_page_index:int) -> Base_Page:
        """
        Creates a base page directory in disk.
        """

        base_page_dir_path = os.path.join(self.page_range_dir_path, f"BP{base_page_index}")
        if os.path.exists(base_page_dir_path):
            raise ValueError

        DISK.create_path_directory(base_page_dir_path)
        metadata = {
            "base_page_dir_path": base_page_dir_path,
            "base_page_index": base_page_index,
        }
        DISK.write_metadata_to_disk(base_page_dir_path, metadata)
        self.base_pages[base_page_index] = Base_Page(base_page_dir_path, base_page_index)

    def create_tail_page(self, tail_page_index:int) -> Base_Page:
        """
        Creates a tail page directory in disk.
        """

        tail_page_dir_path = os.path.join(self.page_range_dir_path, f"TP{tail_page_index}")
        if os.path.exists(tail_page_dir_path):
            raise ValueError()

        DISK.create_path_directory(tail_page_dir_path)
        metadata = {
            "tail_page_dir_path": tail_page_dir_path,
            "tail_page_index": tail_page_index,
        }
        DISK.write_metadata_to_disk(tail_page_dir_path, metadata)
        self.tail_pages[tail_page_index] = Tail_Page(tail_page_dir_path, tail_page_index)

    def insert_record(self, record:Record)->None:
        """
        Insert record into page range.
        """
        print("INSERT PAGE_RANGE")
        # checks if base page exists to put in new record, else create one
        if len(self.base_pages) == 0 or not record.get_base_page_index() in self.base_pages:
            self.create_base_page(self.__get_num_base_pages())
        
        # appends new record to base page
        self.base_pages[record.get_base_page_index()].insert_record(record)

    def get_data(self, rid:RID)->tuple:
        """
        Get data from page range.
        """
        if rid.get_base_page_index() not in self.base_pages:
            raise ValueError
        
        return self.base_pages[rid.get_base_page_index()].get_data(rid=rid)
    

    def update_record(self, rid:int, updated_column:tuple)->None:
        """
        Update record from page range.
        """

        if rid.get_base_page_index() not in self.base_pages:
            raise ValueError
        
        self.base_pages[rid.get_base_page_index()].update_record(rid, updated_column)

    def delete_record(self, rid:RID)->None:
        """
        Delete record from page range.
        """
        
        if rid.get_base_page_index() not in self.base_pages:
            raise ValueError
        
        self.base_pages[rid.get_base_page_index()].delete_record(rid)
