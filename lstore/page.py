""" page module for lstore """

import os
from lstore.disk import DISK
from lstore.bufferpool import BUFFERPOOL
from lstore.record import Record, RID
from lstore import config as Config


class BasePage:
    """Base Page class"""

    def __init__(self, base_page_dir_path: str, base_page_index: int) -> None:
        self.base_page_index = base_page_index
        self.base_page_path = base_page_dir_path
        # nums of columns plus the META_DATA columns
        self.num_columns = self.__get_num_columns() + Config.META_DATA_NUM_COLUMNS

    def __get_num_columns(self) -> int:
        table_path = os.path.dirname(os.path.dirname(self.base_page_path))
        return DISK.read_metadata_from_disk(table_path)["num_columns"]

    def insert_record(self, record: Record) -> None:

        # for j in range(self.num_columns):
        #     print(f'Base page {self.base_page_index} physical page ({j})')
        #     for i in range(0, 4096, 8):
        #         print(int.from_bytes(BUFFERPOOL.frames[frame_index].physical_pages[j].data[i:i+8], byteorder='big')
        BUFFERPOOL.insert_record(
            page_path=self.base_page_path, record=record, num_columns=self.num_columns
        )

    # get data from bufferpool
    def get_data(self, rid: RID) -> tuple:
        """Get data from bufferpool"""
        return BUFFERPOOL.get_data_from_buffer(
            rid=rid, page_path=self.base_page_path, num_columns=self.num_columns
        )

    def get_meta_data(self, rid: RID) -> list[int]:
        return BUFFERPOOL.get_meta_data(
            rid=rid, path_to_page=self.base_page_path, num_columns=self.num_columns
        )

    def update_meta_data(self, rid: RID, meta_data: list) -> None:
        # print(f'Updating meta data for {rid.to_int()} with these meta {meta_data}')
        BUFFERPOOL.update_meta_data(
            rid=rid,
            path_to_page=self.base_page_path,
            num_columns=self.num_columns,
            meta_data=meta_data,
        )

    def delete_record(self, rid: RID) -> None:
        BUFFERPOOL.delete_record(
            rid=rid, page_path=self.base_page_path, num_columns=self.num_columns
        )


class TailPage:
    """Tail Page class"""

    def __init__(self, tail_page_dir_path: str, tail_page_index: int) -> None:
        self.tail_page_index = tail_page_index
        self.tail_page_path = tail_page_dir_path
        self.num_columns = self.__get_num_columns() + Config.META_DATA_NUM_COLUMNS

    def __get_num_columns(self) -> int:
        table_path = os.path.dirname(os.path.dirname(self.tail_page_path))
        return DISK.read_metadata_from_disk(table_path)["num_columns"]

    def insert_record(self, record: Record, record_meta_data: list) -> None:

        # print(f'Inserting TID {record.get_rid()} to path {self.tail_page_path} with meta data for the record: {record_meta_data}')
        # for j in range(self.num_columns):
        #     print(f'Base page {self.base_page_index} physical page ({j})')
        #     for i in range(0, 4096, 8):
        #         print(int.from_bytes(BUFFERPOOL.frames[frame_index].physical_pages[j].data[i:i+8], byteorder='big')
        BUFFERPOOL.insert_record(
            page_path=self.tail_page_path,
            record=record,
            num_columns=self.num_columns,
            record_meta_data=record_meta_data,
        )

    # get data from bufferpool
    def get_data(self, rid: RID) -> tuple:
        # print(f"selecting {self.tail_page_path} ")
        return BUFFERPOOL.get_data_from_buffer(
            rid=rid, page_path=self.tail_page_path, num_columns=self.num_columns
        )

    def get_meta_data(self, rid: RID) -> list[int]:
        return BUFFERPOOL.get_meta_data(
            rid=rid, path_to_page=self.tail_page_path, num_columns=self.num_columns
        )
