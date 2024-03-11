""" This module contains the Page Range class. The Page Range class represents a range of pages in the database. """

import os
import lstore.config as Config
from lstore.disk import DISK
from lstore.page import BasePage, TailPage
from lstore.record import Record, RID


class PageRange:
    """Page Range class. Represents a range of pages in the database."""

    def __init__(
        self, page_range_dir_path: str, page_range_index: int, tps_index: int
    ) -> None:
        self.page_range_dir_path: str = page_range_dir_path
        self.page_range_index: int = page_range_index
        self.tps_index: int = tps_index
        self.tid = 0

        self.base_pages: dict[int, BasePage] = dict()
        if self.__get_num_base_pages():
            self.__load_base_pages()

        self.tail_pages: dict[int, TailPage] = dict()
        if self.__get_num_tail_pages():
            self.__load_tail_pages()

    def __tid_count(self) -> int:
        return self.tid

    def __decrement_tid(self) -> int:
        self.tid -= 1
        return self.tid

    def __get_num_base_pages(self):
        return len(
            [
                os.path.join(self.page_range_dir_path, _)
                for _ in os.listdir(self.page_range_dir_path)
                if os.path.isdir(os.path.join(self.page_range_dir_path, _))
                and _.startswith("BP")
            ]
        )

    def __get_num_tail_pages(self):
        return len(
            [
                os.path.join(self.page_range_dir_path, _)
                for _ in os.listdir(self.page_range_dir_path)
                if os.path.isdir(os.path.join(self.page_range_dir_path, _))
                and _.startswith("TP")
            ]
        )

    def __load_base_pages(self) -> None:
        base_page_dirs = [
            os.path.join(self.page_range_dir_path, _)
            for _ in os.listdir(self.page_range_dir_path)
            if os.path.isdir(os.path.join(self.page_range_dir_path, _))
            and _.startswith("BP")
        ]
        for base_page_dir in base_page_dirs:
            base_page_index = int(base_page_dir.removeprefix("BP"))
            metadata = DISK.read_metadata_from_disk(base_page_dir)
            self.base_pages[base_page_index] = BasePage(
                base_page_dir_path=metadata["base_page_dir_path"],
                base_page_index=metadata["base_page_index"],
            )

    def __load_tail_pages(self) -> None:
        tail_page_dirs = [
            os.path.join(self.page_range_dir_path, _)
            for _ in os.listdir(self.page_range_dir_path)
            if os.path.isdir(os.path.join(self.page_range_dir_path, _))
            and _.startswith("TP")
        ]
        for tail_page_dir in tail_page_dirs:
            tail_page_index = int(tail_page_dir.removeprefix("TP"))
            metadata = DISK.read_metadata_from_disk(tail_page_dir)
            self.tail_pages[tail_page_index] = TailPage(
                metadata["tail_page_dir_path"], metadata["tail_page_index"]
            )

    def create_base_page(self, base_page_index: int) -> None:
        """
        Creates a base page directory in disk.
        """

        base_page_dir_path = os.path.join(
            self.page_range_dir_path, f"BP{base_page_index}"
        )
        if os.path.exists(base_page_dir_path):
            raise ValueError

        DISK.create_path_directory(base_page_dir_path)
        metadata = {
            "base_page_dir_path": base_page_dir_path,
            "base_page_index": base_page_index,
        }
        DISK.write_metadata_to_disk(base_page_dir_path, metadata)
        self.base_pages[base_page_index] = BasePage(
            base_page_dir_path, base_page_index
        )

    def create_tail_page(self, tail_page_index: int) -> None:
        """
        Creates a tail page directory in disk.
        """

        tail_page_dir_path = os.path.join(
            self.page_range_dir_path, f"TP{tail_page_index}"
        )
        if os.path.exists(tail_page_dir_path):
            raise ValueError()

        DISK.create_path_directory(tail_page_dir_path)
        metadata = {
            "tail_page_dir_path": tail_page_dir_path,
            "tail_page_index": tail_page_index,
        }
        DISK.write_metadata_to_disk(tail_page_dir_path, metadata)
        self.tail_pages[tail_page_index] = TailPage(
            tail_page_dir_path, tail_page_index
        )

    def insert_record(
        self, record: Record, page_type: str = "Base", record_meta_data: list = None
    ) -> None:
        """
        Insert record into page range.
        """
        # print("INSERT PAGE_RANGE")

        # Checks if base page exists to put in new record, else create one
        if page_type == "Base":
            if (
                len(self.base_pages) == 0
                or record.get_base_page_index() not in self.base_pages
            ):
                self.create_base_page(self.__get_num_base_pages())

            # Appends new record to base page
            self.base_pages[record.get_base_page_index()].insert_record(record)

        elif page_type == "Tail":
            # Checks if tail pages list is empty or if it's reached the limit of records in tail page
            if (
                len(self.tail_pages) == 0
                or (self.__tid_count() + 1) % Config.RECORDS_PER_PAGE == 0
            ):
                self.create_tail_page(self.__get_num_tail_pages())

            # Appends new record to tail page

            self.tail_pages[record.rid.get_tail_page_index()].insert_record(
                record=record, record_meta_data=record_meta_data
            )

    def update_record(self, record: Record, record_meta_data: list) -> int:
        """update record from page range."""
        # change rid to be a tid which corresponds to this specific page range only
        record.rid = RID(self.__decrement_tid())
        self.insert_record(
            record=record, page_type="Tail", record_meta_data=record_meta_data
        )
        return record.get_rid()

    def get_data(self, rid: RID, page_type: str = "Base") -> tuple:
        """get data from page in page range"""
        # Check if page type is 'Base'
        if page_type == "Base":
            base_page_index = rid.get_base_page_index()
            # Check if base page index is in base pages dictionary
            if base_page_index not in self.base_pages:
                raise ValueError("Base page index not found.")
            # Retrieve data from base page and return
            return self.base_pages[base_page_index].get_data(rid=rid)

        else:
            tail_page_index = rid.get_tail_page_index()
            # Check if tail page index is in tail pages dictionary
            if tail_page_index not in self.tail_pages:
                raise ValueError("Tail page index not found.")
            # Retrieve data from tail page and return
            return self.tail_pages[tail_page_index].get_data(rid=rid)

    # returns list of meta data
    def get_meta_data(self, rid: RID) -> list[int]:
        """
        get meta data from basepage in page range
        """
        if rid.get_base_page_index() not in self.base_pages:
            raise ValueError

        return self.base_pages[rid.get_base_page_index()].get_meta_data(rid=rid)

    def get_tail_meta_data(self, rid: RID) -> list[int]:
        """
        get meta data from tailpage in page range
        """
        if rid.get_tail_page_index() not in self.tail_pages:
            raise ValueError

        return self.tail_pages[rid.get_tail_page_index()].get_meta_data(rid=rid)

    def update_meta_data(self, rid: RID, meta_data: list) -> bool:
        """
        Update record from page range.
        """

        if rid.get_base_page_index() not in self.base_pages:
            raise ValueError

        self.base_pages[rid.get_base_page_index()].update_meta_data(
            rid=rid, meta_data=meta_data
        )
        return True

    def delete_record(self, rid: RID) -> None:
        """
        Delete record from page range.
        """

        if rid.get_base_page_index() not in self.base_pages:
            raise ValueError

        self.base_pages[rid.get_base_page_index()].delete_record(rid)
