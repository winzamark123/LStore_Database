""" This file contains the Frame class, which is a data structure that holds a list of PhysicalPage objects. The Frame class is used to manage the physical pages of a page in the buffer pool. The Frame class is used to manage the physical pages of a page in the buffer pool. The Frame class is used to manage the physical pages of a page in the buffer pool. The Frame class is used to manage the physical pages of a page in the buffer pool. The Frame class is used to manage the physical pages of a page in the buffer pool. The Frame class is used to manage the physical pages of a page in the buffer pool. The Frame class is used to manage the physical pages of a page in the buffer pool. The Frame class is used to manage the physical pages of a page in the buffer pool. The Frame class is used to manage the physical pages of a page in the buffer pool. The Frame class is used to manage the physical pages of a page in the buffer pool."""

import os
from datetime import datetime

from lstore.disk import DISK
import lstore.config as Config
from lstore.record import Record, RID
from lstore.physical_page import PhysicalPage


class Frame:
    """Frame class"""

    def __init__(self, path_to_page: str):
        self.is_dirty = 0  # Boolean to check if the physical_page has been modified
        self.pin_count = 0
        self.is_pin = False
        self.physical_pages: list[PhysicalPage] = []
        self.path_to_page = path_to_page
        self.last_time_used = datetime.now()

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

    def load_data(self, num_columns: int, path_to_page: str) -> None:
        self.pin_frame()

        for i in range(num_columns):
            # self.physical_pages.append(PhysicalPage()) path_to_physical_page = f"{path_to_page}/{i}.bin"

            path_to_physical_page = f"{path_to_page}/{i}.bin"
            # Check if the file exists to decide whether to read from it or initialize a new one
            if os.path.exists(path_to_physical_page):
                self.physical_pages.append(
                    DISK.read_physical_page_from_disk(path_to_physical_page)
                )

            else:
                # If the file does not exist, you may need to create and initialize it
                # Example: initializing an empty file
                with open(path_to_physical_page, "wb") as f:
                    # Initialize the file if needed; for example, writing empty bytes:

                    f.write(
                        b"\x00" * Config.PHYSICAL_PAGE_SIZE
                    )  # Adjust this according to your data structure needs
                self.physical_pages.append(
                    DISK.read_physical_page_from_disk(path_to_physical_page)
                )

        self.unpin_frame()

    def insert_record(self, record: Record, record_meta_data: list = None) -> None:
        """insert record to the pp in the frame"""
        self.pin_frame()
        rid = record.get_rid()
        # print(f"Rid putting inputted {rid}")
        if record_meta_data is None:
            for i, pp in enumerate(self.physical_pages):

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
                    pp.edit_byte_array(
                        record.columns[i - Config.META_DATA_NUM_COLUMNS], rid
                    )
        else:

            everything = []
            for i, pp in enumerate(self.physical_pages):
                if i == Config.RID_COLUMN:
                    everything.append(rid)
                    pp.edit_byte_array(value=rid, rid=rid)
                elif i == Config.INDIRECTION_COLUMN:
                    everything.append(record_meta_data[Config.INDIRECTION_COLUMN])
                    pp.edit_byte_array(
                        value=record_meta_data[Config.INDIRECTION_COLUMN], rid=rid
                    )
                elif i == Config.BASE_RID_COLUMN:
                    everything.append(record_meta_data[Config.BASE_RID_COLUMN])
                    pp.edit_byte_array(
                        value=record_meta_data[Config.BASE_RID_COLUMN], rid=rid
                    )
                elif i == Config.SCHEMA_ENCODING_COLUMN:
                    everything.append(record_meta_data[Config.SCHEMA_ENCODING_COLUMN])
                    pp.edit_byte_array(
                        value=record_meta_data[Config.SCHEMA_ENCODING_COLUMN], rid=rid
                    )
                else:
                    everything.append(record.columns[i - Config.META_DATA_NUM_COLUMNS])
                    pp.edit_byte_array(
                        value=record.columns[i - Config.META_DATA_NUM_COLUMNS], rid=rid
                    )

            # print(f"inserting tail record {rid}, {everything}")
        if not self.check_dirty_status():
            # print("frame set to dirty")
            # sets frame to be dirty
            self.set_dirty()

        self.unpin_frame()

    def delete_record(self, rid: RID) -> None:
        """delete record from the pp in the frame"""

        self.pin_frame()

        for i, physical_page in enumerate(self.physical_pages):
            if i == Config.RID_COLUMN:
                physical_page.edit_byte_array(value=0, rid=rid)

        if not self.check_dirty_status():
            self.set_dirty()

        self.unpin_frame()

    def update_meta_data(self, rid: RID, meta_data: list):
        self.pin_frame()

        rid = rid.to_int()

        # print(f'Updating meta data for base record {rid}')
        # updates base record indirection and schema encoding
        for i, pp in enumerate(self.physical_pages):
            if i == Config.INDIRECTION_COLUMN:
                pp.edit_byte_array(value=meta_data[0], rid=rid)
            elif i == Config.SCHEMA_ENCODING_COLUMN:
                pp.edit_byte_array(value=meta_data[1], rid=rid)

        self.unpin_frame()

    def get_data(self, rid: RID) -> tuple:
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
        # print(f'data columns: {data_columns}')
        self.unpin_frame()
        return data_columns

    # gets meta data for page
    def get_meta_data(self, rid: RID) -> list[int]:

        self.pin_frame()
        meta_data_columns = []
        for i, physical_page in enumerate(self.physical_pages):
            if i == Config.RID_COLUMN:
                meta_data_columns.append(physical_page.get_data(rid))
            elif i == Config.INDIRECTION_COLUMN:
                meta_data_columns.append(physical_page.get_data(rid))
            elif i == Config.BASE_RID_COLUMN:
                meta_data_columns.append(physical_page.get_data(rid))
            elif i == Config.SCHEMA_ENCODING_COLUMN:
                meta_data_columns.append(physical_page.get_data(rid))
            else:
                continue
        # print(f'meta_data columns: {meta_data_columns}')
        self.unpin_frame()
        return meta_data_columns
