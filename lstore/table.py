"""table module for lstore"""

import os
import threading
import copy
from lstore.index import Index
from lstore.physical_page import PhysicalPage
import lstore.config as Config
from lstore.page_range import PageRange
from lstore.disk import DISK
from lstore.record import Record, RID


class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns
    :param key: int             #Index of table key in columns
    """

    def __init__(
        self, table_dir_path: str, num_columns: int, key_index: int, num_records: int
    ) -> None:
        self.table_dir_path: str = table_dir_path
        self.num_columns: int = num_columns
        self.key_index: int = key_index
        # self.key_column:int        = Config.META_DATA_NUM_COLUMNS + key_index
        self.num_records: int = num_records

        self.index: Index = Index(
            table_dir_path=table_dir_path,
            num_columns=num_columns,
            primary_key_index=key_index,
            order=Config.ORDER_CHOICE,
        )

        self.page_ranges: dict[int, PageRange] = dict()
        if self.__get_num_page_ranges():
            self.load_page_ranges()

    def __update_num_records_in_metadata(self) -> None:
        metadata = DISK.read_metadata_from_disk(self.table_dir_path)
        metadata["num_records"] = self.num_records
        DISK.write_metadata_to_disk(self.table_dir_path, metadata)

    def __increment_num_records(self) -> int:
        self.num_records += 1
        self.__update_num_records_in_metadata()
        return self.num_records

    def __decrement_num_records(self) -> int:
        self.num_records -= 1
        self.__update_num_records_in_metadata()
        return self.num_records

    def __get_num_page_ranges(self) -> int:
        count = 0
        for _ in os.listdir(self.table_dir_path):
            if _ == "index":
                continue
            elif os.path.isdir(os.path.join(self.table_dir_path, _)):
                count += 1

        # print(f'Page Ranges {count}')
        return count

    def load_page_ranges(self):
        """load page ranges from disk"""
        page_range_dirs = [
            os.path.join(self.table_dir_path, _)
            for _ in os.listdir(self.table_dir_path)
            if os.path.isdir(os.path.join(self.table_dir_path, _))
        ]
        for page_range_dir in page_range_dirs:
            page_range_index = int(os.path.basename(page_range_dir).removeprefix("PR"))
            metadata = DISK.read_metadata_from_disk(self.table_dir_path)
            self.page_ranges[page_range_index] = PageRange(
                page_range_dir_path=metadata["page_range_dir_path"],
                page_range_index=metadata["page_range_index"],
                tps_index=metadata["tps_index"],
            )

    def create_page_range(self, page_range_index: int) -> None:
        """
        Creates a page range directory in disk
        """
        page_range_dir_path = os.path.join(self.table_dir_path, f"PR{page_range_index}")
        if os.path.exists(page_range_dir_path):
            raise ValueError

        DISK.create_path_directory(page_range_dir_path)
        metadata = {
            "page_range_dir_path": page_range_dir_path,
            "page_range_index": page_range_index,
            "tps_index": 0,
        }
        DISK.write_metadata_to_disk(page_range_dir_path, metadata)
        self.page_ranges[page_range_index] = PageRange(
            page_range_dir_path=page_range_dir_path,
            page_range_index=page_range_index,
            tps_index=0,
        )

    def create_record(self, columns: tuple, record_type: str = "Base") -> Record:
        """
        Create a record.
        """

        if len(columns) != self.num_columns:
            raise ValueError

        # print("CREATE RECORD")
        if record_type == "Base":
            return Record(
                rid=self.__increment_num_records(),
                key=columns[self.key_index],
                columns=columns,
            )

        return Record(rid=0, key=columns[self.key_index], columns=columns)

    def insert_record(self, record: Record) -> None:
        """
        Insert record into table.
        """

        # checks if a page range is available
        if (
            self.__get_num_page_ranges() == 0
            or not record.get_page_range_index() in self.page_ranges
        ):
            self.create_page_range(self.__get_num_page_ranges())

        # insert record to index
        self.index.insert_record(
            record_columns=record.get_columns(), rid=record.get_rid()
        )

        # insert record to page range
        self.page_ranges[record.get_page_range_index()].insert_record(record=record)

    def get_data(self, rid: RID) -> tuple:
        """
        Get data for record from table.
        """

        if not rid.get_page_range_index() in self.page_ranges:
            raise ValueError
        return self.page_ranges[rid.get_page_range_index()].get_data(rid)

    def select_version(self, rid: RID, roll_back: int) -> tuple:
        """select based on relative version"""
        cur_meta_data = self.page_ranges[rid.get_page_range_index()].get_meta_data(rid)

        while roll_back < 0:
            # print(f"Rolling back {roll_back} times")
            cur_tid = RID(rid=cur_meta_data[Config.INDIRECTION_COLUMN])
            # print(f"Current TID: {cur_tid.to_int()}")
            cur_meta_data = self.page_ranges[
                rid.get_page_range_index()
            ].get_tail_meta_data(cur_tid)

            roll_back += 1

        # get the rest of the schema encoding with 0s
        schema_encoding = cur_meta_data[Config.SCHEMA_ENCODING_COLUMN]
        # Get indexes of schema encoding that have 0s
        list_of_columns_updated_0 = self.__analyze_schema_encoding(
            schema_encoding=schema_encoding, zero=True
        )
        # Get indexes of schema encoding that have 0s
        list_of_columns_updated_1 = self.__analyze_schema_encoding(
            schema_encoding=schema_encoding
        )
        # Base record columns
        base_columns = self.page_ranges[rid.get_page_range_index()].get_data(rid=rid)
        tail_columns = self.page_ranges[rid.get_page_range_index()].get_data(
            rid=cur_tid, page_type="Tail"
        )

        dict_values = {}

        for i in list_of_columns_updated_0:
            dict_values[i] = base_columns[i]

        for i in list_of_columns_updated_1:
            dict_values[i] = tail_columns[i]

        # Sort the dictionary based on keys
        sorted_dict = {k: dict_values[k] for k in sorted(dict_values)}

        # Extract values and create a tuple
        values_tuple = tuple(sorted_dict.values())

        return values_tuple

    def select(self, rid: RID) -> tuple:
        """
        Select most up to date columns for record
        """
        # Get base meta data
        base_meta_data = self.page_ranges[rid.get_page_range_index()].get_meta_data(rid)

        # if RID is equal to the indirection (most up to date)
        if base_meta_data[Config.INDIRECTION_COLUMN] == rid.to_int():
            return self.page_ranges[rid.get_page_range_index()].get_data(rid=rid)

        # Make tid to tail record columns
        tid = RID(rid=base_meta_data[Config.INDIRECTION_COLUMN])
        tail_record_columns = self.page_ranges[rid.get_page_range_index()].get_data(
            rid=tid, page_type="Tail"
        )

        # schema encoding
        schema_encoding = base_meta_data[Config.SCHEMA_ENCODING_COLUMN]

        if schema_encoding == 2 ** (self.num_columns - 1):
            return

        # Get indexes of schema encoding that have 1s and 0s
        list_of_columns_updated_0 = self.__analyze_schema_encoding(
            schema_encoding=schema_encoding, zero=True
        )
        list_of_columns_updated_1 = self.__analyze_schema_encoding(
            schema_encoding=schema_encoding
        )

        # Base record columns
        base_columns = self.page_ranges[rid.get_page_range_index()].get_data(rid=rid)

        dict_values = {}

        for i in list_of_columns_updated_0:
            dict_values[i] = base_columns[i]

        for i in list_of_columns_updated_1:
            dict_values[i] = tail_record_columns[i]

        # Sort the dictionary based on keys
        sorted_dict = {k: dict_values[k] for k in sorted(dict_values)}

        # Extract values and create a tuple
        values_tuple = tuple(sorted_dict.values())

        return values_tuple

    def update_record(self, rid: RID, updated_column: tuple) -> None:
        """
        Update record from table.
        """
        # Ensure that the page range index exists
        if rid.get_page_range_index() not in self.page_ranges:
            raise ValueError("Page range index not found.")

        # Get meta data of the base record to update
        base_meta_data = self.page_ranges[rid.get_page_range_index()].get_meta_data(
            rid=rid
        )

        # Create meta data for the tail record
        record_meta_data = base_meta_data.copy()
        record_meta_data[Config.BASE_RID_COLUMN] = rid.to_int()
        new_schema_encoding = base_meta_data[
            Config.SCHEMA_ENCODING_COLUMN
        ] | self.__get_schema_encoding(list(updated_column))

        # Check if the indirection is equal to itself
        if base_meta_data[Config.INDIRECTION_COLUMN] == rid.to_int():
            # print(f"First update to RID ({rid.to_int()})")
            base_columns = self.page_ranges[rid.get_page_range_index()].get_data(
                rid=rid
            )

            # Add a copy of the base record
            prev_tid = self.page_ranges[rid.get_page_range_index()].update_record(
                record=self.create_record(columns=base_columns, record_type="Tail"),
                record_meta_data=record_meta_data,
            )

            # Modify columns, otherwise make them 0
            modified_columns = tuple(
                0 if val is None or (i == self.key_index) else val
                for i, val in enumerate(updated_column)
            )

            # Change indirection to point to the previous tail record
            record_meta_data[Config.INDIRECTION_COLUMN] = prev_tid
            record_meta_data[Config.SCHEMA_ENCODING_COLUMN] = new_schema_encoding

            prev_tid = self.page_ranges[rid.get_page_range_index()].update_record(
                record=self.create_record(columns=modified_columns, record_type="Tail"),
                record_meta_data=record_meta_data,
            )

        else:
            # print("Adding to a tail record")
            # Find the TID to update
            tid_to_find = RID(rid=record_meta_data[Config.INDIRECTION_COLUMN])

            # Retrieve tail record columns
            tail_columns = self.page_ranges[rid.get_page_range_index()].get_data(
                rid=tid_to_find, page_type="Tail"
            )
            tail_columns_list = list(tail_columns)

            # Get list of updated columns in the tail record using the schema encoding
            updated_columns_indices = self.__analyze_schema_encoding(
                schema_encoding=record_meta_data[Config.SCHEMA_ENCODING_COLUMN]
            )

            # Initialize update_list with zeros
            update_list = [0] * self.num_columns

            # Populate update_list with values from the previous tail record
            for index in range(self.num_columns):
                if index in updated_columns_indices:
                    update_list[index] = tail_columns_list[index]

            # Check if the updated_column tuple contains integers
            for index, item in enumerate(list(updated_column)):
                if isinstance(item, int):
                    update_list[index] = item

            # Update the tail record's schema encoding
            record_meta_data[Config.SCHEMA_ENCODING_COLUMN] = new_schema_encoding

            # Update the base record
            prev_tid = self.page_ranges[rid.get_page_range_index()].update_record(
                record=self.create_record(
                    columns=tuple(update_list), record_type="Tail"
                ),
                record_meta_data=record_meta_data,
            )

        # Finish the update by updating the base record's indirection
        update_meta_columns = [prev_tid, new_schema_encoding]
        if self.page_ranges[rid.get_page_range_index()].update_meta_data(
            rid=rid, meta_data=update_meta_columns
        ):
            return True

        return False

    def __get_schema_encoding(self, columns: list):
        schema_encoding = ""
        for item in columns:
            # if value in column is not 'None' add 1
            if item or item == 0:
                schema_encoding = schema_encoding + "1"
            # else add 0
            else:
                schema_encoding = schema_encoding + "0"
        return int(schema_encoding, 2)

    # help determine what columns have been updated
    def __analyze_schema_encoding(
        self, schema_encoding: int, zero: bool = False
    ) -> list:
        schema_encoding = abs(schema_encoding)
        if not isinstance(schema_encoding, int):
            raise TypeError("Schema encoding must be an integer.")

        if schema_encoding < 0 or schema_encoding >= 2 ** (self.num_columns):
            raise ValueError("Schema encoding must be a 5-bit integer (0-31).")

        # Initialize an empty list to store positions of bits with value 1
        positions = []

        # Iterate through each bit position
        if not zero:
            for i in range(0, self.num_columns):
                if i != self.key_index:
                    # Check if the bit at position i is 1
                    if schema_encoding & (1 << (4 - i)):
                        positions.append(i)
        else:
            for i in range(0, self.num_columns):
                # Check if the bit at position i is 0
                if not schema_encoding & (1 << (4 - i)):
                    positions.append(i)

        return positions

    def delete_record(self, rid: RID) -> None:
        """
        Delete record from table.
        """

        if not rid.get_page_range_index() in self.page_ranges:
            raise ValueError

        self.page_ranges[rid.get_page_range_index()].delete_record(rid)

        # remove from index
        record_data = self.select(rid)
        self.index.delete_record(record_data, rid.to_int())

        self.__decrement_num_records()

    # Get Page Range and Base Page from RID
    def get_list_of_addresses(self, rids) -> list:
        addreses = []

        for rid in rids:
            rid -= 1
            page_range_num = rid // (Config.RECORDS_PER_PAGE * Config.NUM_BASE_PAGES)
            base_page_num = (rid // Config.RECORDS_PER_PAGE) % Config.NUM_BASE_PAGES
            addreses.append((page_range_num, base_page_num))
        # return page_range_num, base_page_num
        return addreses

    # TODO: complete merge (in the works: Testing in merge_test.py)
    # Implementing without bufferpool right now, will when bufferpool is finished
    @staticmethod
    def __merge(page_range: PageRange):
        # print("\nMerge starting!!")
        # print(f"TPS before merge: {page_range.tps_range}")
        # records that need updating in base page
        updated_records = {}

        target_page_num = 1

        # checking until what tail pages to merge
        if page_range.tps_range != 0:
            target_page_num = page_range.get_page_number(page_range.tps_range)
            # print(f'Tail page to stop iterating: {page_range.get_page_number(page_range.tps_range)}')

        # iterate backwards through tail pages list
        for i in range(len(page_range.tail_pages) - 1, -1, -1):
            # Access the Tail_Page object using the current index
            tail_page = page_range.tail_pages[i]

            # Check if the current page number matches the target page number
            if tail_page.page_number >= target_page_num:
                # print(f"MergingTail Page: {tail_page.page_number} ")
                # print(f"Found tail page {tail_page.page_number}")

                # base_rid and tid page in tail page
                base_rid_page = tail_page.get_base_rid_page()
                tid_page = tail_page.physical_pages[1]

                # jumps straight to the end of the tail page (TODO: make it jump to very last tail record even if tail page isn't full)
                for i in range(
                    Config.PHYSICAL_PAGE_SIZE - Config.COLUMN_SIZE,
                    -1,
                    -Config.COLUMN_SIZE,
                ):

                    tid_for_tail_record_bytes = tid_page.data[
                        i : i + Config.COLUMN_SIZE
                    ]

                    tid_for_tail_record_value = -(
                        int.from_bytes(
                            tid_for_tail_record_bytes, byteorder="big", signed=True
                        )
                    )

                    # continue through loop meaning it's at a tail record that hasn't been set
                    if tid_for_tail_record_value == 0:
                        continue

                    # Extract 8 bytes at a time using slicing
                    base_rid_for_tail_record_bytes = base_rid_page.data[
                        i : i + Config.COLUMN_SIZE
                    ]

                    base_rid_for_tail_record_value = int.from_bytes(
                        base_rid_for_tail_record_bytes, byteorder="big", signed=True
                    )

                    if -tid_for_tail_record_value < page_range.tps_range:
                        # print(f"TPS for range : {page_range.tps_range} and TID: {-tid_for_tail_record_value}")

                        # adds rid if rid is not in update_records dictionary - used to know what base pages to use
                        if (
                            base_rid_for_tail_record_value
                            not in updated_records.values()
                        ):
                            updated_records[-(tid_for_tail_record_value)] = (
                                base_rid_for_tail_record_value
                            )

            # rids to base_pages
            rid_to_base = {}

            # base_page numbers
            base_pages_to_get = []

            for value in updated_records.values():
                base_page_num = page_range.get_page_number(value)
                rid_to_base[value] = base_page_num

                if base_page_num not in base_pages_to_get:
                    base_pages_to_get.append(base_page_num)

        # sorts list
        base_pages_to_get.sort()
        # print(base_pages_to_get)

        i = 0
        # iterate through base pages in page range to find the base pages we are merging
        for base_page in page_range.base_pages:
            if (
                i < len(base_pages_to_get)
                and base_page.page_number == base_pages_to_get[i]
                and base_page.num_records == Config.RECORDS_PER_PAGE
            ):
                # print(f"Base page we're in : {base_page.page_number}")
                # print(f"rid_to_base: {rid_to_base}")

                # keeps track of what columns were updated in this base page
                updated_columns = []

                # iterate through rids that are updated and their corresponding base page
                for key, value in rid_to_base.items():
                    if value == base_page.page_number:
                        # print(f"value: {value}")
                        schema_encoding_for_rid = (
                            base_page.check_base_record_schema_encoding(key)
                        )

                        # let's us know what columns have been updated
                        update_cols_for_rid = page_range.analyze_schema_encoding(
                            schema_encoding_for_rid
                        )

                        # grabs updated values
                        for column in update_cols_for_rid:
                            # updated values of specific columns
                            updated_val = page_range.return_column_value(key, column)

                            # print(f'schema encoding: {base_page.check_base_record_schema_encoding(key)} : {update_cols_for_rid} -> [{column} : {updated_val}]  -> RID : {key} -> Page_Num {base_page.page_number}')

                            # updates column for record
                            base_page.physical_pages[
                                Config.META_DATA_NUM_COLUMNS + column
                            ].write_to_physical_page(
                                value=updated_val, rid=key, update=True
                            )

                            if column not in updated_columns:
                                updated_columns.append(column)

                            # print(f"Updated grade column ({column}) in base page {base_page.page_number} at RID: {key}\n")

                        # merge_updated_list.append(key)

                # iterates to see what columns to read to disk if they've been modified
                # for column in updated_columns:
                # __merge_update_to_buffer(base_page.physical_pages[Config.META_DATA_NUM_COLUMNS + column], base_page.page_number)
                # print("Physical pages saved")

                i += 1

            # last record merged
            page_range.tps_range = next(iter(updated_records))

    # TODO: takes buffer pool that it's going to use, since merge has it's own buffer pool so it won't interfere with the main thread
    @staticmethod
    def __merge_update_to_buffer(physical_page: PhysicalPage, base_page_number: int):
        # print(f'Physical Page : {physical_page.column_number} for Base Page ({base_page_number})')

        pass

    # checks if merging needs to happen
    # def _merge_checker(self, page_range_num):
    #     if (
    #         self.page_range_directory[page_range_num].num_updates
    #         % Config.MERGE_THRESHOLD
    #         == 0
    #     ):
    #         # creates deep copy of page range
    #         page_range_copy = copy.deepcopy(self.page_range_directory[page_range_num])
    #         merging_thread = threading.Thread(target=self.__merge())
    #         merging_thread.start()

    def close(self):
        """
        Writes all of the frames in the bufferpool to the disk.
        """
        # TODO
        pass
