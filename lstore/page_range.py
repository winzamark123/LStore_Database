import lstore.config as Config
from lstore.disk import DISK
from lstore.page import Base_Page, Tail_Page, Page
from lstore.record import Record, RID
import os

class Page_Range:
    def __init__(self, page_range_dir_path:str, page_range_index:int, tps_index)->None:
        self.page_range_dir_path = page_range_dir_path
        self.page_range_index = page_range_index
        self.tps_index= tps_index

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
                    metadata["base_page_dir_path"],
                    metadata["base_page_index"],
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

        if len(self.base_pages) == 0 or not record.get_base_page_index() in self.base_pages:
            self.create_base_page(self.__get_num_base_pages())
        self.base_pages[record.get_base_page_index()].insert_record(record)

    def get_record(self, rid:RID)->Record:
        """
        Get record from page range.
        """

        if rid.get_base_page_index() not in self.base_pages:
            raise ValueError

        return self.base_pages[rid.get_base_page_index()].get_record(rid)

    def update_record(self, rid:RID, updated_record:Record)->None:
        """
        Update record from page range.
        """

        if rid.get_base_page_index() not in self.base_pages:
            raise ValueError
        
        self.base_pages[rid.get_base_page_index()].update_record(rid, updated_record)

    def delete_record(self, rid:RID)->None:
        """
        Delete record from page range.
        """
        
        if rid.get_base_page_index() not in self.base_pages:
            raise ValueError
        
        self.base_pages[rid.get_base_page_index()].delete_record(rid)

    # # updates record
    # def update(self, rid:int, columns_of_update:list)->bool:

    #     # uses a copy of the list
    #     columns_of_update_copy = columns_of_update.copy()

    #     none_counter = 0
    #     for grade in columns_of_update_copy:
    #         if grade == None:
    #             none_counter += 1

    #     # if list passed is full of None, then no update needs to happen
    #     if none_counter == 5:
    #         return True


    #     # gets base page number the RID is in
    #     base_page_number = self.get_page_number(rid)

    #     # base page that has RID
    #     base_page_to_work = self.__search_list(self.base_page_object, base_page_number, 1)

    #     # retrieves indirection value for rid
    #     indirection_base_value = base_page_to_work.check_base_record_indirection(rid)

    #     # retrieves schema encoding value for rid
    #     schema_encoding_base_value = base_page_to_work.check_base_record_schema_encoding(rid)

    #     # new schema encoding based on updates wanted (going into tail record and updating base indirection)
    #     new_schema_encoding = schema_encoding_base_value | self.__get_schema_encoding(columns_of_update_copy)

    #     # list of the the updated columns in tail record using the schema encoding
    #     list_of_columns_updated_2 = self.analyze_schema_encoding(new_schema_encoding)

    #     # update TID
    #     self.inc_tid()

    #     # checks if tail page has enough capacity
    #     if not self.tail_page_object[-1].has_capacity():
    #             #print("Inserted a Tail Page")
    #             self.insert_tail_pages()

    #     # checks if the base record indirection is pointing to itself
    #     if indirection_base_value == rid:
    #         # return original record
    #         base_page_record = self.return_record(rid)

    #         first_copy_tid = self.tid

    #         base_page_record.rid = first_copy_tid

    #         # make copy of base record for first update to record to keep hold of original base record after merging
    #         print('\ncreating copy of base page record first')
    #         self.tail_page_object[-1].insert_new_record(base_page_record,indirection_value=rid, Base_RID=rid, schema_encoding=schema_encoding_base_value, update=True)

    #         self.inc_tid()
    #         print('\nappending the update now to copy of base page in tail page')
    #         self.__insert_tail_record(self.tid, schema_encoding=new_schema_encoding, Base_rid=rid, indirection=first_copy_tid, columns=columns_of_update_copy)
    #     # else it's pointing to a tail record
    #     else:
    #         print('\nappending to a tail page regular')
    #         # gets tail page number the TID is in
    #         tail_page_number = self.get_page_number(indirection_base_value)

    #         # tail page TID is in
    #         tail_page_to_work = self.__search_list(self.tail_page_object, tail_page_number, 0)

    #         # retrieves schema encoding value for tid
    #         schema_encoding_tail_value = tail_page_to_work.check_tail_record_schema_encoding(indirection_base_value)

    #         # list of the the updated columns in tail record using the schema encoding
    #         list_of_columns_updated = self.analyze_schema_encoding(schema_encoding_tail_value)

    #         update_list = [None] * 5 # Initialize update_list with five None values

    #         # gets values from previous tail record
    #         for num in range(1, 5):
    #             if num in list_of_columns_updated:
    #                 physical_page_of_column = tail_page_to_work.get_page(num)
    #                 value_at_tail_record_column = physical_page_of_column.value_exists_at_bytes(indirection_base_value)
    #                 update_list[num] = value_at_tail_record_column

    #         # Check if columns_of_update_copy contains integers
    #         for i, item in enumerate(columns_of_update_copy):
    #             if isinstance(item, int):
    #                 update_list[i] = item

    #         # inserts new tail record with previous tail record data (Cumulative)
    #         self.__insert_tail_record(self.tid, new_schema_encoding, rid ,indirection_base_value, update_list)

    #     # update to the base page occurred
    #     if base_page_to_work.update_indirection_base_column(self.tid, rid) and base_page_to_work.update_schema_encoding_base_column(new_schema_encoding, rid):
    #         self.num_updates += 1
    #         return True
    #     return False

    # # TODO: Change it so it only changes the rid to 0 doesn't change the columns to 0
    # # delete record (Lazy delete)
    # def delete_record(self, rid:int)->int:

    #     # gets base page number the RID is in
    #     base_page_number = self.get_page_number(rid)

    #     # base page that has RID
    #     base_page_to_work = self.__search_list(self.base_page_object, base_page_number, 1)

    #     # rid physical page
    #     rid_page = base_page_to_work.get_rid_page()

    #     # rid that's getting deleted
    #     delete_rid = rid_page.value_exists_at_bytes(rid)

    #     # update grades to equal 0
    #     for num in range(1, 5):
    #         base_page_to_work.get_page(num).value_exists_at_bytes(rid)
    #         base_page_to_work.get_page(num).write_to_physical_page(0, rid, update=True)

    #     # updates base record indirection to point to 0 (meaning it's been deleted)
    #     base_page_to_work.update_indirection_base_column(0, rid)

    #     return delete_rid

    # # for testing
    # def return_base_record(self,rid:int)->Record:
    #     # gets base page number the RID is in
    #     base_page_number = self.get_page_number(rid)

    #     # print(rid)
    #     # base page that has RID
    #     base_page_to_work = self.__search_list(self.base_page_object, base_page_number, 1)

    #     # retrieves indirection value for rid
    #     indirection_base_value = base_page_to_work.check_base_record_indirection(rid)

    #     if indirection_base_value == 0:
    #         return

    #     # retrieves schema encoding value for rid
    #     schema_encoding_base_value = base_page_to_work.check_base_record_schema_encoding(rid)

    #     return_list = []
    #     for num in range(1, 5):
    #         physical_page_of_column = base_page_to_work.get_page(num)
    #         value_at_base_record_column = physical_page_of_column.value_exists_at_bytes(rid)
    #         return_list.append(value_at_base_record_column)

    #     values_tuple = tuple(return_list)


    #     key_page = base_page_to_work.get_primary_key_page()

    #     # Student ID of record
    #     stID = key_page.value_exists_at_bytes(rid)
    #     print(f'TPS: {self.tps_range} <= Indirection : {indirection_base_value}')
    #     print(f'In Base Page {base_page_to_work.page_number}')
    #     # returns record wanted
    #     return Record(rid, stID, values_tuple)


    # # return record object
    # def return_record(self, rid:int)->Record:
    #     # gets base page number the RID is in
    #     base_page_number = self.get_page_number(rid)

    #     # print(rid)
    #     # base page that has RID
    #     base_page_to_work = self.__search_list(self.base_page_object, base_page_number, 1)

    #     # retrieves indirection value for rid
    #     indirection_base_value = base_page_to_work.check_base_record_indirection(rid)

    #     if indirection_base_value == 0:
    #         return

    #     # retrieves schema encoding value for rid
    #     schema_encoding_base_value = base_page_to_work.check_base_record_schema_encoding(rid)
    #     if indirection_base_value != rid and self.tps_range > indirection_base_value:
    #         print(f'TPS: {self.tps_range} > Indirection : {indirection_base_value}')

    #         # gets tail page number the TID is in
    #         tail_page_number = self.get_page_number(indirection_base_value)

    #         # tail page TID is in
    #         tail_page_to_work = self.__search_list(self.tail_page_object, tail_page_number, 0)

    #         print(f'In Tail Page {tail_page_to_work.page_number}')

    #         # gets indexes of schema encoding that has 1s and 0s
    #         list_of_columns_updated_0 = self.analyze_schema_encoding(schema_encoding_base_value, return_record=True)
    #         list_of_columns_updated_1 = self.analyze_schema_encoding(schema_encoding_base_value)

    #         dict_values = {}

    #         if len(list_of_columns_updated_0) != 0:
    #             for i in list_of_columns_updated_0:
    #                 x = base_page_to_work.get_value_at_column(rid,i)
    #                 dict_values[i] = x

    #         if len(list_of_columns_updated_1) != 0:
    #             for i in list_of_columns_updated_1:
    #                 x = tail_page_to_work.get_value_at_column(indirection_base_value,i)
    #                 dict_values[i] = x


    #         # Sorting the dictionary based on keys
    #         sorted_dict = {k: dict_values[k] for k in sorted(dict_values)}

    #         # Extracting values and creating a tuple
    #         values_tuple = tuple(sorted_dict.values())

    #     elif indirection_base_value == rid or self.tps_range <= indirection_base_value:
    #         print(f'TPS: {self.tps_range} <= Indirection : {indirection_base_value}')
    #         return_list = []
    #         for num in range(1, 5):
    #             physical_page_of_column = base_page_to_work.get_page(num)
    #             value_at_tail_record_column = physical_page_of_column.value_exists_at_bytes(rid)
    #             return_list.append(value_at_tail_record_column)

    #         values_tuple = tuple(return_list)


    #     key_page = base_page_to_work.get_primary_key_page()

    #     # Student ID of record
    #     stID = key_page.value_exists_at_bytes(rid)

    #     print(f'Base Page {base_page_to_work.page_number}')
    #     # returns record wanted
    #     return Record(rid, stID, values_tuple)

    # # return record object
    # def return_column_value(self, rid:int ,column_number:int):

    #     # gets base page number the RID is in
    #     base_page_number = self.get_page_number(rid)

    #     # base page that has RID
    #     base_page_to_work = self.__search_list(self.base_page_object, base_page_number, 1)

    #     # if column wanted is 0, then we return key
    #     if column_number == 0:
    #         key_page = base_page_to_work.get_primary_key_page()
    #         return key_page.value_exists_at_bytes(rid)

    #     # retrieves schema encoding value for rid
    #     schema_encoding_base_value = base_page_to_work.check_base_record_schema_encoding(rid)

    #     # gets indexes of schema encoding that has 1s and 0s
    #     list_of_columns_updated_0 = self.analyze_schema_encoding(schema_encoding_base_value, return_record=True)
    #     list_of_columns_updated_1 = self.analyze_schema_encoding(schema_encoding_base_value)

    #     indirection_base_value = base_page_to_work.check_base_record_indirection(rid)

    #     # checks TPS is less than indirection, if so then just take from base page
    #     if column_number in list_of_columns_updated_0:
    #         print("In base record")
    #         print(column_number in list_of_columns_updated_0 or self.tps_range <= indirection_base_value)
    #         print(list_of_columns_updated_0)
    #         return base_page_to_work.get_value_at_column(rid,column_number)

    #     if column_number in list_of_columns_updated_1:
    #         print("In tail record")
    #         # retrieves indirection value for rid
    #         #print(indirection_base_value)

    #         if indirection_base_value == 0:
    #             return

    #         # gets tail page number the TID is in
    #         tail_page_number = self.get_page_number(indirection_base_value)

    #         # tail page TID is in
    #         tail_page_to_work = self.__search_list(self.tail_page_object, tail_page_number, 0)

    #         #print(tail_page_to_work.get_page(column_number).value_exists_at_bytes(indirection_base_value))

    #         return tail_page_to_work.get_value_at_column(indirection_base_value,column_number)

    #     print("Doesn't exist in this page_range")

    # # search base and tail pages list to find the page we are going to use
    # def __search_list(self, page_list:list, page_number:int, type_of_list:int)->Page:

    #     for page in page_list:
    #         if page.page_number == page_number:
    #             page_to_work = page
    #             return page_to_work
    #     else:
    #         if type_of_list == 1:
    #             print(f"RID is not in any of base pages")
    #         else:
    #             print("TID is not in any of tail pages")

    # # inserts tail record into tail page
    # def __insert_tail_record(self, tid:int, schema_encoding:int, Base_rid:int, indirection:int , columns:list):
    #     # takes first item in list out since it's just for the key column
    #     columns.pop(0)

    #     # make None types equal 0
    #     for i in range(len(columns)):
    #         if columns[i] == None:
    #             columns[i] = 0

    #     new_tuple = tuple(columns)
    #     new_tail_record = Record(tid, 0, new_tuple)
    #     self.tail_page_object[-1].insert_new_record(new_tail_record, indirection, Base_rid, schema_encoding, update=True)

    # # returns base page number that the rid is on (each base page and tail page have a page number)
    # def get_page_number(self, rid: int) -> int:
    #     page_index = (abs(rid) - 1) // RECORDS_PER_PAGE
    #     return page_index + 1

    # # No sure if this should go in table.py or in page_range.py
    # def __get_schema_encoding(self,columns:list):
    #     schema_encoding = ''
    #     for item in columns:
    #         # if value in column is not 'None' add 1
    #         if item or item == 0:
    #             schema_encoding = schema_encoding + '1'
    #         # else add 0
    #         else:
    #             schema_encoding = schema_encoding + '0'
    #     return int(schema_encoding, 2)

    # help determine what columns have been updated

    # def analyze_schema_encoding(self,schema_encoding: int, return_record:bool = False) -> list:
    #     """
    #     Analyzes a schema encoding represented as a 5-bit integer and returns a list
    #     containing the positions of bits with value 1, excluding the first bit.

    #     Args:
    #     - schema_encoding (int): The schema encoding represented as a 5-bit integer.

    #     Returns:
    #     - list: A list containing the positions of bits with value 1, excluding the first bit.
    #     """
    #     schema_encoding = abs(schema_encoding)
    #     if not isinstance(schema_encoding, int):
    #         raise TypeError("Schema encoding must be an integer.")

    #     if schema_encoding < 0 or schema_encoding >= 32:
    #         raise ValueError("Schema encoding must be a 5-bit integer (0-31).")

    #     # Initialize an empty list to store positions of bits with value 1
    #     positions = []

    #     if not return_record:
    #         # Iterate through each bit position (1 to 4) since key is never going to change
    #         for i in range(1, 5):
    #             # Check if the bit at position i is 1
    #             if schema_encoding & (1 << (4 - i)):
    #                 positions.append(i)
    #     else:
    #         # Iterate through each bit position (1 to 4) since key is never going to change
    #         for i in range(1, 5):
    #             # Check if the bit at position i is 0
    #             if not schema_encoding & (1 << (4 - i)):
    #                 positions.append(i)

    #     return positions

    # def convert_page_range_meta_to_dict(self)-> dict:
    #     page_range_data = {
    #         "num_columns": self.num_columns,
    #         "entry_size_for_columns": self.entry_size_for_columns,
    #         "key_column": self.key_column,
    #         "tid": self.tid,
    #     }
    #     return page_range_data

    # # increment the TID (RID for tails records)
    # def inc_tid(self):
    #     self.tid -= 1

    # # checks if the page range has capacity for more records
    # def has_capacity(self)-> bool:
    #     if len(self.base_page_object) >= NUM_BASE_PAGES:
    #         # checks if the last base page in the base page list is full
    #         if not self.base_page_object[-1].has_capacity():
    #             # print("FUNCTION PAGE_RANGE: has_capacity()")
    #             # print(len(self.base_page_object))
    #             return False
    #     return True
