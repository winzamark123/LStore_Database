from lstore.index import Index
from lstore.config import *
from time import time
from lstore.page_range import Page_Range
import threading
import copy

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns
    :param key: int             #Index of table key in columns
    """
    
    def __init__(self, name:str, num_columns:int, key_index:int)->None:
        self.name = name
        self.num_columns = num_columns
        self.key_column = META_DATA_NUM_COLUMNS + key_index
        self.deleted_rids = []
        self.index = Index(num_columns, ORDER_CHOICE)
        self.key_column_index = key_index

        # rid for base records - increase by 1 only when a record is added (for base records)
        self.rid = 0

        # tid (rid) for tail records - decrease by 1 once a record is added or updated (for tails records)
        self.tid = 0
    
        self.entry_size_for_columns = [2]

        for _ in range(META_DATA_NUM_COLUMNS - 1 + num_columns):
            self.entry_size_for_columns.append(COLUMN_SIZE)


        #CREATE THE PAGE DIRECTORY with SIZE BASED ON THE num_columns 
        self.page_directory = [Page_Range(num_columns, self.entry_size_for_columns, self.key_column)]

    # Get Page Range and Base Page from RID
    def get_list_of_addresses(self, rids)-> list:
        addreses = []

        for rid in rids:
            rid -= 1
            page_range_num = rid // (RECORDS_PER_PAGE * NUM_BASE_PAGES)
            base_page_num = (rid // RECORDS_PER_PAGE) % NUM_BASE_PAGES
            addreses.append((page_range_num, base_page_num))
        #return page_range_num, base_page_num
        return addreses

    # Increment RID for base records
    def inc_rid(self)-> int:
        self.rid += 1
        return self.rid # returns unique new RID for base records

    # insert new page_range into page_directory
    def insert_page_range(self)-> bool:
        if not self.page_directory[-1].has_capacity():
            self.page_directory.append(Page_Range(self.num_columns, self.entry_size_for_columns, self.key_column))
            #print("Function: insert_page_range(), Total page ranges: ", len(self.page_directory))
            return True
        return False 

    # convert table to dictionary for disk storage
    def meta_table_to_disk(self) -> dict:
        table_data = {
            "name": self.name,
            "num_columns": self.num_columns,
            "key_column": self.key_column,
            "key_column_index": self.key_column_index,
            "rid": self.rid,
            "tid": self.tid,
            "entry_size_for_columns": self.entry_size_for_columns,
        }

        return table_data

    @staticmethod
    def meta_disk_to_table(data) -> 'Table':
        # Reconstruct the Table object from a dictionary
        table = Table(data['name'], data['num_columns'], data['key_column'])
        table.key_column_index = data['key_column_index'],
        table.rid = data['rid'] 
        table.tid = data['tid'], 
        table.tid = data['entry_size_for_columns'], 
        table.tid = data['page_directory']
        return table

    # TODO: complete merge (in the works: Testing in merge_test.py)
    # Implementing without bufferpool right now, will when bufferpool is finished
    @staticmethod
    def __merge(page_range:Page_Range):
        
        # records that need updating in base page
        updated_records = {}

        # iterate backwards through tail pages list
        for tail_page in reversed(page_range.tail_pages):

            # base_rid and tid page in tail page
            base_rid_page = tail_page.get_base_rid_page()
            tid_page = tail_page.physical_pages[1]
            
            # jumps straight to the last tail record (TODO: make it jump to very last tail record even if tail page isn't full)
            for i in range(PHYSICAL_PAGE_SIZE - COLUMN_SIZE, -1, -COLUMN_SIZE):
                # Extract 8 bytes at a time using slicing
                base_rid_for_tail_record_bytes = base_rid_page.data[i:i+COLUMN_SIZE]

                base_rid_for_tail_record_value = int.from_bytes(base_rid_for_tail_record_bytes, byteorder='big', signed=True)

                # continue through loop meaning it's at a tail record that hasn't been set
                if base_rid_for_tail_record_value == 0:
                    continue

                tid_for_tail_record_bytes = tid_page.data[i:i+COLUMN_SIZE]

                tid_for_tail_record_value = -(int.from_bytes(tid_for_tail_record_bytes, byteorder='big', signed=True))

                # adds rid if rid is not in update_records dictionary - used to know what base pages to use
                if base_rid_for_tail_record_value not in updated_records.values():
                    updated_records[-(tid_for_tail_record_value)] = base_rid_for_tail_record_value 


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

        i = 0
        # iterate through base pages in page range to find the base pages we are merging
        for base_page in page_range.base_pages:
            if i < len(base_pages_to_get) and base_page.page_number == base_pages_to_get[i]:

                # iterate through rids that are updated and their corresponding base page
                for key, value in rid_to_base.items():
                    if value == base_page.page_number:
                        schema_encoding_for_rid = base_page.check_base_record_schema_encoding(key)

                        # let's us know what columns have been updated
                        update_cols_for_rid = page_range.analyze_schema_encoding(schema_encoding_for_rid)

                        for column in update_cols_for_rid:

                            # updated values of specific columns
                            updated_val = page_range.return_column_value(key,column)

                            print(f'schema encoding: {base_page.check_base_record_schema_encoding(key)} : {update_cols_for_rid} -> [{column} : {updated_val}]  -> RID : {key} -> Page_Num {base_page.page_number}')
                
                
                i += 1
    
    # checks if merging needs to happen
    def __merge_checker(self, page_range_num):
        if self.page_directory[page_range_num].num_updates % MERGE_THRESHOLD == 0:
            # creates deep copy of page range
            page_range_copy = copy.deepcopy(self.page_directory[page_range_num])
            merging_thread = threading.Thread(target=self.__merge())
            merging_thread.start()