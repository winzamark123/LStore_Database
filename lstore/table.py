from lstore.index import Index
from lstore.config import *
from time import time
from lstore.page_range import Page_Range

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

        self.entry_size_for_columns = [2,8,8]
        for i in range(num_columns): 
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

    # get table data to write to disk
    def get_table_data(self):
        table_data = {
            "name": self.name,
            "num_columns": self.num_columns,
            "key_column": self.key_column,
            "deleted_rids": self.deleted_rids,
            "index": self.index,
            "key_column_index": self.key_column_index,
            "rid": self.rid,
            "tid": self.tid,
            "entry_size_for_columns": self.entry_size_for_columns,
            "page_directory": self.page_directory
        }

        return table_data

    def convert_to_dict(self):
        # Convert the Table object to a dictionary
        table_data = {
            "name": self.name,
            "num_columns": self.num_columns,
            "key_column": self.key_column,
            "deleted_rids": self.deleted_rids,
            "index": self.index,
            "key_column_index": self.key_column_index,
            "rid": self.rid,
            "tid": self.tid,
            "entry_size_for_columns": self.entry_size_for_columns,
            "page_directory": self.page_directory
        }
        return table_data

    # @staticmethod
    def from_dict(data):
        # Reconstruct the Table object from a dictionary
        table = Table(data['name'], data['columns'])
        # Set other attributes as needed
        return table