from lstore.index import Index
from lstore.config import *
from time import time
from lstore.page_range import Page_Range
from lstore.bufferpool import Bufferpool
from lstore.disk import Disk

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
        self.rid = 0
        self.tid = 0

        self.entry_size_for_columns = [2,8,8]
        for i in range(num_columns): 
            self.entry_size_for_columns.append(COLUMN_SIZE)

        self.page_directory = [Page_Range(num_columns, self.entry_size_for_columns, self.key_column)]

        self.bufferpool = Bufferpool(self.name)
        self.disk = Disk(self.name, self.num_columns)


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

    def get_record_info(self, rid)-> dict:
        page_range_num = rid // (RECORDS_PER_PAGE * NUM_BASE_PAGES)
        base_page_num = (rid // RECORDS_PER_PAGE) % NUM_BASE_PAGES
        record_num = rid % RECORDS_PER_PAGE

        record_info = {
            "page_range_num": page_range_num,
            "base_page_num": base_page_num,
            "record_num": record_num
        }

        return record_info

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

    def convert_table_meta_to_dict(self)-> dict:
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