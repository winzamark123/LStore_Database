import time
from lstore.config import *
from lstore.record import Record
from lstore.physical_page import Physical_Page

class Page:
    """
    :param num_columns: int         # amount of columns in table (5 columns for our example)
    :param entry_sizes: list        # list of the entry sizes ([2 bytes, 8 bytes, ...]) for each physical page
    :param key_column: int          # column (physical page) the key value is going to be at 
    :param is_tail_page: bool       # to know if page is tail_page or not(if not then it's base page, automatically set to false)

    """

    # Class variables to keep track of the number of Base_Page and Tail_Page objects created
    base_page_counter = 0
    tail_page_counter = 0

    def __init__(self, num_columns:int, entry_sizes:list, key_column:int, is_tail_page: bool = False)->None:

        if is_tail_page:
            # Increment the tail_page_counter each time a Tail_Page object is created
            Page.tail_page_counter += 1
            self.page_number = Page.tail_page_counter
        else:
            # Increment the base_page_counter each time a Base_Page object is created
            Page.base_page_counter += 1
            self.page_number = Page.base_page_counter
        
        # list of physical_pages in base page
        self.physical_pages = [] 
    
        # column (physical page) the key value is going to be at 
        self.primary_key_column = key_column

        # adds to list of physical_pages depending the amount of columns
        for column_number ,entry_size in enumerate(entry_sizes, start=0): 
            column_page = Physical_Page(entry_size=entry_size, column_number=column_number)
            self.physical_pages.append(column_page)

        self.num_records = 0

    # updates indirection column with new LID
    def update_indirection_base_column(self, new_value_LID:int, rid:int,)->bool:

            # indirection physical_page(column) of base page 
            indirection_page = self._get_indirection_page() 

            # update indirection of base record with LID
            indirection_page.write_to_physical_page(new_value_LID, rid, update=True)

            return True

    # updates schema encoding column with updated schema encoding
    def update_schema_encoding_base_column(self, new_value_encoding:int, rid:int)->bool:

            # schema encoding physical_page(column) of base page 
            schema_encoding_page = self._get_schema_encoding_page() 

            # update schema encoding of base record with LID
            schema_encoding_page.write_to_physical_page(new_value_encoding, rid, update=True)

            return True

    # inserts new record to Base Page or Tail Page
    def insert_new_record(self, new_record: Record, indirection_value:int = 0, schema_encoding:int = 0, update: bool=False)->bool:

        if not self.has_capacity():
        # checks if page is full of records
            return False
        
        # rid of record 
        rid = new_record.rid
        
        # grabs rid page
        rid_page = self.get_rid_page()

        # checks if RID exists)
        if(rid_page.check_value_in_page(rid,rid) == False): 

            # grabs schema encoding page
            schema_encoding_page = self._get_schema_encoding_page()

            # grabs Indirection page
            indirection_page = self._get_indirection_page()

            # if not update record then we pass the key, if not then we don't need key in tail records 
            if not update:
                # new base record indirection is set to equal it's self
                indirection_page.write_to_physical_page(rid, rid)

                # key of record (Student ID) being inserted
                key = new_record.key

                # grabs key page
                key_page = self.get_primary_key_page()

                # write to key page 
                key_page.write_to_physical_page(key,rid)

                # writes updated schema encoding 
                schema_encoding_page.write_to_physical_page(0,rid)

            # if an (update = True) record then we write to the indirection physical_page which is in the tail page else base record stays as NULL since it's not pointing to any new updates (pages (byte array) are set to null automatically),
            # so no else statement is needed
            if update:

                # writes updated schema encoding 
                schema_encoding_page.write_to_physical_page(schema_encoding,rid)

                # write to Indirection page - if passed in that this record is an update record, then it means that it's going to the tail page, 
                # so we need the indirection value to be passed in as well
                indirection_page.write_to_physical_page(indirection_value, rid)

            # write to RID page
            rid_page.write_to_physical_page(rid,rid)

            # columns of record
            columns = new_record.columns
        
            for column_num, column_data in enumerate(columns, start = 1):
                # gets page which corresponds to each column
                page_to_write = self.get_page(column_number=column_num) 
                page_to_write.write_to_physical_page(value=column_data, rid=rid)
            self.num_records += 1

            
            
            
            return True
        else:
            raise KeyError(f"RID ({rid}) already exists.")

    # returns physical_page(column) that has the primary keys 
    def get_primary_key_page(self)->Physical_Page:
        for physical_Page in self.physical_pages:
            if(physical_Page.column_number == self.primary_key_column ):
                return physical_Page

    # returns physical_page(column) that has the RIDs
    def get_rid_page(self)->Physical_Page:
        for physical_Page in self.physical_pages:
            if(physical_Page.column_number == RID_COLUMN):
                return physical_Page

    # returns physical_page(column) that has the Indirections
    def _get_indirection_page(self)->Physical_Page:
        for physical_Page in self.physical_pages:
            if(physical_Page.column_number == INDIRECTION_COLUMN):
                return physical_Page

    # returns physical_page(column) that has the Schema Encodings
    def _get_schema_encoding_page(self)->Physical_Page:
        for physical_Page in self.physical_pages:
            if(physical_Page.column_number == SCHEMA_ENCODING):
                return physical_Page

    # returns page needed
    def get_page(self, column_number:int)->Physical_Page:
        for physical_Page in self.physical_pages:
            if(physical_Page.column_number == (META_DATA_NUM_COLUMNS + column_number)):
                return physical_Page

    # returns value of column you want 
    def get_value_at_column(self,rid:int, column_to_index:int)->int:
        column_page = self.get_page(column_to_index)

        return column_page.value_exists_at_bytes(rid)

    def has_capacity(self)->bool:
    # checks if page is full
        if self.num_records >= RECORDS_PER_PAGE:
            #print(f'Page is full of records on base_page ({self.page_number})')
            return False
        
        return True

        

class Tail_Page(Page):

    def __init__(self, num_columns:int, entry_sizes:list, key_column:int)->None:
        # Call the constructor of the parent class (Page)
        super().__init__(num_columns, entry_sizes, key_column, is_tail_page=True)

        # returns value of indirection in for base records
    
    def check_tail_record_indirection(self, rid:int)->int:
            
        # indirection column of base page
        indirection_page = self._get_indirection_page()

        indirection_value_base_record = indirection_page.value_exists_at_bytes(rid)

        return indirection_value_base_record

    # returns value of schema encoding in base record
    def check_tail_record_schema_encoding(self, rid:int)->int:
    
        # indirection column of base page
        schema_encoding_page = self._get_schema_encoding_page()

        schema_encoding_value_base_record = schema_encoding_page.value_exists_at_bytes(rid)

        return schema_encoding_value_base_record
        

class Base_Page(Page):

    def __init__(self, num_columns:int, entry_sizes:list, key_column:int)->None:
        # Call the constructor of the parent class (Page)
        super().__init__(num_columns, entry_sizes, key_column, is_tail_page=False)

    # returns value of indirection in for base records
    def check_base_record_indirection(self, rid:int)->int:
            
        # indirection column of base page
        indirection_page = self._get_indirection_page()

        indirection_value_base_record = indirection_page.value_exists_at_bytes(rid)

        return indirection_value_base_record

    # returns value of schema encoding in base record
    def check_base_record_schema_encoding(self, rid:int)->int:
    
        # indirection column of base page
        schema_encoding_page = self._get_schema_encoding_page()

        schema_encoding_value_base_record = schema_encoding_page.value_exists_at_bytes(rid)

        return schema_encoding_value_base_record