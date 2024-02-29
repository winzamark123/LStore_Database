import os 
from lstore.disk import DISK
from lstore.bufferpool import BUFFERPOOL
from lstore.config import *
from lstore.record import Record, RID
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

    def __init__(self, num_columns:int, key_index:int, is_tail_page: bool = False)->None:

        if is_tail_page:
            # Increment the tail_page_counter each time a Tail_Page object is created
            self.page_number = 0
        else:
            # Increment the base_page_counter each time a Base_Page object is created
            Page.base_page_counter += 1
            self.page_number = Page.base_page_counter
        
        self.physical_pages:list     = [] 
        self.key_index:int           = key_index
        self.num_columns:int         = num_columns

        for i in range(self.num_columns):
            self.physical_pages.append(Physical_Page(column_number=i))
        
        self.num_records:int        = 0
        self.last_record:int        = 0

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
    def insert_new_record(self, new_record: Record,indirection_value:int = 0, Base_RID:int = 0 ,schema_encoding:int = 0, update: bool=False)->bool:

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

            # grabs Base Rid page
            base_rid_page = self.get_base_rid_page()

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

                base_rid_page.write_to_physical_page(value=rid, rid=rid )

            # if an (update = True) record then we write to the indirection physical_page which is in the tail page else base record stays as NULL since it's not pointing to any new updates (pages (byte array) are set to null automatically),
            # so no else statement is needed
            if update:

                # writes updated schema encoding 
                schema_encoding_page.write_to_physical_page(schema_encoding,rid)

                # write to Indirection page - if passed in that this record is an update record, then it means that it's going to the tail page, 
                # so we need the indirection value to be passed in as well
                indirection_page.write_to_physical_page(indirection_value, rid)

                base_rid_page.write_to_physical_page(value=Base_RID, rid=rid )

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
            if(physical_Page.column_number == self.key_index ):
                return physical_Page

    # returns physical_page(column) that has the RIDs
    def get_rid_page(self)->Physical_Page:
        for physical_Page in self.physical_pages:
            if(physical_Page.column_number == RID_COLUMN):
                return physical_Page

    # returns physical_page(column) that has the BASE_RIDs
    def get_base_rid_page(self)->Physical_Page:
        for physical_Page in self.physical_pages:
            if(physical_Page.column_number == BASE_RID_COLUMN):
                return physical_Page

    # returns physical_page(column) that has the Indirections
    def _get_indirection_page(self)->Physical_Page:
        for physical_Page in self.physical_pages:
            if(physical_Page.column_number == INDIRECTION_COLUMN):
                return physical_Page

    # returns physical_page(column) that has the Schema Encodings
    def _get_schema_encoding_page(self)->Physical_Page:
        for physical_Page in self.physical_pages:
            if(physical_Page.column_number == SCHEMA_ENCODING_COLUMN):
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

class Base_Page:

    def __init__(self, base_page_dir_path:str, base_page_index:int)->None:
        self.base_page_index = base_page_index
        self.path_to_page = base_page_dir_path

        self.meta_data = self.__read_metadata()

        self.num_columns = self.meta_data["num_columns"] + META_DATA_NUM_COLUMNS 
        self.key_index = self.meta_data["key_index"]
        
    def __read_metadata(self)->dict:
        table_path = os.path.dirname(os.path.dirname(self.path_to_page))
        print(table_path)
        return DISK.read_metadata_from_disk(table_path)

    def insert_record(self, record:Record)->None:
        print("INSERT PAGE")
        # self.insert_new_record(record)
        record_info = {
            "page_range_num": record.get_page_range_index(),
            "page_type": "base",
            "page_num": record.get_base_page_index()
        } 

        #META = RID, IC, SCHEMA, BASE_RID

        frame_index = BUFFERPOOL.import_frame(path_to_page=self.path_to_page, num_columns=self.num_columns, record_info=record_info)
        BUFFERPOOL.insert_record(key_index=self.key_index, frame_index=frame_index, record=record)

        



    def get_record(self, rid:RID)->Record:
        pass

    def update_record(self, rid:RID, new_record:Record)->None:
        pass

    def delete_record(self, rid:RID)->None:
        pass

class Tail_Page:

    def __init__(self, tail_page_dir_path:str, tail_page_index:int)->None:
        self.tail_page_index = tail_page_index
        self.path_to_page = tail_page_dir_path

        self.meta_data = self.__read_metadata()

        self.num_columns = self.meta_data["num_columns"] + META_DATA_NUM_COLUMNS 
        self.key_index = self.meta_data["key_index"]

    def __read_metadata(self)->dict:
        table_path = os.path.dirname(os.path.dirname(self.path_to_page))
        return DISK.read_metadata_from_disk(table_path)

    def insert_record(self, record:Record)->None:
        self.insert_new_record(record)

        record_info = {
            "page_range_num": record.get_page_range_index(),
            "page_type": "tail",
            "page_num": record.get_base_page_index()
        } 

        #META = RID, IC, SCHEMA, BASE_RID

        frame_index = BUFFERPOOL.import_frame(path_to_page=self.path_to_page, num_columns=self.num_columns, record_info=record_info)
        BUFFERPOOL.insert_record(key_index=self.key_index, frame_index=frame_index, record=record)

    def get_record(self, rid:RID)->Record:
        pass

    def update_record(self, rid:RID, new_record:Record)->None:
        pass

    def delete_record(self, rid:RID)->None:
        pass