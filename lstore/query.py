from lstore.table import Table 
from lstore.record import Record
from lstore.index import Index

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table:Table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key)->bool:
        
        pass
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns:tuple)->bool:
        latest_page_range = self.table.page_directory[-1]

        #Check if the latest page_range has capacity
        if  latest_page_range.has_capacity() == False:
            print("INSERT: PAGE_RANGE IS FULL")
            self.table.insert_page_range()
            latest_page_range = self.table.page_directory[-1]

        latest_base_page = latest_page_range.base_pages[-1]

        #Check if the latest base_page has capacity
        if latest_page_range.base_pages[-1].has_capacity() == False:
            print("INSERT: BASE_PAGE IS FULL")
            self.table.page_directory[-1].insert_base_page()
            latest_base_page = latest_page_range.base_pages[-1]

        #Create a new record and insert it into the latest base_page
        new_record = Record(self.table.inc_rid(), columns[0], columns[1:])
        insertSuccess = latest_base_page.insert_new_record(new_record)
        
        self.table.index.insert_record_to_index(columns, new_record.rid)

        #Checking 
        print("TOTAL_PAGE_RANGE", len(self.table.page_directory))
        for i in range(len(self.table.page_directory)):
            for j in range(len(self.table.page_directory[i].base_pages)):
                print("PAGE_RANGE", i, "BASE_PAGES", j, "TOTAL_RECORDS", self.table.page_directory[i].base_pages[j].num_records) 
                print("Size of Page Range", len(self.table.page_directory[i].base_pages))

        return True if insertSuccess else False
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key: int, search_key_index: int, projected_columns_index: list) -> Record:
        #search key: SID 
        #search_key_index: 0 (the column that the SID resides in)
        #projected_columns_index: what columns you want to show 
                                    #[1,1,1,1,1] (all columns)

        #Convert SID to RID with Indexing
        rids = [] #list of rids
        rids = self.table.index.locate(search_key, search_key_index) #list of rids (page_range_num, base_page_num, record_num

        #GET Page_RANGE and Base_page from get_address in table.py
        addresses = []
        addresses = self.table.get_list_of_addresses(rids) #list of addresses (page_range_num, base_page_num)

        #GET RECORDS
        records = [] # list of records

        for rid, address in zip(rids, addresses): #zip the rids and addresses together iterating through both
            print("RID", rid)
            print("ADDRESS", address)

            page_range_num = address[0]  # get the first element of the tuple
            cur_page_range = self.table.page_directory[page_range_num]
            
            print("PAGE_RANGE_NUM", page_range_num)
            
            record_data = cur_page_range.return_record(rid)

            record_data_key = record_data.get_key() #get the key of the record
            record_data_values = record_data.get_values() #get the values of the record

            record = Record(rid, record_data_key, record_data_values) #create a new record object
            records.append(record)
        return records



    


    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):

        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        #insert but for the TailPage
        #increment in PageRange 
        pass

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        pass

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
