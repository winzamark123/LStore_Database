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
        rids = []
        rids = self.table.index.locate(primary_key, self.table.key_column_index)

        addresses = []
        addresses = self.table.get_list_of_addresses(rids)

        for rid, address in zip(rids, addresses):
            page_range_num = address[0]
            cur_page_range = self.table.page_directory[page_range_num]
            cur_page_range.delete_record(rid)

        return True
        
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns:tuple)->bool:
        latest_page_range = self.table.page_directory[-1]

        #Check if the latest page_range has capacity
        if  latest_page_range.has_capacity() == False:
            #print("INSERT: PAGE_RANGE IS FULL")
            self.table.insert_page_range()
            latest_page_range = self.table.page_directory[-1]

        latest_base_page = latest_page_range.base_pages[-1]

        #Check if the latest base_page has capacity
        if latest_page_range.base_pages[-1].has_capacity() == False:
            #print("INSERT: BASE_PAGE IS FULL")
            self.table.page_directory[-1].insert_base_page()
            latest_base_page = latest_page_range.base_pages[-1]

        #Create a new record and insert it into the latest base_page
        new_record = Record(self.table.inc_rid(), columns[0], columns[1:])
        insertSuccess = latest_base_page.insert_new_record(new_record)
        
        self.table.index.insert_record_to_index(columns, new_record.rid)
        
        
        #get record info from table
        record_info = self.table.get_record_info(new_record.rid)
        #get the bufferpool from the table
        table_buffer = self.table.bufferpool

        #db
            #table
                #page_range
                    #base
                        #base_page

        if table_buffer.is_record_in_buffer(record_info) == False:
            path_to_table = table_buffer.path_to_table
            path_to_pageRange = path_to_table + '/page_range' + str(record_info["page_range_num"])

            if record_info["isTail"]:
                path_to_type = path_to_pageRange + '/tail'
            else:
                path_to_type = path_to_pageRange + '/base'

            path_to_basePage = path_to_type + '/base_page' + str(record_info["base_page_num"])
            table_buffer.load_frame(path_to_page=path_to_basePage, table_name=self.table.table_name, num_columns=self.table.num_columns)
        

            




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
            # print("RID", rid)
            # print("ADDRESS", address)

            page_range_num = address[0]  # get the first element of the tuple
            cur_page_range = self.table.page_directory[page_range_num]
            
            # print("PAGE_RANGE_NUM", page_range_num)
            
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
        rids = self.table.index.locate(primary_key, self.table.key_column_index)
        addresses = self.table.get_list_of_addresses(rids) 

        columns_as_list = list(columns)

        for rid, address in zip(rids, addresses):
            page_range_num = address[0]
            cur_page_range = self.table.page_directory[page_range_num]
            cur_page_range.update(rid, columns_as_list)

        return True

     

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        #FOR MILESTONE 2 IMPLEMENTATION: think about accessing the base page column itself and iterate through that directly, need to be able to handle 
        #ranges that span multiple page ranges

        rids = self.table.index.locate_range(start_range,end_range, 0)
        addresses = self.table.get_list_of_addresses(rids)
        sum = 0


        #
        if rids:
            for i in range(len(addresses)):
                sum += self.table.page_directory[addresses[i][0]].return_column_value(rids[i], aggregate_column_index)
        
            return sum
            
        else:
            return False

        
    
    def sum_old(self, start_range, end_range, aggregate_column_index):
        # TO DO: change this and try to use locate_range, double check on array pointers 
        
        sum = 0
        array = []
        checker = False

        for i in range(start_range,end_range+1):
            rid = self.table.index.locate(i, self.table.key_column_index)
            address = self.table.get_list_of_addresses(rid)
            
            #checks if rid exists
            if rid:
                checker = True
                rid = rid[0]
            else:
                continue
            
            #if autograder indexes with 0 they want SIDs, address to .key
            if aggregate_column_index == 0:
                sum += self.table.page_directory[address[0][0]].return_record(rid).key
                continue

            #sum is index -1 because can't index 0 column correctly
            sum += self.table.page_directory[address[0][0]].return_record(rid).columns[aggregate_column_index-1]

        #checks if rids exist within the range, if not returns false
        if checker:
            return sum
        else:
            return checker
        


    
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
