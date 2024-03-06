from lstore.table import Table
from lstore.record import RID
from lstore.record import Record

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table:Table):
        self.table = table
        self.inserted_keys = {}


    def delete(self, primary_key)->bool:
        """
        # internal Method
        # Read a record with specified RID
        # Returns True upon succesful deletion
        # Return False if record doesn't exist or is locked due to 2PL
        """

        rids = self.table.index.locate(primary_key, self.table.key_column)

        try:
            for rid in rids:
                self.table.delete_record(rid)
        except ValueError:
            return False
        else:
            return True
    
        # TODO: implement TPL record locking

    def insert(self, *columns:tuple)->bool:
        """
        # Insert a record with specified columns
        # Return True upon succesful insertion
        # Returns False if insert fails for whatever reason
        """
        try:
            print("INSERT")
            self.table.insert_record(self.table.create_record(columns))
        except ValueError:
            return False
        else:
            return True

    def select(self, search_key: int, search_key_index: int, projected_columns_index:list)->list[Record]:
        """
        # Read matching record with specified search key
        # :param search_key: the value you want to search based on
        # :param search_key_index: the column index you want to search based on
        # :param projected_columns_index: what columns to return. array of 1 or 0 values.
        # Returns a list of Record objects upon success
        # Returns False if record locked by TPL
        # Assume that select will never be called on a key that doesn't exist
        """
        rids = self.table.index.locate(search_key, search_key_index)
        records_list = list()
        try:
            for rid in rids:
                rid = RID(rid=rid)
                data_columns = self.table.get_data(rid)

                filtered_list = [data_columns[i] for i in range(len(data_columns)) 
                                   if projected_columns_index[i] == 1]


                filtered_record = Record(rid=rid, key=self.table.key_index, columns=filtered_list)
                records_list.append(filtered_record)
        except ValueError:
            return False

        return records_list 

        # TODO: implement TPL record locking

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
        # TODO
        pass


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns)->bool:
        rid = self.table.index.locate(primary_key, self.table.key_index)
        if len(rid) > 1: raise ValueError
        elif len(rid) == 0:
            return False

        try:
            self.table.update_record(rid.pop(), columns)
        except ValueError: # TODO: TPL record locking
            return False
        else:
            return True

    def sum(self, start_range, end_range, aggregate_column_index)->int:
        """
        :param start_range: int         # Start of the key range to aggregate
        :param end_range: int           # End of the key range to aggregate
        :param aggregate_columns: int  # Index of desired column to aggregate
        # this function is only called on the primary key.
        # Returns the summation of the given range upon success
        # Returns False if no record exists in the given range
        """

        rids = self.table.index.locate_range(start_range,end_range, self.table.key_column)
        return len(rids) if len(rids) > 0 else False

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
        # TODO
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
        # TODO: idk anything abt this
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
