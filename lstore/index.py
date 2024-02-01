from lstore.table import Table

"""
A data structure holding indices for various columns of a table. 
Key column should be indexed by default, other columns can be indexed 
through this object. Indices are usually B-Trees, but other data 
structures can be used as well.

According to Google's AI:
    A database index is a data structure that maps column values to their
    physical location in a database.

We should try implementing a B+ Tree: https://www.geeksforgeeks.org/insertion-in-a-b-tree/?ref=lbp
    - another link: https://www.programiz.com/dsa/b-plus-tree
Time Complexities (found from wiki: https://en.wikipedia.org/wiki/B%2B_tree#Characteristics)
    - search time: O(log_b(n))
    - insert time: O(log_b(n))
    - removing a located record time: O(log_b(n))
    - range query w/ k elements time: O(log_b(n) + k) -> O(n)
    
Each node's key contains:
    - key: column value
    - value: base RID of specific column value (index of record in physical space)
Thus nodes will be created with record_info w/ indices:
    - 0: primary key
    - 1: base RID
(maybe make it a dictionary/hash-table instead w/ list of RIDs?)

Q: Each key will store a base RID, but what happens when we update the column?
A: When we update the column, the record 'key' associated to the RID will be
   removed and re-inserted with the new value as 'key' and the same RID as
   'value' (assuming 'key' and 'value' are utilized in dictionary implementation)
    - "when a new version of a record is created (i.e., a new tail record),
       first, all indexes defined on unaffected columns do not have to
       be modified and, second, only the affected indexes are modified
       with the updated values, but they continue to point to base records
       and not the newly created tail records." pg. 543
"""

class Column_Index_Node:
    
    def __init__(self, min_degree:int, is_leaf:bool):
        self.keys = [None] * (2 * min_degree - 1)
        self.child_pointers = [None] * (2 * min_degree)
        self.num_keys = 0
        self.is_leaf = is_leaf
    
    def insert_at_leaf(self):
                pass

class Column_Index_Tree:

     def __init__(self):
          pass


class Index:

    def __init__(self, table:Table):
        # One index for each table. All are empty initially.
        self.indices = [Column_Index_Tree()] *  table.num_columns

    def locate(self, column, value):
        """
        # returns the location of all records with the given value on column "column"
        """
        pass


    def locate_range(self, begin, end, column):
        """
        # Returns the RIDs of all records with values in column 
        "column" between "begin" and "end"
        """
        pass


    def create_index(self, column):
        """
        # optional: Create index on specific column
        """
        pass


    def drop_index(self, column):
        """
        # optional: Drop index of specific column
        """
        pass
