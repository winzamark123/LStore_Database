from table import Table

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

    def __init__(self, order)->None:
        self.order = order
        self.entry_values = []
        self.child_nodes = [] # only used for non-leaf nodes
        self.rids = [] # only used for leaf nodes
        self.next_node = None
        self.is_leaf = True
    
    def insert_value_in_node(self, entry_value, rid:int):
        if not self.entry_values:
            self.entry_values.append(entry_value)
            self.rids.append([rid])
            return

        for i, item in enumerate(self.entry_values):
            if entry_value == item:
                self.rids[i].append(rid)
                break
            elif entry_value < item:
                self.entry_values = self.entry_values[:i] + [entry_value] + self.entry_values[i:]
                self.rids = self.rids[:i] + [[rid]] + self.rids[i:]
                break
            elif i + 1 == len(self.entry_values):
                self.entry_values.append(entry_value)
                self.rids.append([rid])
                break
        return
    
    def split_node(self):
        left_node = Column_Index_Node(self.order)
        right_node = Column_Index_Node(self.order)
        mid = self.order // 2

        left_node.entry_values = self.entry_values[:mid]
        right_node.entry_values = self.entry_values[mid:]
        
        left_node.next_node = right_node
        
        self.entry_values = right_node.entry_values[0]
        if self.is_leaf:
            left_node.rids = self.rids[:mid]
            right_node.rids = self.rids[mid:]
            self.child_nodes = [left_node, right_node]
            self.rids = None
            self.is_leaf = False
        else:
            self.child_nodes = self.child_nodes[:mid]
            self.child_nodes = self.child_nodes[mid:]

    def is_full(self):
        return len(self.entry_values) == self.order


class Column_Index_Tree:

    def __init__(self, order):
        self.root = Column_Index_Node(order)
        self.order = order

    def find_child_node(self, prev_node:Column_Index_Node, entry_value)->tuple[Column_Index_Node, int]:
        for i, cur_val in enumerate(prev_node.entry_values):
            if cur_val < entry_value:
                return prev_node.child_nodes[i], i
        return prev_node.child_nodes[i+1], i + 1
    
    def merge(self, parent:Column_Index_Node, child:Column_Index_Node, index:int)->None:
        parent.child_nodes.pop(index)
        pivot = child.entry_values[0]

        for i, item in enumerate(parent.entry_values):
            if pivot < item:
                parent.entry_values = parent.entry_values[:i] + [pivot] + parent.entry_values[i:]
                break
            elif i + 1 == len(parent.entry_values):
                parent.entry_values.append(pivot)
                parent.child_nodes += child.child_nodes
                break

    def insert_value(self, entry_value, rid:int)->None:
        parent = Column_Index_Node(self.order)
        child = self.root

        while not child.is_leaf:
            parent = child
            child, index = self.find_child_node(child, entry_value)

        child.insert_value_in_node(entry_value, rid)

        if child.is_full():
            child.split_node()

            if parent and not parent.is_full():
                self.merge(parent, child, index)

    def get_rids_equality_search(self, entry_value)->list[int]:
        cur_node = self.root
        while not cur_node.is_leaf:
            for i, val in enumerate(cur_node):
                if val > entry_value:
                    cur_node = cur_node.child_nodes[i]
                elif val == entry_value or i + 1 == len(cur_node.entry_values):
                    cur_node = cur_node.child_nodes[i+1]
        return cur_node.rids[cur_node.entry_values.index(entry_value)]

    def get_rids_range_search(self, lower_bound, upper_bound)->list[int]:
        if lower_bound > upper_bound: raise ValueError

        cur_node = self.root
        while not cur_node.is_leaf:
            for i, val in enumerate(cur_node):
                if val > lower_bound:
                    cur_node = cur_node.child_nodes[i]
                elif val == lower_bound or i + 1 == len(cur_node.entry_values):
                    cur_node = cur_node.child_nodes[i+1]

        return_list = list()
        while cur_node != None:
            for i, val in enumerate(cur_node.entry_values):
                if val > upper_bound: break
                if lower_bound <= val and val <= upper_bound:
                    return_list += cur_node.rids[i]
            
            if val > upper_bound: break
            cur_node = cur_node.next_node

        return return_list

    def __str__(self):
        return self.print_node(self.root)


    def print_node(self, cur_node:Column_Index_Node, count:int=0)->str:
        if not cur_node.is_leaf:
            for child_node in cur_node.child_nodes:
                self.print_node(child_node, count + 1)
        return f"[lvl {count}] -- " + str(cur_node.entry_values)

class Index:

    def __init__(self, table:Table):
        # One index for each table. All are empty initially.
        self.indices = [Column_Index_Tree()] *  table.num_columns

    def locate(self, value, column_index:int):
        """
        # returns the location of all records with the given value on column "column"
        """
        pass


    def locate_range(self, begin, end, column_index:int):
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
