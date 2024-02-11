from lstore.config import *
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

    def __init__(self, order:int)->None:
        self.order:int                           = order
        self.entry_values:list                   = []
        self.rids:list[list[int]]                = [] # only used for leaf nodes (i:i w/ entry_values)
        self.child_nodes:list[Column_Index_Node] = [] # only used for non-leaf nodes (i:i+1 w/ entry_values)

        self.parent:Column_Index_Node            = None
        self.next_node:Column_Index_Node         = None
        self.is_leaf:bool                        = True

    def insert_value_in_node(self, entry_value, rid:int):
        assert self.is_leaf, ValueError

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

    def split_node(self)->None:
        left_node = Column_Index_Node(self.order)
        right_node = Column_Index_Node(self.order)
        mid = self.order // 2

        # assign entry values to child nodes
        mid_entry_value = self.entry_values[mid]
        left_node.entry_values = self.entry_values[:mid]
        if self.is_leaf:
            right_node.entry_values = self.entry_values[mid:]
        else:
            right_node.entry_values = self.entry_values[mid+1:]

        # make current node parent
        left_node.parent = self
        right_node.parent = self
        self.entry_values = [mid_entry_value]

        if self.is_leaf:
            # child node properties
            left_node.rids = self.rids[:mid]
            right_node.rids = self.rids[mid:]

            # set linear pointers between leaf nodes
            left_node.next_node = right_node
            if self.next_node:
                right_node.next_node = self.next_node

            # set current node to non-leaf properties
            self.rids = None
            self.next_node = None
            self.is_leaf = False
        else:
            left_node.child_nodes = self.child_nodes[:mid+1]
            right_node.child_nodes = self.child_nodes[mid+1:]

        self.child_nodes = [left_node, right_node]

        return

    def is_full(self):
        return len(self.entry_values) == self.order

    def __str__(self):
        return (f"entry values: {str(self.entry_values)}\n" +
               f"has {len(self.child_nodes)} children")

class Column_Index_Tree:

    def __init__(self, order:int)->None:
        self.root:Column_Index_Node = Column_Index_Node(order)
        self.order:int              = order
        self.size:int               = 0

    def __len__(self)->int:
        return self.size

    def find_child_node(self, prev_node:Column_Index_Node, entry_value)->tuple[Column_Index_Node,int]:
        for i, cur_val in enumerate(prev_node.entry_values):
            if cur_val > entry_value:
                return prev_node.child_nodes[i], i
        return prev_node.child_nodes[i+1], i + 1

    def merge(self, parent:Column_Index_Node, child:Column_Index_Node, index:int)->None:
        if parent == None: return

        # assign child node's child nodes' parent as parent node
        for child_node in child.child_nodes:
            child_node.parent = parent

        # add child node to parent child node array
        pivot = child.entry_values[0]
        parent.child_nodes.pop(index)
        for i, item in enumerate(parent.entry_values):
            if item > pivot:
                parent.entry_values = parent.entry_values[:i] + [pivot] + parent.entry_values[i:]
                parent.child_nodes = parent.child_nodes[:i] + child.child_nodes + parent.child_nodes[i:]
                break
            elif i + 1 == len(parent.entry_values):
                parent.entry_values.append(pivot)
                parent.child_nodes += child.child_nodes
                break

    def insert_value(self, entry_value, rid:int)->None:
        """
        Insert an entry value to the column.

        Note: RIDs start at index value 1.
        """

        assert rid == self.size + 1, IndexError

        cur_node = self.root
        index = None
        while not cur_node.is_leaf:
            cur_node, index = self.find_child_node(cur_node, entry_value)
        cur_node.insert_value_in_node(entry_value, rid)

        while cur_node != None and cur_node.is_full():
            # prev node was parent if cur_node is None
            cur_node.split_node()
            if cur_node.parent:
                self.merge(cur_node.parent, cur_node, index)
            cur_node = cur_node.parent

        self.size += 1

    def get_rids_equality_search(self, entry_value)->list[int]:
        """
        Returns a list of RIDs associated to a provided entry value.

        If the entry value cannot be found in the tree, an empty list
        will be returned.
        """
        cur_node = self.root
        while not cur_node.is_leaf:
            for i, val in enumerate(cur_node.entry_values):
                if val > entry_value:
                    cur_node = cur_node.child_nodes[i]
                elif val == entry_value or i + 1 == len(cur_node.entry_values):
                    cur_node = cur_node.child_nodes[i+1]
        try:
            return cur_node.rids[cur_node.entry_values.index(entry_value)]
        except ValueError:
            return []

    def get_rids_range_search(self, lower_bound, upper_bound)->list[int]:
        """
        Returns all RIDs associated with entry values in between
        the specified upper and lower bound values (inclusive).

        If no entry values can be found, an empty list will be returned.
        """

        assert lower_bound <= upper_bound, ValueError

        cur_node = self.root
        while not cur_node.is_leaf:
            for i, val in enumerate(cur_node.entry_values):
                if val > lower_bound:
                    cur_node = cur_node.child_nodes[i]
                    break
                elif val == lower_bound or i + 1 == len(cur_node.entry_values):
                    cur_node = cur_node.child_nodes[i+1]
                    break

        return_list = []
        while cur_node != None:
            for i, val in enumerate(cur_node.entry_values):
                if val > upper_bound: break
                if lower_bound <= val and val <= upper_bound:
                    return_list += cur_node.rids[i]
            if val > upper_bound: break
            cur_node = cur_node.next_node
        return return_list

    def return_entry_values_lists(self, node:Column_Index_Node, return_list:list)->list:
        return_list += node.entry_values
        for child_node in node.child_nodes:
            return_list += self.return_entry_values_lists(child_node)
        return return_list

class Index:

    def __init__(self, num_columns:int)->None:
        # One index for each table. All are empty initially.
        self.indices = [Column_Index_Tree(ORDER_CHOICE)] *  num_columns

    def locate(self, value, column_index:int)->list[int]:
        """
        # returns the location of all records with the given value on column "column"
        """                
        print(f"Locating value {value} in column {column_index}")
        return self.indices[column_index].get_rids_equality_search(value)
        

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