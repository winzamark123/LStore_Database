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
        self.split_leaf_node() if self.is_leaf else self.split_nonleaf_node()

    def split_leaf_node(self):
        mid = self.order // 2
        left_node = Column_Index_Node(self.order)
        right_node = Column_Index_Node(self.order)

        # make current node parent of new child nodes
        left_node.parent = self
        right_node.parent = self

        # set next node pointers
        left_node.next_node = right_node
        right_node.next_node = self.next_node

        # split entry values
        left_node.entry_values = self.entry_values[:mid]
        right_node.entry_values = self.entry_values[mid:]
        self.entry_values = [self.entry_values[mid]]

        # split RIDs
        left_node.rids = self.rids[:mid]
        right_node.rids = self.rids[mid:]

        # set current node to non-leaf properties
        self.is_leaf = False
        self.next_node = None
        self.rids = []
        self.child_nodes = [left_node, right_node]

    def split_nonleaf_node(self):
        assert self.rids == [], ValueError

        mid = self.order // 2
        left_node = Column_Index_Node(self.order)
        left_node.is_leaf = False
        right_node = Column_Index_Node(self.order)
        right_node.is_leaf = False

        # make current node parent of two children
        left_node.parent = self
        right_node.parent = self

        # set next node pointers
        left_node.next_node = right_node
        right_node.next_node = self.next_node

        # split entry values
        left_node.entry_values = self.entry_values[:mid]
        right_node.entry_values = self.entry_values[mid+1:]
        self.entry_values = [self.entry_values[mid]]

        #split child nodes
        left_node.child_nodes = self.child_nodes[:mid+1]
        for child_node in left_node.child_nodes:
            child_node.parent = left_node
        right_node.child_nodes = self.child_nodes[mid+1:]
        for child_node in right_node.child_nodes:
            child_node.parent = right_node

        self.child_nodes = [left_node, right_node]

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

    def find_node(self, prev_node:Column_Index_Node, entry_value)->Column_Index_Node:
        for i, cur_val in enumerate(prev_node.entry_values):
            if cur_val > entry_value:
                return prev_node.child_nodes[i]
        return prev_node.child_nodes[i+1]

    def get_node_index(self, prev_node:Column_Index_Node, entry_value)->int:
        for i, cur_val in enumerate(prev_node.entry_values):
            if cur_val > entry_value:
                return i
        return i + 1

    def merge(self, parent_tree:Column_Index_Node, child_tree:Column_Index_Node, index:int)->None:
        if parent_tree == None: return

        # assign child node's child nodes' parent as parent node
        for child_node in child_tree.child_nodes:
            child_node.parent = parent_tree

        # add child node to parent child node array
        pivot = child_tree.entry_values[0]
        parent_tree.child_nodes.pop(index)
        for i, item in enumerate(parent_tree.entry_values):
            if item > pivot:
                # get node pointers
                ## previous node
                prev_node = parent_tree.child_nodes[i-1]
                while (not prev_node.is_leaf):
                    prev_node = prev_node.child_nodes[-1]
                ## next node
                next_node = parent_tree.child_nodes[i]
                while (not prev_node.is_leaf):
                    next_node = next_node.child_nodes[0]

                # fix node pointers for child tree leaf nodes
                ## connect front of child tree
                next_to_prev_leaf_node_pointer = child_tree.child_nodes[0]
                while (not next_to_prev_leaf_node_pointer.is_leaf):
                    next_to_prev_leaf_node_pointer = next_to_prev_leaf_node_pointer.child_nodes[0]
                prev_node.next_node = next_to_prev_leaf_node_pointer
                ## connect back of child tree
                prev_from_next_leaf_node_pointer = child_tree.child_nodes[-1]
                while (not prev_from_next_leaf_node_pointer.is_leaf):
                    prev_from_next_leaf_node_pointer = prev_from_next_leaf_node_pointer.child_nodes[-1]
                prev_from_next_leaf_node_pointer.next_node = next_node

                # add child tree to parent tree
                parent_tree.entry_values = parent_tree.entry_values[:i] + [pivot] + parent_tree.entry_values[i:]
                parent_tree.child_nodes = parent_tree.child_nodes[:i] + child_tree.child_nodes + parent_tree.child_nodes[i:]
                break
            elif i + 1 == len(parent_tree.entry_values):
                # get node pointers
                prev_node = parent_tree.child_nodes[-1]
                while (not prev_node.is_leaf):
                    prev_node = prev_node.child_nodes[-1]

                # fix node pointers for child tree leaf nodes
                ## connect front of child tree
                next_to_prev_leaf_node_pointer = child_tree.child_nodes[0]
                while (not next_to_prev_leaf_node_pointer.is_leaf):
                    next_to_prev_leaf_node_pointer = next_to_prev_leaf_node_pointer.child_nodes[0]
                prev_node.next_node = next_to_prev_leaf_node_pointer
                ## connect back of child tree
                last_leaf_node_pointer = child_tree.child_nodes[-1]
                while (not last_leaf_node_pointer.is_leaf):
                    last_leaf_node_pointer = last_leaf_node_pointer.child_nodes[-1]
                last_leaf_node_pointer.next_node = None

                ## add child tree to parent tree
                parent_tree.entry_values.append(pivot)
                parent_tree.child_nodes += child_tree.child_nodes
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
            cur_node = self.find_node(cur_node, entry_value)
        cur_node.insert_value_in_node(entry_value, rid)

        value_to_find = entry_value
        while cur_node != None and cur_node.is_full():
            cur_node.split_node()
            split_node_root_value = cur_node.entry_values[0]
            if cur_node.parent:
                index = self.get_node_index(cur_node.parent, value_to_find)
                self.merge(cur_node.parent, cur_node, index)
            cur_node = cur_node.parent
            value_to_find = split_node_root_value

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
                    break
                elif val == entry_value or i + 1 == len(cur_node.entry_values):
                    cur_node = cur_node.child_nodes[i+1]
                    break
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

    def __init__(self, num_columns:int, order:int)->None:
        self.indices = [Column_Index_Tree(order)] * num_columns

    def insert_record_to_index(self, record_columns, rid:int)->None:
        """
        Inserts all entry values into the index with the record's associated
        RID.

        Note: all record columns must be initialized. If a record column
        does not have a value, simply pass None as its element value.
        """
        for i, record_entry_value in enumerate(record_columns):
            self.indices[i].insert_value(record_entry_value, rid)

    def locate(self, value, column_index:int)->list[int]:
        """
        Returns the location of all records with the given value
        within a specified column.
        """
        return self.indices[column_index].get_rids_equality_search(value)


    def locate_range(self, begin, end, column_index:int)->list[int]:
        """
        Returns the RIDs of all records with values in a specified column
        between "begin" and "end" (bounds-inclusive).
        """
        return self.indices[column_index].get_rids_range_search(begin, end)

    def drop_index(self, column_index:int):
        """
        TODO?
        """
        pass