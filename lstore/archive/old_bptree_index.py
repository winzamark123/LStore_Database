from copy import deepcopy

"""
A data structure holding indices for various columns of a table.
entry_value column should be indexed by default, other columns can be indexed
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

Each node's entry_value contains:
    - entry_value: column value
    - value: base RID of specific column value (index of record in physical space)
Thus nodes will be created with record_info w/ indices:
    - 0: primary entry_value
    - 1: base RID
(maybe make it a dictionary/hash-table instead w/ list of RIDs?)

Q: Each entry_value will store a base RID, but what happens when we update the column?
A: When we update the column, the record 'entry_value' associated to the RID will be
   removed and re-inserted with the new value as 'entry_value' and the same RID as
   'value' (assuming 'entry_value' and 'value' are utilized in dictionary implementation)
    - "when a new version of a record is created (i.e., a new tail record),
       first, all indexes defined on unaffected columns do not have to
       be modified and, second, only the affected indexes are modified
       with the updated values, but they continue to point to base records
       and not the newly created tail records." pg. 543
"""

class Entry_Value:

    def __init__(self, key, starting_rid:int)->None:
        self.key           = key
        self.rids:set[int] = {starting_rid}
        self.is_leaf:bool  = True

    def __str__(self)->str:
        return f"{self.key}: {str(self.rids)}"

    def set_as_non_leaf(self)->None:
        assert self.is_leaf, ValueError

        self.is_leaf = False
        self.rids = {}

    def add_rid(self, rid:int)->None:
        """
        Adds RID to the entry value object.

        Returns ValueError if RID not found.
        """
        assert self.is_leaf, ValueError

        if rid in self.rids: raise ValueError
        self.rids.add(rid)

    def delete_rid(self, rid:int)->None:
        """
        Deletes RID from the entry value object.

        Returns ValueError if RID not found.
        """

        assert self.is_leaf, ValueError

        if rid not in self.rids: raise ValueError
        self.rids.remove(rid)

class Column_Index_Node:

    def __init__(self, order:int)->None:
        self.order:int                           = order
        self.entry_values:list[Entry_Value]      = []
        self.child_nodes:list[Column_Index_Node] = [] # only used for non-leaf nodes (i:i+1 w/ entry_values)

        self.parent:Column_Index_Node            = None
        self.next_node:Column_Index_Node         = None
        self.prev_node:Column_Index_Node         = None
        self.is_leaf:bool                        = True

    def __len__(self)->int:
        return len(self.entry_values)

    def __str__(self)->str:
        return (f"{str(self.get_keys())} (has {len(self.child_nodes)} children)")

    def create_entry_value_object(self, entry_value, rid:int)->Entry_Value:
        return Entry_Value(entry_value, rid)

    def insert_value_in_node(self, entry_value, rid:int)->None:
        assert self.is_leaf, ValueError

        if not self.entry_values:
            self.entry_values.append(self.create_entry_value_object(entry_value, rid))
            return

        for i, item in enumerate(self.entry_values):
            if item.key == entry_value:
                item.add_rid(rid)
                break
            elif item.key > entry_value:
                self.entry_values.insert(i, self.create_entry_value_object(entry_value, rid))
                break
            elif i + 1 == len(self.entry_values):
                self.entry_values.append(self.create_entry_value_object(entry_value, rid))
                break

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
        if (self.next_node):
            self.next_node.prev_node = right_node
        left_node.prev_node = self.prev_node
        if (self.prev_node):
            self.prev_node.next_node = left_node
        right_node.prev_node = left_node

        # split entry values
        left_node.entry_values = self.entry_values[:mid]
        right_node.entry_values = self.entry_values[mid:]
        self.entry_values = [deepcopy(self.entry_values[mid])]

        # set current node to non-leaf properties
        self.is_leaf = False
        self.next_node = None
        self.entry_values[0].set_as_non_leaf()
        self.child_nodes = [left_node, right_node]

    def split_nonleaf_node(self):
        assert self.next_node == None, ValueError
        assert self.prev_node == None, ValueError
        assert self.get_rids() == [], ValueError

        mid = self.order // 2
        left_node = Column_Index_Node(self.order)
        left_node.is_leaf = False
        right_node = Column_Index_Node(self.order)
        right_node.is_leaf = False

        # make current node parent of two children
        left_node.parent = self
        right_node.parent = self

        # split entry values
        left_node.entry_values = self.entry_values[:mid]
        right_node.entry_values = self.entry_values[mid+1:]
        self.entry_values = [deepcopy(self.entry_values[mid])]

        #split child nodes
        left_node.child_nodes = self.child_nodes[:mid+1]
        for child_node in left_node.child_nodes:
            child_node.parent = left_node
        right_node.child_nodes = self.child_nodes[mid+1:]
        for child_node in right_node.child_nodes:
            child_node.parent = right_node

        self.child_nodes = [left_node, right_node]

    def get_keys(self)->list:
        return [_.key for _ in self.entry_values]

    def get_rids(self)->list[set[int]]:
        rlist = list()
        for _ in self.entry_values:
            if _.rids: rlist.append(_.rids)
        return rlist

    def get_entry_by_key(self, entry_value)->Entry_Value:
        return self.entry_values[self.get_keys().index(entry_value)]

    def is_full(self):
        return len(self.entry_values) == self.order

class Column_Index_Tree:

    def __init__(self, order:int)->None:
        self.root:Column_Index_Node = Column_Index_Node(order)
        self.order:int              = order
        self.size:int               = 0

    def __len__(self)->int:
        return self.size

    def find_child_node(self, node:Column_Index_Node, entry_value)->Column_Index_Node:
        for i, item in enumerate(node.entry_values):
            if item.key > entry_value:
                return node.child_nodes[i]
        return node.child_nodes[i+1]

    def find_non_leaf_reference(self, node:Column_Index_Node, entry_value)->tuple[Column_Index_Node, int]:
        while node != None:
            node = node.parent
            if entry_value in node.get_keys():
                return node, node.get_keys().index(entry_value)
        raise ValueError

    def get_index_for_split_node(self, prev_node:Column_Index_Node, entry_value)->int:
        for i, item in enumerate(prev_node.entry_values):
            if item.key > entry_value:
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
            if item.key > pivot.key:
                # add child tree to parent tree
                parent_tree.entry_values.insert(i, pivot)
                parent_tree.child_nodes = parent_tree.child_nodes[:i] + child_tree.child_nodes + parent_tree.child_nodes[i:]
                break
            elif i + 1 == len(parent_tree.entry_values):
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
        while not cur_node.is_leaf:
            cur_node = self.find_child_node(cur_node, entry_value)
        cur_node.insert_value_in_node(entry_value, rid)

        value_to_find = entry_value
        while cur_node != None and cur_node.is_full():
            cur_node.split_node()
            split_node_root_value = cur_node.entry_values[0]
            if cur_node.parent:
                self.merge(cur_node.parent, cur_node, self.get_index_for_split_node(cur_node.parent, value_to_find))
            cur_node = cur_node.parent
            value_to_find = split_node_root_value.key

        self.size += 1

    def get_rids_equality_search(self, entry_value)->set[int]:
        """
        Returns a list of RIDs associated to a provided entry value.

        If the entry value cannot be found in the tree, an empty list
        will be returned.
        """

        cur_node = self.root
        while not cur_node.is_leaf:
            cur_node = self.find_child_node(cur_node, entry_value)
        try:
            return cur_node.entry_values[cur_node.get_keys().index(entry_value)].rids
        except ValueError:
            return {}

    def get_rids_range_search(self, lower_bound, upper_bound)->set[int]:
        """
        Returns all RIDs associated with entry values in between
        the specified upper and lower bound values (inclusive).

        If no entry values can be found, an empty list will be returned.
        """

        assert lower_bound <= upper_bound, ValueError

        cur_node = self.root
        while not cur_node.is_leaf:
            cur_node = self.find_child_node(cur_node, lower_bound)

        rset = set()
        break_while_loop = False
        while cur_node != None and not break_while_loop:
            for item in cur_node.entry_values:
                if item.key > upper_bound:
                    break_while_loop = True
                    break
                elif lower_bound <= item.key and item.key <= upper_bound:
                    rset = rset.union(item.rids)
                else:
                    raise ValueError
            cur_node = cur_node.next_node
        return rset

    def delete_entry(self, entry_value, rid:int)->None:
        # find entry value in tree
        cur_node = self.root
        while not cur_node.is_leaf:
            cur_node = self.find_child_node(cur_node, entry_value)
        try:
            index = cur_node.get_keys().index(entry_value)
            entry_value_object = cur_node.entry_values[index]
        except ValueError:
            print(f"Error: Entry value {entry_value} not found.")
            return

        # find RID in entry value
        try:
            entry_value_object.delete_rid(rid)
        except ValueError:
            print(f"Error: RID value {rid} is not associated with entry value {entry_value}.")
            return
        # stop deletion process if entry value still has associated RIDs
        if len(entry_value_object.rids): return

        # case 1: entry value is not found in any non-leaf nodes
        if index != 0:
            del cur_node.entry_values[index]
            return

        # TODO: determine if deleting from root is its own case

        reference_node, reference_index = self.find_non_leaf_reference(cur_node, entry_value)
        # case 2: entry value is at beginning of leaf node but can be replaced by the next entry value
        if len(cur_node.entry_values) > 1:
            del cur_node.entry_values[0]
            reference_node.entry_values[reference_index] = deepcopy(cur_node.entry_values[0])
            reference_node.entry_values[reference_index].set_as_non_leaf()
            return

        # case 3a: entry value is at beginning of leaf node and steal from left neighbor if possible
        left_stolen_neighbor_node = reference_node.child_nodes[reference_index]
        while not left_stolen_neighbor_node.is_leaf:
            left_stolen_neighbor_node = left_stolen_neighbor_node.child_nodes[-1]
        if len(left_stolen_neighbor_node) > 1:
            left_stolen_neighbor = deepcopy(left_stolen_neighbor_node.entry_values[-1])
            cur_node.entry_values.append(deepcopy(left_stolen_neighbor))
            
            left_stolen_neighbor.set_as_non_leaf()
            reference_node.entry_values[reference_index] = left_stolen_neighbor
            del cur_node.entry_values[index]
            del left_stolen_neighbor_node.entry_values[-1]
            return

        # case 3b: entry value is at beginning of leaf node and steal from right neighbor if possible
        print(reference_node)
        print([str(_) for _ in reference_node.child_nodes])
        print("child node index", reference_index+1)
        try:
            right_stolen_neighbor_node = reference_node.child_nodes[reference_index+1]
        except IndexError:
            pass
        else:
            while not right_stolen_neighbor_node.is_leaf:
                right_stolen_neighbor_node = right_stolen_neighbor_node.child_nodes[0]
            print("current node", cur_node.entry_values[0])
            print("reference node", reference_node)
            print("reference children", [str(_) for _ in reference_node.child_nodes])
            print("right stolen neighbor node", right_stolen_neighbor_node)
            if len(right_stolen_neighbor_node) > 1:
                right_stolen_neighbor = deepcopy(right_stolen_neighbor_node.entry_values[0])
                cur_node.entry_values.append(deepcopy(right_stolen_neighbor))
                right_stolen_neighbor.set_as_non_leaf()
                reference_node.entry_values[reference_index] = right_stolen_neighbor
                del cur_node.entry_values[index]
                del right_stolen_neighbor_node.entry_values[0]
                return






class Index:

    def __init__(self, num_columns:int, order:int)->None:
        self.indices = [Column_Index_Tree(order) for _ in range(num_columns)]

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

    def update_entry(self, entry_value, rid:int)->None:
        """
        Updates an RID-associated entry value.
        """
        pass

    def drop_index(self, column_index:int):
        """
        Returns the RIDs of all records with values in a specified column
        between "begin" and "end" (bounds-inclusive).
        """
        pass