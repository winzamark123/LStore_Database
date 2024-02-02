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

    def __init__(self, order, rids:list[int], children_nodes:list)->None:
        self.entry_values = [None] * order
        self.rids = rids if (rids) else [None] * order
        self.parent_node = None
        self.children_nodes = children_nodes if (children_nodes) else [None] * (order + 1)
        self.next_leaf_node = None
        self.is_leaf = False

    def insert_to_leaf(self, entry_value, rid:int):
        """
        Inserts entry_value (key) and/or RID (value) into node's array.
        It is possible that the new array will be larger than order * 2. If so,
        there is a function in the tree that splits the node apart (with the
        help of some functions in this node object).
        """
        if self.entry_values:
            for i, cur_val in enumerate(self.entry_values):
                if entry_value == cur_val:
                    self.rids[i].append(rid) # associate RID to entry value
                    break
                elif entry_value < cur_val:
                    self.entry_values = self.entry_values[:i] + [entry_value] + self.entry_values[i:]
                    self.rids = self.rids[:i] + [[rid]] + self.rids[i:]
                    break
                elif i == len(self.entries) - 1:
                    self.entry_values += [entry_value]
                    self.rids += [[rid]]
        else:
            self.entry_values = [entry_value]
            self.rids = [[rid]]

class Column_Index_Tree:

    def __init__(self, order):
        self.order = order
        self.root = Column_Index_Node()
        self.root.is_leaf = True

    def insert_key(self, entry_value, rid:int):
        cur_node = self.search_for_node(entry_value)
        cur_node.insert_to_leaf(entry_value, rid)
        self.split_leaf(cur_node)
        self.split_chain_parents(cur_node.parent_node)

    def split_leaf(self, leaf:Column_Index_Node)->None:
        if not leaf.is_leaf: raise ValueError
        if len(leaf.entry_values) > self.order: return

        parent_is_root = False
        if leaf.parent_node == None:
            parent = Column_Index_Node()
            parent_is_root = True
        else:
            parent = leaf.parent_node

        mid = len(leaf.entry_values) // 2
        mid_entry_value = leaf.entry_values[mid]
        leaf = Column_Index_Node(leaf.entry_values[:mid], leaf.rids[:mid], None)
        new_leaf_node = Column_Index_Node(leaf.entry_values[mid:], leaf.rids[mid:], None)

        # insert new node info into parent key array
        for i, cur_val in enumerate(parent.entry_values):
            if cur_val > new_leaf_node.entry_values[0]:
                parent.entry_values = parent.entry_values[:i] + [mid_entry_value] + parent.entry_values[i:]
                parent.children_nodes = parent.children_nodes[:i] + [new_leaf_node] + parent.children_nodes[i:]
                break
            elif cur_val == new_leaf_node.entry_values[0] or i + 1 == len(parent.entry_values):
                parent.entry_values = parent.entry_values[:i+1] + [mid_entry_value] + parent.entry_values[i+1:]
                parent.children_nodes = parent.children_nodes[:i+1] + [new_leaf_node] + parent.children_nodes[i+1:]
                break

        # make parent root if applicable
        if parent_is_root:
            self.root = parent

    def split_chain_parents(self, inner_node:Column_Index_Node):
        if inner_node.is_leaf: raise ValueError
        if len(inner_node.entry_values) <= self.order: return

        parent_is_root = False
        if inner_node.parent_node == None:
            parent = Column_Index_Node()
            parent_is_root = True
        else:
            parent = inner_node.parent_node

        mid = len(inner_node.entry_values) // 2
        mid_entry_value = inner_node.entry_values[mid]
        inner_node = Column_Index_Node(inner_node.entry_values[:mid], None, inner_node.children_nodes[:mid+1])
        new_inner_node = Column_Index_Node(inner_node.entry_values[mid+1:], None, inner_node.children_nodes[mid+1:]) # we aren't going to include the middle key in the right child node

        # insert new node into parent key array (don't include key into children nodes)
        for i, cur_val in enumerate(parent.entry_values):
            if cur_val < new_inner_node.entry_values[0]:
                parent.entry_values = parent.entry_values[:i] + [mid_entry_value] + parent.entry_values[i:]
                parent.children_nodes = parent.children_nodes[:i] + [new_inner_node] + parent.children_nodes[i:]
                break
            elif cur_val == new_inner_node.entry_values[0] or i + 1 == len(parent.entry_values):
                parent.entry_values = parent.entry_values[:i+1] + [mid_entry_value] + parent.entry_values[i+1:]





        if parent_is_root:
            self.root = parent


    def search_for_node(self, entry_value):
        cur_node = self.root
        while cur_node.is_leaf == False:
            for i, cur_val in enumerate(cur_node.entry_values):
                if cur_val < entry_value:
                    cur_node = cur_node.children_nodes[i]
                    break
                elif cur_val == entry_value or i + 1 == len(cur_node.entry_values):
                    cur_node = cur_node.children_nodes[i+1]
        return cur_node


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

# TESTS
if __name__ == "__main__":
    tree = Column_Index_Tree()