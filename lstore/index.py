import os
from bplustree import BPlusTree
from pickle import loads, dumps

# source for bplustree module: https://github.com/NicolasLM/bplustree

class Index_Column:

  def __init__(self, column_index_file:str, order:int)->None:
    self.tree = BPlusTree(filename=column_index_file, order=order)
    self.is_key = False

  def __del__(self):
    self.tree.close()

  def set_as_primary_key(self):
    self.is_key = True

  def add_value(self, entry_value, rid:int)->None:
    def add_value_not_in_tree():
      self.tree[entry_value] = dumps({rid})

    def add_value_in_tree():
      if self.is_key: raise ValueError
      entry_value_set = set(loads(self.tree[entry_value]))
      if rid in entry_value_set: raise ValueError
      entry_value_set.add(rid)
      self.tree[entry_value] = dumps(entry_value_set)

    add_value_in_tree() if entry_value in self.tree else add_value_not_in_tree()

  def update_value(self, old_entry_value, new_entry_value, rid:int)->None:
    # remove previous entry tied to RID
    if not old_entry_value in self.tree:
      raise ValueError
    old_entry_set = set(loads(self.tree[old_entry_value]))
    if rid not in old_entry_set:
      raise ValueError
    old_entry_set.remove(rid)
    self.tree[old_entry_value] = dumps(old_entry_set)

    # add RID back w/ new entry
    if not new_entry_value in self.tree:
      self.tree[new_entry_value] = dumps({rid})
    else:
      new_entry_value_set = set(loads(self.tree[new_entry_value]))
      new_entry_value_set.add(rid)
      self.tree[new_entry_value] = dumps(new_entry_value_set)

  def get_single_entry(self, entry_value)->set[int]:
    # print("here for", entry_value)
    if not entry_value in self.tree:
      return {}
    return set(loads(self.tree[entry_value]))

  def get_ranged_entry(self, lower_bound, upper_bound)->set[int]:
    rset = set()
    try:
      for key, rids in self.tree.items():
        if lower_bound <= key and key <= upper_bound:
          rset = rset.union(set(loads(rids)))
    except RuntimeError: # bypasses issue w/ using iteration on tree
      pass
    return rset


class Index:

  def __init__(self, table_dir_path:str, primary_key_index:int, order:int)->None:
    self.index_dir_path = os.path.exists(os.path.join(table_dir_path, "index"))
    if not os.path.exists(self.index_dir_path):
      os.makedirs(self.index_dir_path, exist_ok=False)
    self.indices:dict[int,Index_Column] = dict() # {column_index: Index_Column}
    self.order = order
    self.primary_key_index = primary_key_index

  def create_index(self, column_index:int)->None:
    """
    Creates an index for a specified column.

    Raises a ValueError if a column's index has already been created.
    """

    if column_index in self.indices:
      raise ValueError

    column_index_file = os.path.join(self.index_dir_path, f"{column_index}.db")
    self.indices[column_index] = Index_Column(column_index_file, self.order)

  def drop_index(self, column_index:int)->None:
    if len(self.indices) - 1 >= column_index:
      del self.indices[column_index]

  def insert_record_to_index(self, record_columns, rid:int)->None:
    """
    Inserts all entry values into the index with the record's associated
    RID.

    If not all record fields are declared, returns ValueError.

    Note: all record columns must be initialized. If a record column
    does not have a value, simply pass None as its element value.
    """

    if len(record_columns) != len(self.columns):
      raise ValueError

    for i, record_entry_value in enumerate(record_columns):
        self.indices[i].add_value(record_entry_value, rid)

  def locate(self, entry_value, column_index:int)->set[int]:
    """
    Returns the location of all records with the given value
    within a specified column.
    """

    return self.indices[column_index].get_single_entry(entry_value)

  def locate_range(self, begin, end, column_index:int)->set[int]:
    """
    Returns the RIDs of all records with values in a specified column
    between "begin" and "end" (bounds-inclusive).
    """

    return self.indices[column_index].get_ranged_entry(begin, end)

  def update_entry(self, old_entry_value, new_entry_value, rid:int, column_index:int)->None:
    """
    Updates an RID-associated entry value.
    """

    try:
      self.indices[column_index].update_value(old_entry_value, new_entry_value, rid)
    except ValueError:
      print("Error: Unable to perform update properly.")

