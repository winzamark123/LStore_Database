import os
from bplustree import BPlusTree
from pickle import loads, dumps

# source for bplustree module: https://github.com/NicolasLM/bplustree

class Index_Column:

  def __init__(self, table_name:str, column_name:str, order:int):
    self.name = column_name
    self.tree = BPlusTree(filename=f"index/{table_name}_{column_name}.db",
                          order=order)
    self.is_key = False

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
    if not len(old_entry_set):
      del self.tree[old_entry_value]
    else:
      self.tree[old_entry_value] = dumps(old_entry_set)

    # add RID back w/ new entry
    if not self.tree[new_entry_value]:
      self.tree[new_entry_value] = dumps({rid})
    else:
      self.tree[new_entry_value] = dumps(set(loads(self.tree[new_entry_value])).add(rid))

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

  def __init__(self, table_name:str, columns:list[str], primary_key_index:int, order:int)->None:
    if not os.path.exists("index"):
        os.mkdir("index")
    self.table_name = table_name
    self.columns = columns
    self.order = order
    self.indices = [Index_Column(table_name, column, order) for column in columns]
    self.indices[primary_key_index].set_as_primary_key()

  def __del__(self):
    for index in self.indices:
      index.tree.close()

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

  def create_index(self, column_name:str, index_to_insert_column:int=None)->None:
    new_index = Index_Column(self.table_name, column_name, self.order)
    if index_to_insert_column == None:
      self.indices.append(new_index)
    else:
      self.indices.insert(index_to_insert_column, new_index)

  def drop_index(self, column_index:int)->None:
    if len(self.indices) - 1 >= column_index:
      del self.indices[column_index]
