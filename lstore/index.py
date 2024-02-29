import os
from bplustree import BPlusTree
from pickle import loads, dumps

# source for bplustree module: https://github.com/NicolasLM/bplustree

class Index_Column:

  def __init__(self, file_path:str, order:int)->None:
    self.tree = BPlusTree(filename=file_path, order=order)
    self.is_key = False

  def __del__(self):
    """
    Delete index column.
    """

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

  def __init__(self, table_dir_path:str, num_columns:int, primary_key_index:int, order:int)->None:
    assert primary_key_index < num_columns, IndexError
    
    self.index_dir_path:str             = os.path.join(table_dir_path, "index")
    self.num_columns:int                = num_columns
    self.primary_key_index:int          = primary_key_index
    self.order:int                      = order
    self.indices:dict[int,Index_Column] = dict() # {column_index: Index_Column}

    if os.path.exists(self.index_dir_path):
      self.__load_column_indices()
    else:
      os.makedirs(self.index_dir_path, exist_ok=False)
      # automatically creates an index for the primary key
      self.create_index(primary_key_index)

  def __load_column_indices(self)->None:
    for column_db_file in os.listdir(self.index_dir_path):
      column_index = int(column_db_file.removesuffix(".db"))
      column_index_path = os.path.join(self.index_dir_path, column_db_file)
      self.indices[column_index] = Index_Column(column_index_path, self.order)

  def __get_column_index_filename(self, column_index:int)->str:
    return os.path.join(self.index_dir_path, f"{column_index}.db")

  def create_index(self, column_index:int)->None:
    """
    Creates an index for a specified column.
    """

    if os.path.exists(self.__get_column_index_filename(column_index)):
      raise FileExistsError
    if column_index in self.indices:
      raise ValueError

    self.indices[column_index] = Index_Column(self.__get_column_index_filename(column_index), self.order)

  def drop_index(self, column_index:int)->None:
    """
    Drops an index of a specified column.

    Warning: deletes the data from the disk.
    """

    if not os.path.exists(self.__get_column_index_filename(column_index)):
      raise ValueError
    if not column_index in self.indices:
      raise ValueError

    del self.indices[column_index]
    os.remove(self.__get_column_index_filename(column_index))

  def insert_record_to_index(self, record_columns, rid:int)->None:
    """
    Inserts all entry values into the index with the record's associated
    RID.

    If not all record fields are declared, returns ValueError.

    Note: all record columns must be initialized. If a record column
    does not have a value, simply pass None as its element value.
    """

    if len(record_columns) != self.num_columns:
      raise ValueError

    for i, record_entry_value in enumerate(record_columns):
        if i in self.indices:
          self.indices[i].add_value(record_entry_value, rid)

  def locate(self, entry_value, column_index:int)->set[int]:
    """
    Returns the location of all records with the given value
    within a specified column.
    """

    if not column_index in self.indices:
      raise ValueError

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