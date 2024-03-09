import os
from bplustree import BPlusTree
from pickle import loads, dumps

from lstore.disk import DISK
import lstore.config as Config

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

  def __add_rid_to_non_existant_entry_value(self, entry_value, rid:int)->None:
    """
    Adds an RID to a non-existant entry value of the column's tree.

    If entry value is found, raises a KeyError.
    """
    if entry_value in self.tree: raise KeyError
    self.tree[entry_value] = dumps({rid})

  def __add_rid_to_existing_entry_value(self, entry_value, rid:int)->None:
    """
    Adds an RID to an existing entry value of the column's tree.

    If not entry value is found or the RID exists, raises a KeyError.
    """
    if not entry_value in self.tree: raise KeyError
    entry_value_set = set(loads(self.tree[entry_value]))
    if rid in entry_value_set: raise KeyError
    entry_value_set.add(rid)
    self.tree[entry_value] = dumps(entry_value_set)

  def __remove_rid_from_entry_value(self, entry_value, rid:int)->None:
    """
    Deletes an RID from a specified entry value of the column's tree.

    If no entry value or RID found, raises a KeyError.
    """
    if not entry_value in self.tree: raise KeyError
    entry_value_set = set(loads(self.tree[entry_value]))
    entry_value_set.remove(rid)
    if len(entry_value_set) == 0:
      del self.tree[entry_value]
    else:
      self.tree[entry_value] = dumps(entry_value_set)

  def set_as_primary_key(self):
    self.is_key = True

  def add_value(self, entry_value, rid:int)->None:
    if not entry_value in self.tree:
      self.__add_rid_to_non_existant_entry_value(entry_value, rid)
    else:
      self.__add_rid_to_existing_entry_value(entry_value, rid)

  def update_value(self, old_entry_value, new_entry_value, rid:int)->None:
    self.__remove_rid_from_entry_value(old_entry_value, rid)
    if not new_entry_value in self.tree:
      self.__add_rid_to_non_existant_entry_value(new_entry_value, rid)
    else:
      self.__add_rid_to_existing_entry_value(new_entry_value, rid)

  def delete_value(self, entry_value, rid:int)->None:
    self.__remove_rid_from_entry_value(entry_value, rid)

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


class Column_Data_Getter:

  def __init__(self, column_index:int, index_dir_path:str)->None:
    self.column_index:int   = column_index
    self.index_dir_path:str = index_dir_path
    self.values:dict        = dict()

  def get_column_data(self)->dict:
    table_dir_path = os.path.dirname(self.index_dir_path)
    for _ in os.listdir(table_dir_path):
      if os.path.isdir(os.path.join(table_dir_path, _)):
        self.__get_page_range_column_data(os.path.join(table_dir_path, _))
    return self.values

  def __get_page_range_column_data(self, page_range_dir_path:str)->None:
    for _ in os.listdir(page_range_dir_path):
      if os.path.isdir(os.path.join(page_range_dir_path, _)) and _.startswith("BP"):
        self.__get_base_page_column_data(os.path.join(page_range_dir_path, _))

  def __get_base_page_column_data(self, base_page_dir_path:str)->None:
    num_records = DISK.read_metadata_from_disk(base_page_dir_path)["num_records"]
    # TODO: use Diego's implementation of select to grab the latest entry based on RID


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
      # always have an index for the primary key
      self.create_index(primary_key_index)

  def __load_column_indices(self)->None:
    for column_db_file in os.listdir(self.index_dir_path):
      column_index = int(column_db_file.removesuffix(".db"))
      column_index_path = os.path.join(self.index_dir_path, column_db_file)
      self.indices[column_index] = Index_Column(column_index_path, self.order)

  def __get_column_index_filename(self, column_index:int)->str:
    return os.path.join(self.index_dir_path, f"{column_index}.db")

  def __does_index_filename_exist(self, column_index:int)->bool:
    return os.path.exists(self.__get_column_index_filename(column_index))

  def __is_index_in_indices(self, column_index:int)->bool:
    return column_index in self.indices

  def __is_index_key(self, column_index:int)->bool:
    return column_index == self.primary_key_index

  def __check_num_columns_valid(self, columns:tuple)->None:
    if len(columns) != self.num_columns: raise ValueError

  def create_index(self, column_index:int)->None:
    """
    Creates an index for a specified column. This scans the existing data
    in the disk.
    """
    if self.__does_index_filename_exist(column_index): raise FileExistsError
    if self.__is_index_in_indices(column_index): raise KeyError
    if self.__is_index_key(column_index): raise ValueError
    self.indices[column_index] = Index_Column(self.__get_column_index_filename(column_index), self.order)

  def drop_index(self, column_index:int)->None:
    """
    Drops an index of a specified column.

    Warning: deletes the data of the column's index from disk.
    """
    if not self.__does_index_filename_exist(column_index): raise FileNotFoundError
    if not self.__is_index_in_indices(column_index): raise KeyError
    if self.__is_index_key(column_index): raise ValueError
    del self.indices[column_index]
    os.remove(self.__get_column_index_filename(column_index))

  def insert_record(self, record_columns:tuple, rid:int)->None:
    """
    Adds record information to the created index columns.
    """
    self.__check_num_columns_valid(record_columns)
    for i, record_entry_value in enumerate(record_columns):
      if i in self.indices:
        self.indices[i].add_value(record_entry_value, rid)

  def delete_record(self, record_columns:tuple, rid:int)->None:
    """
    Deletes record information from the created index columns.
    """
    self.__check_num_columns_valid(record_columns)
    for i, record_entry_value in enumerate(record_columns):
      if i in self.indices:
        self.indices[i].delete_value(record_entry_value, rid)

  def locate(self, entry_value, column_index:int)->set[int]:
    """
    Returns the location of all records with the given value
    within a specified column.
    """
    if not column_index in self.indices:
      raise KeyError
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
    self.indices[column_index].update_value(old_entry_value, new_entry_value, rid)
