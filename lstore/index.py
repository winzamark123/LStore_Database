import os
from pickle import loads, dumps
from bplustree import BPlusTree
from threading import RLock

from lstore.disk import Disk
from lstore.bufferpool import BUFFERPOOL
from lstore.record_info import RID
import lstore.config as Config

# source for bplustree module: https://github.com/NicolasLM/bplustree


class Index_Column:

    def __init__(self, file_path: str, order: int) -> None:
        self.file_path = file_path
        self.tree = BPlusTree(filename=file_path, order=order)
        self.is_key = False

        self.latch:RLock = RLock()

    def __del__(self):
        """
        Delete index column.
        """
        self.tree.close()

    def __add_rid_to_non_existant_entry_value(self, entry_value, rid: RID) -> None:
        """
        Adds an RID to a non-existant entry value of the column's tree.

        If entry value is found, raises a KeyError.
        """
        if entry_value in self.tree:
            raise KeyError
        self.tree[entry_value] = dumps({rid})

    def __add_rid_to_existing_entry_value(self, entry_value, rid: RID) -> None:
        """
        Adds an RID to an existing entry value of the column's tree.

        If not entry value is found or the RID exists, raises a KeyError.
        """
        if not entry_value in self.tree:
            raise KeyError
        entry_value_set = set(loads(self.tree[entry_value]))
        if rid in entry_value_set:
            raise KeyError
        entry_value_set.add(rid)
        self.tree[entry_value] = dumps(entry_value_set)

    def __remove_rid_from_entry_value(self, entry_value, removed_rid:RID) -> None:
        """
        Deletes an RID from a specified entry value of the column's tree.

        If no entry value or RID found, raises a KeyError.
        """
        if not entry_value in self.tree:
            raise KeyError
        entry_value_set = set(loads(self.tree[entry_value]))
        for rid in entry_value_set:
            if int(rid) == int(removed_rid):
                entry_value_set.remove(rid)
                break
        self.tree[entry_value] = dumps(entry_value_set)

    def set_as_primary_key(self):
        self.is_key = True

    def add_value(self, entry_value, rid: int) -> None:
        with self.latch:
            if not entry_value in self.tree:
                self.__add_rid_to_non_existant_entry_value(entry_value, rid)
            else:
                self.__add_rid_to_existing_entry_value(entry_value, rid)

    def update_value(self, old_entry_value, new_entry_value, rid: RID) -> None:
        with self.latch:
            self.__remove_rid_from_entry_value(old_entry_value, rid)
            if not new_entry_value in self.tree:
                self.__add_rid_to_non_existant_entry_value(new_entry_value, rid)
            else:
                self.__add_rid_to_existing_entry_value(new_entry_value, rid)

    def delete_value(self, entry_value, rid: RID) -> None:
        with self.latch:
            self.__remove_rid_from_entry_value(entry_value, rid)

    def get_single_entry(self, entry_value) -> set[RID]:
        with self.latch:
            if not entry_value in self.tree:
                return {}
            return set(loads(self.tree[entry_value]))

    def get_ranged_entry(self, lower_bound, upper_bound) -> set[RID]:
        with self.latch:
            rset = set()
            try:
                for key, rids in self.tree.items():
                    if lower_bound <= key and key <= upper_bound:
                        rset = rset.union(set(loads(rids)))
            except RuntimeError:  # bypasses issue w/ using iteration on tree
                pass
            return rset


class Index:

    def __init__(self, table_dir_path:str, num_columns:int, primary_key_index:int) -> None:
        assert primary_key_index < num_columns, IndexError

        self.index_dir_path:str              = os.path.join(table_dir_path, "index")
        self.num_columns:int                 = num_columns
        self.primary_key_index:int           = primary_key_index
        self.order:int                       = Config.INDEX_ORDER_NUMBER
        self.indices:dict[int, Index_Column] = dict()  # {column_index: Index_Column}

        self.latch:RLock                      = RLock()

        if os.path.exists(self.index_dir_path):
            self.__load_column_indices()
        else:
            os.makedirs(self.index_dir_path, exist_ok=False)
            # always have an index for the primary key
            self.create_index(primary_key_index)

    def __load_column_indices(self) -> None:
        with self.latch:
            for column_db_file in os.listdir(self.index_dir_path):
                column_index = int(column_db_file.removesuffix(".db"))
                column_index_path = os.path.join(self.index_dir_path, column_db_file)
                self.indices[column_index] = Index_Column(column_index_path, self.order)

    def __get_column_index_filename(self, column_index: int) -> str:
        return os.path.join(self.index_dir_path, f"{column_index}.db")

    def __does_index_filename_exist(self, column_index: int) -> bool:
        return os.path.exists(self.__get_column_index_filename(column_index))

    def __is_index_in_indices(self, column_index: int) -> bool:
        return column_index in self.indices

    def __is_index_key(self, column_index: int) -> bool:
        return column_index == self.primary_key_index

    def __check_num_columns_valid(self, columns: tuple) -> None:
        if len(columns) != self.num_columns:
            raise ValueError

    def create_index(self, column_index: int) -> None:
        """
        Creates an index for a specified column. This scans the existing data
        in the disk.
        """
        with self.latch:
            if self.__does_index_filename_exist(column_index): raise FileExistsError
            if self.__is_index_in_indices(column_index): raise KeyError

            self.indices[column_index] = Index_Column(self.__get_column_index_filename(column_index), self.order)

            # add column index values and their respective RIDs to the new tree
            num_records:int = Disk.read_from_path_metadata(os.path.dirname(self.index_dir_path))["num_records"]
            for rid in range(1, num_records+1):
                rid = RID(rid)
                # get info of rid's base page
                base_page_path = os.path.join(os.path.dirname(self.index_dir_path), f"PR{rid.get_page_range_index()}", f"BP{rid.get_base_page_index()}")
                assert os.path.isdir(base_page_path)
                tid = BUFFERPOOL.get_indirection_tid(rid, base_page_path)
                tail_page_path = os.path.join(os.path.dirname(self.index_dir_path), f"PR{tid.get_page_range_index()}", f"TP{tid.get_tail_page_index()}")
                schema_encoding = BUFFERPOOL.get_schema_encoding(rid, base_page_path)
                if not schema_encoding[column_index]: entry_val = BUFFERPOOL.get_record_entry(rid, base_page_path, column_index)
                else:                                 entry_val = BUFFERPOOL.get_record_entry(tid, tail_page_path, column_index)
                self.indices[column_index].add_value(entry_val, rid)

    def drop_index(self, column_index: int) -> None:
        """
        Drops an index of a specified column.

        Warning: deletes the data of the column's index from disk.
        """
        with self.latch:
            if not self.__does_index_filename_exist(column_index):
                raise FileNotFoundError
            if not self.__is_index_in_indices(column_index):
                raise KeyError
            if self.__is_index_key(column_index):
                raise ValueError
            assert column_index in self.indices
            del self.indices[column_index]
            os.remove(self.__get_column_index_filename(column_index))

    def insert(self, record_columns:tuple, rid:RID) -> None:
        """
        Adds record information to the created index columns.
        """
        with self.latch:
            self.__check_num_columns_valid(record_columns)
            for i, record_entry_value in enumerate(record_columns):
                if i in self.indices:
                    self.indices[i].add_value(record_entry_value, rid)

    def delete(self, record_columns:tuple, rid:RID) -> None:
        """
        Deletes record information from the created index columns.
        """
        with self.latch:
            self.__check_num_columns_valid(record_columns)
            for i, record_entry_value in enumerate(record_columns):
                if i in self.indices:
                    self.indices[i].delete_value(record_entry_value, rid)

    def locate(self, entry_value, column_index: int) -> set[RID]:
        """
        Returns the location of all records with the given value
        within a specified column.
        """
        with self.latch:
            if not column_index in self.indices:
                raise KeyError
            return self.indices[column_index].get_single_entry(entry_value)

    def locate_range(self, begin, end, column_index: int) -> set[RID]:
        """
        Returns the RIDs of all records with values in a specified column
        between "begin" and "end" (bounds-inclusive).
        """
        with self.latch:
            if not column_index in self.indices:
                raise KeyError
            return self.indices[column_index].get_ranged_entry(begin, end)

    def update(
        self, old_entries:tuple, new_entries:tuple, rid: int)->None:
        """
        Updates an RID-associated entry value.
        """
        with self.latch:
            for i in range(len(new_entries)):
                if new_entries[i] != None and new_entries[i] != old_entries[i] and i in self.indices:
                    self.indices[i].update_value(old_entries[i], new_entries[i], rid)
