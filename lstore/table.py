import os
from copy import deepcopy
from threading import RLock

from lstore.disk import Disk
from lstore.lock_info import Lock_Manager
from lstore.record_info import Record, RID
from lstore.page_info import Page_Range
from lstore.index import Index


class Table:

    def __init__(self, table_path:str, num_columns:int, key_index:int, num_records:int)->None:
        self.table_path:str                   = table_path
        self.num_columns:int                  = num_columns
        self.key_index:int                    = key_index
        self.num_records:int                  = num_records

        self.index:Index                      = Index(self.table_path, self.num_columns, self.key_index)
        self.lock_manager:Lock_Manager        = Lock_Manager()
        self.latch:RLock                      = RLock()

        self.page_ranges:dict[int,Page_Range] = dict()
        self.__load_page_ranges()

    def __del__(self)->None:
        # delete page ranges
        del self.page_ranges
        self.page_ranges = None
        # delete index
        del self.index
        self.index = None

    def __increment_num_records(self)->None:
        # increment number of records in memory
        self.num_records += 1

        # increment number of records in data
        metadata = Disk.read_from_path_metadata(self.table_path)
        metadata["num_records"] = self.num_records
        Disk.write_to_path_metadata(self.table_path, metadata)

    def __get_page_ranges(self)->tuple[list[str],int]:
        page_range_dirs = Disk.list_directories_in_path(self.table_path)
        return (page_range_dirs, len(page_range_dirs))

    def __load_page_ranges(self):
        """
        Load page ranges from disk.
        """
        with self.latch:
            page_range_paths, num_page_ranges = self.__get_page_ranges()
            if not num_page_ranges: return
            for page_range_path in page_range_paths:
                page_range_index = int(os.path.basename(page_range_path).removeprefix("PR"))
                metadata = Disk.read_from_path_metadata(page_range_path)
                self.page_ranges[page_range_index] = Page_Range(
                    metadata["page_range_path"],
                    metadata["page_range_index"],
                    metadata["latest_tid"],
                    metadata["tps_index"],
                )

    def __create_page_range(self, page_range_index:int):
        page_range_path = os.path.join(self.table_path, f"PR{page_range_index}")
        if os.path.exists(page_range_path): raise FileExistsError
        Disk.create_path_directory(page_range_path)
        metadata = {
            "page_range_path": page_range_path,
            "page_range_index": page_range_index,
            "latest_tid": 0,
            "tps_index": 0,
        }
        Disk.write_to_path_metadata(page_range_path, metadata)
        self.page_ranges[page_range_index] = Page_Range(
            metadata["page_range_path"],
            metadata["page_range_index"],
            metadata["latest_tid"],
            metadata["tps_index"],
        )

    def __access_page_range(self, page_range_index:int)->None:
        with self.latch:
            if not page_range_index in self.page_ranges:
                self.__create_page_range(page_range_index)

    def __get_columns(self, rid:RID, rollback_version:int=0)->tuple:
        with self.latch:
            self.__access_page_range(rid.get_page_range_index())
            return self.page_ranges[rid.get_page_range_index()].get_record_columns(rid, rollback_version)

    def insert_record(self, columns:tuple)->None:
        """
        Insert record to table.
        """
        # lock RID
        while not self.lock_manager.acquire_write(RID(self.num_records + 1).get_page_range_index()): pass

        # create RID from num_records (base RID starts at 1)
        rid = RID(self.num_records + 1)

        # perform checks that may cause operation to be aborted
        try:
            # number of columns in inserted column is wrong
            if len(columns) != self.num_columns: raise Exception
            # key already exists in table
            if len(self.index.locate(columns[self.key_index], self.key_index)): raise Exception
        except Exception:
            return False
        else:
            # create record
            record = Record(rid, self.key_index, columns)
            # insert to index
            self.index.insert(record.get_columns(), rid)
            # insert to physical disk
            self.__access_page_range(record.get_page_range_index())
            self.page_ranges[record.get_page_range_index()].insert_record(record)
            # apply new num_records to table's metadata
            self.__increment_num_records()
        finally:
            self.lock_manager.release_write(rid.get_page_range_index())

    def select_record(self, search_key, search_key_index:int, selected_columns:list=None, rollback_version:int=0)->list[Record]:
        """
        Select Record from Table
        """
        rlist = list()

        # get specific RIDs from index
        try:
            rids = self.index.locate(search_key, search_key_index)
        # if no index available, conduct full table scan
        except KeyError:
            rids = {RID(i) for i in range(1, self.num_records + 1)}

        # construct a list of records
        for rid in rids:
            # lock RID
            while not self.lock_manager.acquire_read(rid.get_page_range_index(), search_key): pass

            try:
                # access column values from disk
                columns = self.__get_columns(rid, rollback_version)
                # conditional that avoids creating records for non-searched info (only really useful for full table scans)
                if columns[search_key_index] != search_key: continue
                # construct record and add to records list
                if selected_columns != None:
                    if len(columns) != len(selected_columns): raise Exception
                    columns = tuple([_ for i, _ in enumerate(columns) if selected_columns[i] == 1])
                rlist.append(Record(rid, self.key_index, columns))
            except Exception:
                return False
            finally:
                self.lock_manager.release_read(rid.get_page_range_index())

        return rlist

    def sum_records(self, start_range, end_range, aggregate_column_index:int, rollback_version:int=0)->int:
        """
        Sum Records from Table
        """
        rsum = 0

        # get RIDs
        try:
            rids = self.index.locate_range(start_range, end_range, self.key_index)
        except KeyError:
            rids = {RID(i) for i in range(1, self.num_records + 1)}

        for rid in rids:
            # lock RID
            while not self.lock_manager.acquire_read(rid.get_page_range_index()): pass

            try:
                # access column from disk
                columns = self.__get_columns(rid, rollback_version)
                rsum += columns[aggregate_column_index]
            except Exception:
                return False
            finally:
                self.lock_manager.release_read(rid.get_page_range_index())

        return rsum

    def update_record(self, primary_key, new_columns:tuple)->bool:
        """
        Update Record from Table
        """
        # identify RID
        rids = self.index.locate(primary_key, self.key_index)
        if len(rids) == 0: return
        assert len(rids) == 1
        rid = rids.pop()

        # lock RID
        while not self.lock_manager.acquire_write(rid.get_page_range_index()): pass

        # perform checks that may abort the operation
        try:
            # get old columns associated to RID
            old_columns = deepcopy(self.__get_columns(rid))
            if len(old_columns) != len(new_columns): raise Exception

            # only update if the new columns are changing values in the record
            is_update_necessary = False
            for i in range(len(new_columns)):
                if new_columns[i] != None and new_columns[i] != old_columns[i]:
                    is_update_necessary = True
            if not is_update_necessary: return True
        except Exception:
            return False
        else:
            # update entry values associated to RID in index
            self.index.update(old_columns, new_columns, rid)
            # update record in disk
            # print(f"UPDATING KEY {primary_key} OF RID {rid} AND ORIGINAL COLUMNS {old_columns} WITH NEW COLUMNS {new_columns}")
            self.__access_page_range(rid.get_page_range_index())
            self.page_ranges[rid.get_page_range_index()].update_record(rid, old_columns, new_columns)
            return True
        finally:
            self.lock_manager.release_write(rid.get_page_range_index())

    def delete_record(self, primary_key)->bool:
        """
        Delete Record from Table
        """
        rids = self.index.locate(primary_key, self.key_index)
        assert len(rids) == 1
        rid = rids.pop()

        # lock RID
        while not self.lock_manager.acquire_write(rid.get_page_range_index()): pass

        try:
            # delete record from index
            columns = self.__get_columns(rid)
        except Exception:
            return False
        else:
            # delete info associated to RID in index
            self.index.delete(columns, rid)
            # delete record from disk
            self.__access_page_range(rid.get_page_range_index())
            self.page_ranges[rid.get_page_range_index()].delete_record(rid)
            return True
        finally:
            self.lock_manager.release_write(rid.get_page_range_index())
