from lstore.index import Index
from lstore.physical_page import Physical_Page
import lstore.config as Config
from lstore.page_range import Page_Range
from lstore.bufferpool import Bufferpool
from lstore.disk import DISK
from lstore.record import Record, RID
import os
import threading
import copy
 
class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns
    :param key: int             #Index of table key in columns
    """

    def __init__(self, table_dir_path:str, num_columns:int, key_index:int, num_records:int)->None:
        self.table_dir_path:str    = table_dir_path
        self.num_columns:int       = num_columns 
        self.key_index:int         = key_index
        # self.key_column:int        = Config.META_DATA_NUM_COLUMNS + key_index
        self.num_records:int       = num_records

        self.index:Index           = Index(table_dir_path=table_dir_path, num_columns=num_columns, primary_key_index=key_index, order=Config.ORDER_CHOICE)

        self.page_ranges:dict[int,Page_Range] = dict()
        if self.__get_num_page_ranges():
            self.page_ranges = self.load_page_ranges()

    def __increment_num_records(self)->int:
        self.num_records += 1
        return self.num_records

    def __get_num_page_ranges(self)->int:
        count = 0
        for _ in os.listdir(self.table_dir_path):
            if _ == "index": continue
            elif os.path.isdir(os.path.join(self.table_dir_path, _)):
                count += 1
            

        print(f'Page Ranges {count}')
        return count

    def load_page_ranges(self)->dict[int,Page_Range]:
        page_range_dirs = [
            os.path.join(self.table_dir_path, _) for _ in os.listdir(self.table_dir_path)
            if os.path.isdir(os.path.join(self.table_dir_path, _))
        ]
        for page_range_dir in page_range_dirs:
            page_range_index = int(page_range_dir[page_range_dir.rfind('/')].removeprefix("PR"))
            metadata = DISK.read_metadata_from_disk()
            self.page_ranges[page_range_index] = \
                Page_Range(
                    page_range_dir_path=metadata["page_range_dir_path"],
                    page_range_index=metadata["page_range_index"],
                    tps_index=metadata["tps_index"]                    
                )

    def create_page_range(self, page_range_index:int)->None:
        """
        Creates a page range directory in disk
        """
        page_range_dir_path = os.path.join(self.table_dir_path, f"PR{page_range_index}")
        if os.path.exists(page_range_dir_path):
            raise ValueError

        DISK.create_path_directory(page_range_dir_path)
        metadata = {
            "page_range_dir_path": page_range_dir_path,
            "page_range_index": page_range_index,
            "tps_index": 0,
        }
        DISK.write_metadata_to_disk(page_range_dir_path, metadata)
        self.page_ranges[page_range_index] = Page_Range(page_range_dir_path=page_range_dir_path, page_range_index=page_range_index, tps_index=0)

    def create_record(self, columns:tuple)->Record:
        """
        Create a record.
        """
        print("CREATE RECORD", columns)

        if len(columns) != self.num_columns:
            raise ValueError
        
        print("CREATE RECORD")
        return Record(rid=self.__increment_num_records(), key=columns[self.key_index], columns=columns)

    def insert_record(self, record:Record)->None:
        """
        Insert record into table.
        """
        
        # checks if a page range is available
        if self.__get_num_page_ranges() == 0 or not record.get_page_range_index() in self.page_ranges:
            self.create_page_range(self.__get_num_page_ranges())

        #insert record to index
        self.index.insert_record_to_index(record_columns=record.get_columns(), rid=record.get_rid())

        # insert record to page range
        self.page_ranges[record.get_page_range_index()].insert_record(record)


    def get_data(self, rid:RID)->tuple:
        """
        Get data for record from table.
        """

        if not rid.get_page_range_index() in self.page_ranges:
            raise ValueError
        return self.page_ranges[rid.get_page_range_index()].get_data(rid)

    def update_record(self, rid:RID, updated_column:tuple)->None:
        """
        Update record from table.
        """

        if not rid.get_page_range_index() in self.page_ranges:
            raise ValueError
        
        self.page_ranges[rid.get_page_range_index()].update_record(rid, updated_column)

    def delete_record(self, rid:RID)->None:
        """
        Delete record from table.
        """

        if not rid.get_page_range_index() in self.page_ranges:
            raise ValueError
        
        self.page_ranges[rid.get_page_range_index()].delete_record(rid)

    # Get Page Range and Base Page from RID
    def get_list_of_addresses(self, rids)-> list:
        addreses = []

        for rid in rids:
            rid -= 1
            page_range_num = rid // (Config.RECORDS_PER_PAGE * Config.NUM_BASE_PAGES)
            base_page_num = (rid // Config.RECORDS_PER_PAGE) % Config.NUM_BASE_PAGES
            addreses.append((page_range_num, base_page_num))
        #return page_range_num, base_page_num
        return addreses

    # TODO: complete merge (in the works: Testing in merge_test.py)
    # Implementing without bufferpool right now, will when bufferpool is finished
    @staticmethod
    def __merge(page_range:Page_Range):
        print("\nMerge starting!!")
        print(f"TPS before merge: {page_range.tps_range}")
        # records that need updating in base page
        updated_records = {}

        target_page_num = 1

        # checking until what tail pages to merge
        if page_range.tps_range != 0:
            target_page_num = page_range.get_page_number(page_range.tps_range)
            print(f'Tail page to stop iterating: {page_range.get_page_number(page_range.tps_range)}')

        # iterate backwards through tail pages list
        for i in range(len(page_range.tail_pages) - 1, -1, -1):
            # Access the Tail_Page object using the current index
            tail_page = page_range.tail_pages[i]

            # Check if the current page number matches the target page number
            if tail_page.page_number >= target_page_num:
                print(f"MergingTail Page: {tail_page.page_number} ")
                print(f"Found tail page {tail_page.page_number}")

                # base_rid and tid page in tail page
                base_rid_page = tail_page.get_base_rid_page()
                tid_page = tail_page.physical_pages[1]

                # jumps straight to the end of the tail page (TODO: make it jump to very last tail record even if tail page isn't full)
                for i in range(Config.PHYSICAL_PAGE_SIZE - Config.COLUMN_SIZE, -1, -Config.COLUMN_SIZE):

                    tid_for_tail_record_bytes = tid_page.data[i:i+Config.COLUMN_SIZE]

                    tid_for_tail_record_value = -(int.from_bytes(tid_for_tail_record_bytes, byteorder='big', signed=True))

                    # continue through loop meaning it's at a tail record that hasn't been set
                    if tid_for_tail_record_value == 0:
                        continue

                    # Extract 8 bytes at a time using slicing
                    base_rid_for_tail_record_bytes = base_rid_page.data[i:i+Config.COLUMN_SIZE]

                    base_rid_for_tail_record_value = int.from_bytes(base_rid_for_tail_record_bytes, byteorder='big', signed=True)

                    if -tid_for_tail_record_value < page_range.tps_range:
                        print(f"TPS for range : {page_range.tps_range} and TID: {-tid_for_tail_record_value}")

                        # adds rid if rid is not in update_records dictionary - used to know what base pages to use
                        if base_rid_for_tail_record_value not in updated_records.values():
                            updated_records[-(tid_for_tail_record_value)] = base_rid_for_tail_record_value

            # rids to base_pages
            rid_to_base = {}

            # base_page numbers
            base_pages_to_get = []

            for value in updated_records.values():
                base_page_num = page_range.get_page_number(value)
                rid_to_base[value] = base_page_num

                if base_page_num not in base_pages_to_get:
                    base_pages_to_get.append(base_page_num)

        # sorts list
        base_pages_to_get.sort()
        print(base_pages_to_get)

        i = 0
        # iterate through base pages in page range to find the base pages we are merging
        for base_page in page_range.base_pages:
            if i < len(base_pages_to_get) and base_page.page_number == base_pages_to_get[i] and base_page.num_records == Config.RECORDS_PER_PAGE:
                print(f"Base page we're in : {base_page.page_number}")
                print(f"rid_to_base: {rid_to_base}")

                # keeps track of what columns were updated in this base page
                updated_columns = []

                # iterate through rids that are updated and their corresponding base page
                for key, value in rid_to_base.items():
                    if value == base_page.page_number:
                        print(f"value: {value}")
                        schema_encoding_for_rid = base_page.check_base_record_schema_encoding(key)

                        # let's us know what columns have been updated
                        update_cols_for_rid = page_range.analyze_schema_encoding(schema_encoding_for_rid)

                        # grabs updated values
                        for column in update_cols_for_rid:
                            # updated values of specific columns
                            updated_val = page_range.return_column_value(key,column)

                            print(f'schema encoding: {base_page.check_base_record_schema_encoding(key)} : {update_cols_for_rid} -> [{column} : {updated_val}]  -> RID : {key} -> Page_Num {base_page.page_number}')

                            # updates column for record
                            base_page.physical_pages[Config.META_DATA_NUM_COLUMNS + column].write_to_physical_page(value=updated_val, rid=key, update=True)

                            if column not in updated_columns:
                                updated_columns.append(column)

                            # print(f"Updated grade column ({column}) in base page {base_page.page_number} at RID: {key}\n")

                        # merge_updated_list.append(key)

                # iterates to see what columns to read to disk if they've been modified
                # for column in updated_columns:
                    # __merge_update_to_buffer(base_page.physical_pages[Config.META_DATA_NUM_COLUMNS + column], base_page.page_number)
                # print("Physical pages saved")

                i += 1

            # last record merged
            page_range.tps_range = next(iter(updated_records))
    print("Merge finished")

    # TODO: takes buffer pool that it's going to use, since merge has it's own buffer pool so it won't interfere with the main thread
    @staticmethod
    def __merge_update_to_buffer(physical_page:Physical_Page, base_page_number:int):
        print(f'Physical Page : {physical_page.column_number} for Base Page ({base_page_number})')

        pass

    # checks if merging needs to happen
    def _merge_checker(self, page_range_num):
        if self.page_range_directory[page_range_num].num_updates % Config.MERGE_THRESHOLD == 0:
            # creates deep copy of page range
            page_range_copy = copy.deepcopy(self.page_range_directory[page_range_num])
            merging_thread = threading.Thread(target=self.__merge())
            merging_thread.start()

    def close(self):
        """
        Writes all of the frames in the bufferpool to the disk.
        """
        #TODO
        pass