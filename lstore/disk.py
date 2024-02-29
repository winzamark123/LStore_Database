import os
import pickle
from lstore.physical_page import Physical_Page
import lstore.config as Config

class Disk:
    def __init__(self):
        self.root = None

    def __is_dir_under_root(self, dir_path:str)->bool:
        assert self.root != None, ValueError

        parent = os.path.abspath(self.root)
        child = os.path.abspath(dir_path)
        return os.path.commonpath([parent]) == os.path.commonpath([parent, child])

    def set_database(self, db_dir_path:str):
        self.root = db_dir_path
        if not os.path.exists(self.root):
            os.makedirs(self.root, exist_ok=True)
            print(f"Database initialized at {self.root}")
        print(f"Database from path {self.root} has been opened.")

    def create_path_directory(self, dir_path:str)->None:
        if os.path.exists(dir_path): raise ValueError
        if not self.__is_dir_under_root(dir_path): raise ValueError
        os.makedirs(dir_path)

    def write_metadata_to_disk(self, path_for_metadata:str, metadata:dict)->None:
        if not self.__is_dir_under_root(path_for_metadata): raise ValueError
        with open(os.path.join(path_for_metadata, "metadata.pkl"), 'wb') as mdf:
            pickle.dump(metadata, mdf)

    def read_metadata_from_disk(self, path_for_metadata)->dict:
        if not self.__is_dir_under_root(path_for_metadata): raise ValueError
        if not os.path.exists(path_for_metadata):
            raise ValueError
        
        metadata_file_path = os.path.join(path_for_metadata, "metadata.pkl")  # Corrected line
        if not os.path.exists(metadata_file_path):
            raise ValueError("Metadata file does not exist")

        with open(metadata_file_path, 'rb') as mdf:  # Corrected line
            return pickle.load(mdf)  # Corrected from pickle.loads to pickle.load     
        # with open(os.path.join(path_for_metadata), "metadata.pkl", 'rb') as mdf:
        #     print("PATH FOR META", path_for_metadata)
        #     return dict(pickle.loads(mdf))

    def write_physical_page_to_disk(self, path_to_physical_page:str, physical_page:Physical_Page)->None:
        if not self.__is_dir_under_root(path_to_physical_page): raise ValueError
        with open(os.path.join(path_to_physical_page, f"{physical_page.column_index}.bin"), 'wb') as ppf:
            ppf.write(physical_page.data)

    def read_physical_page_from_disk(self, path_to_physical_page:str)->Physical_Page:
        if not self.__is_dir_under_root(path_to_physical_page): raise ValueError
        if not os.path.exists(path_to_physical_page):
            raise ValueError
        with open(path_to_physical_page, 'rb') as ppf:
            return ppf.read(Config.PHYSICAL_PAGE_SIZE)

DISK = Disk()

"""
note: 4 metadata columns per base/tail page

Database Directory
    Table 1 Directory (w/ 5 columns)
        metadata.pkl -> {table_dir_path, num_columns, key_index, num_page_ranges}
        PR1
            metadata.pkl -> {page_range_path, num_base_pages, num_tail_pages}
            BP1 (512 records in base page -> 1 physical page per column)
                metadata.pkl -> {base_page_path, num_records, page_index}
                0.pp
                1.pp
                ...
                8.pp
            TP1 (potentially infinite # records)
                1.pp
                2.pp
                ...
                9.pp
            BP2
            TP2
    Table Directory
        PR
            BP1
                1.pp
                ...
                10.bin
"""