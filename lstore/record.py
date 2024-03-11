""" record module """

import lstore.config as Config


class RID:
    """RID class to be able to get indexes"""

    def __init__(self, rid: int) -> None:
        self.rid = rid

    def __int__(self) -> int:
        return self.rid

    def get_page_range_index(self):
        """get the index of the page range in the database"""
        return (abs(self.rid) - 1) // (Config.RECORDS_PER_PAGE * Config.NUM_BASE_PAGES)

    def get_base_page_index(self):
        """get the index of the base page in the page range"""
        return ((abs(self.rid) - 1) // Config.RECORDS_PER_PAGE) % Config.NUM_BASE_PAGES

    def get_pp_index(self):
        """get the index of the pp in the base page"""
        return self.rid % Config.RECORDS_PER_PAGE

    def to_int(self):
        """turns rid to int"""
        return self.rid

    def get_tail_page_index(self):
        """get the index of the tail page in the page range"""
        return (abs(self.rid) - 1) // Config.RECORDS_PER_PAGE


class Record:
    """Record class to represent a record in the database."""

    def __init__(self, rid: int, key: int, columns: tuple) -> None:
        self.rid = RID(rid)
        self.key = key
        self.columns = columns

    def get_key(self):
        return self.key

    def get_values(self):
        return (self.key,) + tuple(self.columns)
        # 123123, 0, 0 ,0, 0

    def get_page_range_index(self):
        return self.rid.get_page_range_index()

    def get_base_page_index(self):
        return self.rid.get_base_page_index()

    def get_rid(self):
        return self.rid.rid

    def get_columns(self):
        return self.columns
