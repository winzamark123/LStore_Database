from lstore.bufferpool import BUFFERPOOL
from lstore.table import Table
from lstore.record import RID
from lstore.record import Record


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table: Table):
        self.table = table
        self.inserted_keys = {}

    # SWITCH THE RID TO 0
    def delete(self, primary_key) -> bool:
        """
        # internal Method
        # Read a record with specified RID
        # Returns True upon succesful deletion
        # Return False if record doesn't exist or is locked due to 2PL
        """

        rids = self.table.index.locate(primary_key, self.table.key_index)

        for rid in rids:
            rid = RID(rid=rid)
            self.table.delete_record(rid)

        return True

        # TODO: implement TPL record locking

    def insert(self, *columns: tuple) -> bool:
        """
        # Insert a record with specified columns
        # Return True upon succesful insertion
        # Returns False if insert fails for whatever reason
        """
        try:
            self.table.insert_record(self.table.create_record(columns))
        except ValueError:
            return False
        else:
            return True

    def select(
        self, search_key: int, search_key_index: int, projected_columns_index: list
    ) -> list[Record]:
        """
        # Read matching record with specified search key
        # :param search_key: the value you want to search based on
        # :param search_key_index: the column index you want to search based on
        # :param projected_columns_index: what columns to return. array of 1 or 0 values.
        # Returns a list of Record objects upon success
        # Returns False if record locked by TPL
        # Assume that select will never be called on a key that doesn't exist
        """
        rids = self.table.index.locate(search_key, search_key_index)

        records_list = list()
        try:
            for rid in rids:
                rid = RID(rid=rid)
                data_columns = self.table.select(rid=rid)

                filtered_list = [
                    data_columns[i]
                    for i in range(len(data_columns))
                    if projected_columns_index[i] == 1
                ]

                # print("FILTERED", filtered_list)
                filtered_record = Record(
                    rid=rid, key=self.table.key_index, columns=filtered_list
                )
                records_list.append(filtered_record)
        except ValueError:
            return False

        return records_list

        # TODO: implement TPL record locking

    def select_version(
        self,
        search_key: int,
        search_key_index: int,
        projected_columns_index: list[int],
        relative_version: int,
    ) -> list[Record]:
        """
        # Read matching record with specified search key
        # :param search_key: the value you want to search based on
        # :param search_key_index: the column index you want to search based on
        # :param projected_columns_index: what columns to return. array of 1 or 0 values.
        # :param relative_version: the relative version of the record you need to retreive.
        # Returns a list of Record objects upon success
        # Returns False if record locked by TPL
        # Assume that select will never be called on a key that doesn't exist
        """

        # [0 0 0 0 0] Base Record
        # [1 1 1 1 1] Tail Record
        # [2 2 2 2 2] Tail Record (Up to date)

        # select_verion(-1)
        # [1 1 1 1 1]

        rids = self.table.index.locate(search_key, search_key_index)

        records_list = list()
        # try:
        for rid in rids:
            rid = RID(rid=rid)
            data_columns = self.table.select_version(
                rid=rid, roll_back=relative_version
            )

            print("DATA COLUMNS", data_columns)

            filtered_list = [
                data_columns[i]
                for i in range(len(data_columns))
                if projected_columns_index[i] == 1
            ]

            filtered_record = Record(
                rid=rid, key=self.table.key_index, columns=filtered_list
            )
            records_list.append(filtered_record)
        # except ValueError:
        #     return False

        return records_list

    def update(self, primary_key, *columns) -> bool:
        """
        # Update a record with specified key and columns
        # Returns True if update is succesful
        # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
        """
        none_count = 0
        for i in range(len(columns)):
            if columns[i] is None:
                none_count += 1
            if none_count is self.table.num_columns:
                return True

        # print("\n\nUpdate starting")
        rid = self.table.index.locate(primary_key, self.table.key_index)
        rid = RID(rid=list(rid)[0])

        try:
            self.table.update_record(rid=rid, updated_column=columns)
        except ValueError:  # TODO: TPL record locking
            return False
        else:
            return True

    def sum(self, start_range, end_range, aggregate_column_index) -> int:
        """
        :param start_range: int         # Start of the key range to aggregate
        :param end_range: int           # End of the key range to aggregate
        :param aggregate_columns: int  # Index of desired column to aggregate
        # this function is only called on the primary key.
        # Returns the summation of the given range upon success
        # Returns False if no record exists in the given range
        """

        rids = self.table.index.locate_range(
            start_range, end_range, self.table.key_index
        )
        output = 0
        try:
            for rid in rids:
                rid = RID(rid=rid)
                data_columns = self.table.select(rid)
                output += data_columns[aggregate_column_index]
        except ValueError:
            return False

        return output

    def sum_version(
        self, start_range, end_range, aggregate_column_index, relative_version
    ):
        """
        :param start_range: int         # Start of the key range to aggregate
        :param end_range: int           # End of the key range to aggregate
        :param aggregate_columns: int  # Index of desired column to aggregate
        :param relative_version: the relative version of the record you need to retreive.
        # this function is only called on the primary key.
        # Returns the summation of the given range upon success
        # Returns False if no record exists in the given range
        """
        rids = self.table.index.locate_range(
            start_range,
            end_range,
            self.table.key_index,
        )
        output = 0
        for rid in rids:
            rid = RID(rid=rid)
            data_columns = self.table.select_version(rid, relative_version)
            output += data_columns[aggregate_column_index]

        return output

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        # TODO: idk anything abt this
        r = self.select(key, self.table.key_index, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
