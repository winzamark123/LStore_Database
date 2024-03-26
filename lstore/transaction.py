from lstore.bufferpool import BUFFERPOOL
from lstore.table import Table


num_transactions = 0

class Transaction:

    def __init__(self):
        """
        Creates a transaction object.
        """
        global num_transactions
        self.id:int = num_transactions
        num_transactions += 1
        self.queries:list[tuple] = list() # [(query method, (args))]

    def add_query(self, query, table:Table, *args):
        """
        Adds the given query to this transaction
        
        Example:
        - q = Query(grades_table)
        - t = Transaction()
        - t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
        """
        self.queries.append((query, args))
        # use grades_table for aborting


    def run(self):
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    def abort(self):
        BUFFERPOOL.abort_writes_to_disk()
        return False

    def commit(self):
        BUFFERPOOL.commit_writes_to_disk()
        return True

