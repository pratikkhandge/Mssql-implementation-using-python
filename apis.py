"""
Description: Module used to handle tinydb basic apis for tinydb basic operation
"""
__author__ = "pratik khandge"
__copyright__ = ""
__credits__ = ["pratik khandge"]
__license__ = ""
__version__ = "0.1"
__maintainer__ = "pratik"
__email__ = "pratik.khandge@gmail.com"
__status__ = "Developement"

# Python imports
import pymssql


class MsSqlConn:
    """
    Class to handle MsSQL database operations
    """

    def __init__(self, connection_obj, database=None):
        """
        Initialise database instance
        """
        try:
            self.conn = pymssql.connect(server=connection_obj['HOST'], user=connection_obj['USER'],
                                        password=connection_obj['PASSWORD'],
                                        database=database if database else connection_obj['DATABASE'])
            self.cursor = self.conn.cursor()
        except Exception as e:
            raise Exception("Could not connect to mssql Exception {}:".format(e))

    def __del__(self):
        if self.cursor is not None:
            self.cursor.close()
        if self.conn is not None:
            self.conn.close()

    def fetch_one_record(self, sql):
        """
        Get one records.
        """
        try:
            doc = {}
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
            if row is None:
                return doc
            desc = self.cursor.description
            for (name, value) in zip(desc, row):
                doc[name[0]] = value
            # end for
            return doc
        except Exception, e:
            raise Exception("Could not fetch record from mssql Exception {}:".format(e))

    def fetch_all_records(self, sql):
        """
        Get all records.
        """
        try:
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()
            if rows is None:
                return None
            desc = self.cursor.description
            result = []
            for row in rows:
                doc = {}
                for (name, value) in zip(desc, row):
                    doc[name[0]] = value
                result.append(doc)
            # end for
            return result
        except Exception, e:
            raise Exception("Could not fetch records from mssql Exception {}:".format(e))

    def execute(self, sql):
        """
        Execute SQL statement.
        """
        try:
            result = self.cursor.execute(sql)
            self.conn.commit()
        except Exception, e:
            result = None
            self.conn.rollback()
            raise Exception("Could not execute mssql query Exception {}:".format(e))
        finally:
            return result
