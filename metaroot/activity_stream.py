import os
import sqlite3
import datetime
from metaroot.config import get_config
from metaroot.common import Result


class ActivityStream:
    """
    A class that encapsulates recording events to database for administrative review or intervention.
    """

    # Static attributes for event types
    ERROR = 0
    WARN = 1
    INFO = 2

    def __init__(self):
        """
        Initialize a new activity stream. Creates the database if it does not exist.
        """
        config = get_config(self.__class__.__name__)

        db_file = config.get_activity_stream_db()
        need_to_create_tables = True
        if os.path.exists(db_file):
            need_to_create_tables = False

        # If the database does not exist, create it
        self._conn = sqlite3.connect(db_file)
        if need_to_create_tables:
            self._conn.execute('''CREATE TABLE events (eventtime timestamp, type integer, action text, arguments text, status integer, message text)''')
            self._conn.commit()

    def __del__(self):
        """
        Commits any pending changes and closes database connection.
        """
        self._conn.commit()
        self._conn.close()

    def _insert(self, values: tuple) -> bool:
        """
        Inserts a new event to the database
        Parameters
        ----------
        values: tuple
            six values in the correct order to bind placeholders

        Returns
        -------
        True
            Always returns True
        """
        self._conn.execute('INSERT INTO events VALUES(?,?,?,?,?,?)',
                           values)
        self._conn.commit()
        return True

    def info(self, action: str, params: object) -> bool:
        """
        Add an informational entry to the database

        Parameters
        ----------
        action : str
            A unique identifier for the action, usually ${method_name}:${class name}
        params : object
            The arguments to the method as scalar, list or dict

        Returns
        ---------
        Result
            True for success

        Raises
        ---------
        Exception
            if the database if an underlying operation raised an exception
        """
        return self._insert((datetime.datetime.now(),
                             ActivityStream.INFO,
                             action,
                             str(params),
                             0,
                              ""))

    def error(self, action: str, params: object, result: Result) -> bool:
        """
        Add an error entry to the database

        Parameters
        ----------
        action : str
            A unique identifier for the action, usually ${method_name}:${class name}
        params : object
            The arguments to the method as scalar, list or dict
        result: Result
            The Result of the failed operation that contains more granular information about the error

        Returns
        ---------
        Result
            True for success

        Raises
        ---------
        Exception
            if the database if an underlying operation raised an exception
        """
        return self._insert((datetime.datetime.now(),
                             ActivityStream.ERROR,
                             action,
                             str(params),
                             result.status,
                             str(result.response)))

    def record(self, action: str, params: object, result: Result) -> bool:
        """
        Adds an entry to the database as info if result.is_success() and as error otherwise

        Parameters
        ----------
        action : str
            A unique identifier for the action, usually ${method_name}:${class name}
        params : object
            The arguments to the method as scalar, list or dict
        result: Result
            The Result of the operation

        Returns
        ---------
        Result
            True for success

        Raises
        ---------
        Exception
            if the database if an underlying operation raised an exception
        """
        if result.is_success():
            return self.info(action, params)
        else:
            return self.error(action, params, result)
