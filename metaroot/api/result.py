class Result:
    """
    A standard result wrapper to ensure uniformity of return types at the top level.
    """

    def __init__(self, status: int, response):
        """
        Initialize a new Result

        Parameters
        ----------
        status: int
            Numeric status of result (by convention 0 is success and all values >0 are errors)
        response: object
            An object composed of one or more primitives that constitutes a generic payload for data
        """
        self.status = status
        self.response = response

    def is_error(self):
        """
        Test if the status of the result indicates error (!= 0)

        Returns
        -------
        bool
            True for error, False for not error
        """
        return self.status != 0

    def is_success(self):
        """
        Test if the status of the result indicates success (== 0)

        Returns
        -------
        bool
            True for success, False for not success
        """
        return self.status == 0

    def to_transport_format(self):
        """
        Wraps the Result as a dictionary for encoding and network transport

        Returns
        -------
        dict
            All properties of the Result
        """
        return {"status": self.status, "response": self.response}

    @staticmethod
    def from_transport_format(obj: dict):
        """
        Instantiates a Result object form a dictionary (e.g., a Result that was converted to a dictionary for transport)

        Parameters
        ----------
        obj: dict
            A dictionary containing keys=values "status"=int and "response"=object

        Returns
        ----------
        Result
            A new instance of Result initialized from the dictionary
        """
        return Result(obj["status"], obj["response"])
