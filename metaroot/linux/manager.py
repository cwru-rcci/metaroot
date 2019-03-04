import subprocess
import metaroot.utils
from metaroot.common import Result


class LinuxManager:
    """
    Implements methods to manage users and groups on Linux servers
    """

    def __init__(self, logfile="messages.log"):
        """
        Override default init to accept an optional log file name if we want to log to an alternative location

        Parameters
        ----------
        logfile : str
            A log file path (default "messages.log")
        """
        self._logger = metaroot.utils.get_logger("LinuxManager", logfile)

    # Runs the argument command and returns the exist status. Attempts to suppress all output.
    def __run_cmd__(self, cmd: str):
        args = cmd.split()
        cp = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if cp.returncode != 0:
            self._logger.error("CMD: {0}".format("\"{0}\"".format(cmd)))
            self._logger.error("STDOUT: {0}".format(cp.stdout.decode("utf-8")))
            self._logger.error("STDERR: {0}".format(cp.stderr.decode("utf-8")))
            self._logger.error("Command failed with exit status %d", cp.returncode)
        else:
            self._logger.debug("\"{0}\" returned {1}".format(cmd, cp.returncode))

        return cp.returncode

    def add_group(self, group_name: str, attributes: dict) -> Result:
        status = self.__run_cmd__("")
        return Result(status, None)

    def exists_group(self, group_name: str) -> Result:
        return Result(1, None)

    def delete_group(self, group_name: str) -> Result:
        return Result(1, None)

    def add_user(self, user_name: str, user_id: str, first_name: str, last_name: str, group_id: str) -> Result:
        return Result(1, None)

    def add_user_to_group(self, user_name: str, group_name: str) -> Result:
        return Result(1, None)

    def remove_user_from_group(self, user_name: str, group_name: str) -> Result:
        return Result(1, None)

    def change_user_primary_group(self, user_name: str, group_name: str) -> Result:
        return Result(1, None)

    def exists_user(self, user_name: str) -> Result:
        return Result(1, None)

    def delete_user(self, user_name: str) -> Result:
        return Result(1, None)
