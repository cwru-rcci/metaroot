from metaroot.api.result import Result
from metaroot.api.notifications import send_email
from metaroot.config import get_config

config = get_config("REACTIONS")


class DefaultReactions:
    @staticmethod
    def occur_in_response_to(clazz: str, action: str, payload: object, result: Result):
        if result.is_error():
            send_email(config.get("METAROOT_REACTION_NOTIFY"),
                       "metaroot operation failed",
                       "<table>" +
                       "<tr><td>Class</td><td>"+clazz+"</td></tr>"
                       "<tr><td>Action</td><td>"+action+"</td></tr>" +
                       "<tr><td>Payload</td><td>"+str(payload)+"</td></tr>" +
                       "<tr><td>Result Status</td><td>"+str(result.status)+"</td></tr>" +
                       "<tr><td>Result Payload</td><td>"+str(result.response)+"</td></tr>" +
                       "</table>")
