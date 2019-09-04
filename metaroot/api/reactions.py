from metaroot.api.result import Result
from metaroot.api.notifications import send_email
from metaroot.config import get_config


config = get_config("DEFAULTREACTIONS")


class DefaultReactions:
    """
    Reactions are defined to occur relative to the result of an action. In the standard deployment, they are applied by
    the router after each action. The default reaction is to send an email any time an error is generated.
    """

    @staticmethod
    def occur_in_response_to(clazz: str, action: str, payload: object, result: Result, n_priors: int) -> int:
        """
        Evaluates the result of an action and performs additional actions as necessary

        Parameters
        ----------
        clazz: str
            The class name that implemented the method handling the action
        action: str
            The name of the method implemented by clazz that was called
        payload: object
            The argument that were passed to the method
        result: Result
            The result of the method call
        n_priors: int
            A value indicating the number of reactions that have occurred during the requested operation. I.e., this
            value is set to 0 as the router begins calling methods of each manager implementing the current request
            action, and it increases by one each time a Result from a manager operation triggers a reaction.
        """
        global config

        if result.is_error():
            send_email(config.get("REACTION_NOTIFY"),
                       "metaroot operation failed",
                       "<table>" +
                       "<tr><td>Class</td><td>"+clazz+"</td></tr>"
                       "<tr><td>Action</td><td>"+action+"</td></tr>" +
                       "<tr><td>Payload</td><td>"+str(payload)+"</td></tr>" +
                       "<tr><td>Result Status</td><td>"+str(result.status)+"</td></tr>" +
                       "<tr><td>Result Payload</td><td>"+str(result.response)+"</td></tr>" +
                       "</table>")
        return 0
