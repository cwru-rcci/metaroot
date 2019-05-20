import email.message
import smtplib
import re
from metaroot.config import get_config, get_global_config
from metaroot.utils import get_logger, instantiate_object_from_class_path

global_config = get_global_config()
logger = get_logger(__name__,
                    global_config.get_log_file(),
                    global_config.get_file_verbosity(),
                    global_config.get_screen_verbosity())


class DefaultEmailAddressResolver:
    """
    Performs a very basic validation that the argument user name looks like a valid email address using a regular
    expression. If 'x' is one or more chars that does not contain @, the regex tests for strings that match x@x.x
    """
    @staticmethod
    def resolve_to_email_address(user_name: str) -> str:
        # The user name looks like it is already an email address (e.g., ?@?.?)
        # Regex from https://stackoverflow.com/a/8022584/3357118
        if re.fullmatch(r"[^@]+@[^@]+\.[^@]+", user_name):
            return user_name
        else:
            raise Exception("The user name {0} does not appear ot be an email address, and no resolver is configured".format(user_name))


def send_email(recipient_user_name: str, subject: str, body: str):
    """
    Method to send an email notification with an HTML body

    Parameters
    ----------
    recipient_user_name: str
        The user name that should receive the notification. This is resolved to an email address using either the
        builtin default resolver, or with a custom resolver specified in the SMTP configuration
    subject: str
        Subject of the email
    body: str
        Body of the email

    Returns
    -------
    True
        If the email was sent
    False
        If the email could not be sent

    """
    try:

        try:
            config = get_config("NOTIFICATIONS")
        except Exception as e:
            config = None

        if config is not None and config.has("SMTP_SERVER") and config.has("SMTP_FROM"):
            # Resolve/validate the recipient email adress
            resolver = DefaultEmailAddressResolver()
            if config.has("SMTP_ADDRESS_RESOLVER"):
                resolver = instantiate_object_from_class_path(config.get("SMTP_ADDRESS_RESOLVER"))

            # # # #
            # The following is based heavily on https://stackoverflow.com/a/32129736/3357118
            msg = email.message.Message()
            msg['Subject'] = subject
            msg['From'] = config.get("SMTP_FROM")
            msg['To'] = resolver.resolve_to_email_address(recipient_user_name)
            msg.add_header('Content-Type', 'text/html')
            msg.set_payload(body)

            s = smtplib.SMTP(config.get("SMTP_SERVER"))

            # Evaluate TLS configuration and start TLS is requested
            if config.has("SMTP_START_TLS") and config.get("SMTP_START_TLS"):
                s.starttls()
            else:
                logger.warning("The value of SMTP_START_TLS is missing or did not evaluate to True, so not using TLS")

            # If a username and password were specified, authenticte to the SMPT server
            if config.has("SMTP_USER") and config.has("SMTP_PASSWORD"):
                logger.debug("Authenticating to the SMTP server")
                s.login(config.get("SMTP_USER"),
                        config.get("SMTP_PASSWORD"))
            else:
                logger.debug("Not authenticating to the SMTP server")

            s.sendmail(msg['From'], [msg['To']], msg.as_string())
            s.quit()
            # # # # # #

            return True
        else:
            logger.warning("SMTP settings are missing on incomplete. Message \"%s\" to %s could not be sent",
                           subject, recipient_user_name)
            return False
    except Exception as e:
        logger.exception(e)
        logger.error("Message \"%s\" to \"%s\" could not be sent", subject, recipient_user_name)
        return False
