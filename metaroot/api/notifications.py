import email.message
import smtplib
import re
from metaroot.config import get_config, get_global_config, Config
from metaroot.utils import get_logger, instantiate_object_from_class_path

global_config = get_global_config()
logger = get_logger(__name__,
                    global_config.get_log_file(),
                    global_config.get_mq_file_verbosity(),
                    global_config.get_mq_screen_verbosity())


class DefaultEmailAddressResolver:
    """
    Performs a very basic validation that the argument user name looks like a valid email address using a regular
    expression. If y = [one or more chars that cannot contain @] the regex tests for strings that match y@y.y
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
        config = get_config("SMTP")
        if config.has("METAROOT_SMTP_SERVER") and config.has("METAROOT_SMTP_FROM"):
            # Resolve/validate the recipient email adress
            resolver = DefaultEmailAddressResolver()
            if config.has("METAROOT_SMTP_ADDRESS_RESOLVER"):
                resolver = instantiate_object_from_class_path(config.get("METAROOT_SMTP_ADDRESS_RESOLVER"))

            # # # #
            # The following is based heavily on https://stackoverflow.com/a/32129736/3357118
            msg = email.message.Message()
            msg['Subject'] = subject
            msg['From'] = config.get("METAROOT_SMTP_FROM")
            msg['To'] = resolver.resolve_to_email_address(recipient_user_name)
            msg.add_header('Content-Type', 'text/html')
            msg.set_payload(body)

            s = smtplib.SMTP(config.get("METAROOT_SMTP_SERVER"))

            # Evaluate TLS configuration and start TLS is requested
            if config.has("METAROOT_START_TLS") and config.get("METAROOT_START_TLS"):
                s.starttls()
            else:
                logger.warn("The value of METAROOT_START_TLS is missing or did not evaluate to True, so not using TLS")

            # If a username and password were specified, authenticte to the SMPT server
            if config.has("METAROOT_SMTP_USER") and config.has("METAROOT_SMTP_PASSWORD"):
                logger.info("Authenticating to the SMPT server")
                s.login(config.get("METAROOT_SMTP_USER"),
                        config.get("METAROOT_SMTP_PASSWORD"))
            else:
                logger.info("Not authenticating to the SMTP server")

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
