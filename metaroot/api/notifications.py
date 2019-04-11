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
    @staticmethod
    def resolve_to_email_address(user_name: str) -> str:
        # The user name looks like it is already an email address
        if re.fullmatch(r"[^@]+@[^@]+\.[^@]+", user_name):
            return user_name
        else:
            raise Exception("The user name {0} does not appear ot be an email address, and no resolver is configured".format(user_name))


def send_email(recipient_user_name: str, subject: str, body: str):
    try:
        config = get_config("SMTP")
        if config.has("METAROOT_SMTP_SERVER") and config.has("METAROOT_SMTP_USER") and \
           config.has("METAROOT_SMTP_PASSWORD") and config.has("METAROOT_SMTP_FROM"):
            msg = email.message.Message()
            msg['Subject'] = subject
            msg['From'] = config.get("METAROOT_SMTP_FROM")

            # Resolve/validate the recipient email adress
            resolver = DefaultEmailAddressResolver()
            if config.has("METAROOT_SMTP_ADDRESS_RESOLVER"):
                resolver = instantiate_object_from_class_path(config.get("METAROOT_SMTP_ADDRESS_RESOLVER"))
            msg['To'] = resolver.resolve_to_email_address(recipient_user_name)

            msg.add_header('Content-Type', 'text/html')
            msg.set_payload(body)

            # Send the message via local SMTP server.
            s = smtplib.SMTP(config.get("METAROOT_SMTP_SERVER"))
            s.starttls()
            s.login(config.get("METAROOT_SMTP_USER"),
                    config.get("METAROOT_SMTP_PASSWORD"))
            s.sendmail(msg['From'], [msg['To']], msg.as_string())
            s.quit()
            return True
        else:
            logger.warning("SMTP settings are missing on incomplete. Message \"%s\" to %s could not be sent",
                           subject, recipient_user_name)
            return False
    except Exception as e:
        logger.exception(e)
        logger.error("Message \"%s\" to \"%s\" could not be sent", subject, recipient_user_name)
        return False
