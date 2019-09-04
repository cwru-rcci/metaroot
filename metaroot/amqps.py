import ssl
from metaroot.config import Config


def get_ssl_context_from_config(config: Config) -> ssl.SSLContext:
    cxt = ssl.SSLContext()

    cxt.verify_mode = ssl.CERT_REQUIRED
    if config.get_ssl_verify_mode() == "NONE":
        cxt.verify_mode = ssl.CERT_NONE
    elif config.get_ssl_verify_mode() == "OPTIONAL":
        cxt.verify_mode = ssl.CERT_OPTIONAL
    elif config.get_ssl_verify_mode() == "REQUIRED":
        cxt.verify_mode = ssl.CERT_REQUIRED

    if config.get_ssl_nocheck_hostname():
        cxt.check_hostname = False
    else:
        cxt.check_hostname = True

    return cxt
