import firebase_admin
from core_data_modules.logging import Logger
from firebase_admin import credentials

log = Logger(__name__)


def initialize_firebase_app(cert, app_name):
    """
    Initializes a Firebase app from the given credentials.

    Connects the returned client to a new app with the given `app_name`, so that this client doesn't interfere with
    any other Firebase clients. Note that this will force the default app to be created if it hasn't already,
    because the Firebase api insists on a default app existing before named apps can be created.

    :param cert: Path to a firebase credentials file or a dictionary containing firebase credentials.
    :type cert: str | dict
    :param app_name: Name to give the Firebase app instance that a client will be constructed for.
    :type app_name: str
    :return: Firebase client.
    :rtype: firebase_admin.App
    """
    # Create the default app if it doesn't already exist, because we can't create an app with a custom `app_name`
    # without creating a default app first.
    try:
        firebase_admin.get_app()
    except ValueError:
        log.debug("Creating default Firebase app")
        firebase_admin.initialize_app()

    log.debug(f"Creating Firebase app {app_name}")
    cred = credentials.Certificate(cert)
    app = firebase_admin.initialize_app(cred, name=app_name)
    return app
