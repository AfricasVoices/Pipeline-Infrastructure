from firebase_admin import firestore

from util.firebase_utils import initialize_firebase_app


def make_firestore_client(cert, app_name):
    """
    Creates a Firestore client from the given credentials.

    Connects the returned client to a new app with the given `app_name`, so that this client doesn't interfere with
    any other Firestore clients. Note that this will force the default app to be created if it hasn't already,
    because the Firestore api insists on a default app existing before named apps can be created.

    :param cert: Path to a firebase credentials file or a dictionary containing firebase credentials.
    :type cert: str | dict
    :param app_name: Name to give the Firestore app instance that a client will be constructed for.
    :type app_name: str
    :return: Firestore client.
    :rtype: google.cloud.firestore.Firestore
    """
    app = initialize_firebase_app(cert, app_name)
    return firestore.client(app)
