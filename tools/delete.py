import argparse
import json
import re
import sys

from core_data_modules.logging import Logger
from id_infrastructure.firestore_uuid_table import FirestoreUuidTable
from storage.google_cloud import google_cloud_utils


def _query_yes_no(question):
    """Asks a yes/no question via input() and returns True if yes, False if no.
    """
    valid_choices = {"y": True, "n": False}

    while True:
        sys.stdout.write(f"{question} (y/n): ")
        user_input = input().lower()
        if user_input not in valid_choices:
            sys.stdout.write("Please respond with 'y' or 'n').\n")
            continue

        return valid_choices[user_input]


def init_uuid_table_client(google_cloud_credentials_file_path, firebase_credentials_file_url, firebase_table_name):
    log.info("Initialising uuid table client...")
    credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, firebase_credentials_file_url
    ))

    uuid_table = FirestoreUuidTable.init_from_credentials(credentials, firebase_table_name, "avf-participant-uuid-")
    log.info(f"Initialised {firebase_table_name} uuid table client")

    return uuid_table


log = Logger(__name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deletes mappings from Firestore")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("firebase_credentials_file_url", metavar="firebase-credentials-file-url",
                        help="GS URL to the private credentials file for the Firebase account where the phone "
                             "number <-> uuid table is stored.")
    parser.add_argument("firebase_table_name", metavar="firebase-table-name",
                        help="Name of the data <-> uuid table in Firebase to use.")
    parser.add_argument("regexp", help="A regular expression to be matched when searching for mapping(s) you want to be deleted.")

    args = parser.parse_args()

    pattern = re.compile(args.regexp)
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    firebase_credentials_file_url = args.firebase_credentials_file_url
    firebase_table_name = args.firebase_table_name

    log.info("Downloading Firestore UUID Table credentials...")
    participants_uuid_table = init_uuid_table_client(google_cloud_credentials_file_path, firebase_credentials_file_url,
                                                     firebase_table_name)
        
    mappings = participants_uuid_table.get_all_mappings()
    mappings_found = {k:v for (k,v) in mappings.items() if pattern.search(k)}
        
    log.info(f"Listing mapping(s) to be deleted:")
    for key in mappings_found.keys():
        log.info(key)
    
    log.info(f"{len(mappings_found)} mapping(s) to be deleted")
    if not _query_yes_no("Are you sure you want to proceed with deletion?"):
        log.info("Skipping deletion...")
        exit(0)

    participants_uuid_table.delete_mappings(filtered_mappings)
