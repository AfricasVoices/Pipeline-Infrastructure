import sys
import argparse
import json

from core_data_modules.logging import Logger
from id_infrastructure.firestore_uuid_table import FirestoreUuidTable
from storage.google_cloud import google_cloud_utils


def query_yes_no(question, default="yes"):
    """Asks a yes/no question via input() and returns user input.
    """
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError(f"invalid default answer: {default}")

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if choice == "" and default is not None:
            return valid[default]
        elif choice in valid:
            return True
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' " "(or 'y' or 'n').\n")


def concat_description(desc_list):
    if len(desc_list) == 0:
        return ''
    if len(desc_list) == 1:
        return desc_list[0]
    if len(desc_list) == 2:
        return desc_list[0] + ' and ' + desc_list[1]
    return ', '.join(desc_list[:-1]) + ', and ' + desc_list[-1]


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
    parser = argparse.ArgumentParser(description="Deletes data from Firestore")
    parser.add_argument("--startswith", metavar="startswith",
                        help="Enter prefix to be searched for in the data")
    parser.add_argument("--contains", metavar="contains",
                        help="Enter substring to be searched for in the data")
    parser.add_argument("--endswith", metavar="endswith",
                        help="Enter suffix to be searched for in the data")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("firebase_credentials_file_url", metavar="firebase-credentials-file-url",
                        help="GS URL to the private credentials file for the Firebase account where the phone "
                             "number <-> uuid table is stored.")
    parser.add_argument("firebase_table_name", metavar="firebase-table-name",
                        help="Name of the data <-> uuid table in Firebase to use.")

    args = parser.parse_args()

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    firebase_credentials_file_url = args.firebase_credentials_file_url
    firebase_table_name = args.firebase_table_name
    
    prefix = args.startswith
    substring = args.contains
    suffix = args.endswith

    log.info("Downloading Firestore UUID Table credentials...")
    participants_uuid_table = init_uuid_table_client(google_cloud_credentials_file_path, firebase_credentials_file_url,
                                                     firebase_table_name)

    filter_by_prefix = lambda q: q[0].startswith(prefix)
    filter_by_substring = lambda q: substring in q[0]
    filter_by_suffix = lambda q: q[0].endswith(suffix)

    firestore_filters = []
    firestore_filter_desc = []

    if prefix:
        firestore_filters.append(filter_by_prefix)
        firestore_filter_desc.append(f"start with {prefix}")

    if substring:
        firestore_filters.append(filter_by_substring)
        firestore_filter_desc.append(f"contain {substring}")

    if suffix:
        firestore_filters.append(filter_by_suffix)
        firestore_filter_desc.append(f"end with {suffix}")

    if len(firestore_filters) == 0:
        log.info("Filter mechanism not specified, skipping...")
        exit(0)
        
    mappings = participants_uuid_table.get_all_mappings()
    filtered_mappings = dict(filter( lambda x: all(f(x) for f in firestore_filters), mappings.items())) # As soon as a single filter returns False, that map element won't be included.

    if not filtered_mappings:
        log.info("No mappings to be deleted")
        exit(0)
        
    log.warning(f"Deleting {len(filtered_mappings)} mapping(s) that {concat_description(firestore_filter_desc)}")
    log.info("See sample data below ...")
    limit = 5 if len(filtered_mappings) > 5 else len(filtered_mappings)
    for i, k in zip(range(limit), filtered_mappings.keys()):
        log.info(f"{k}")

    proceed = query_yes_no("Are you sure you want to proceed with deletion?")
    if not proceed:
        log.info("Skipping deletion of mappings ...")
        exit(0)

    participants_uuid_table.delete_mappings(filtered_mappings)
    log.info(f"Deleted {len(filtered_mappings)} mapping(s)")

    urns = list(filtered_mappings.keys())
    print(json.dumps(urns, indent=2))
