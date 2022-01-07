import argparse
import csv
import json

from core_data_modules.logging import Logger
from id_infrastructure.firestore_uuid_table import FirestoreUuidTable
from storage.google_cloud import google_cloud_utils

log = Logger(__name__)


# Todo move to core
def _normalise_and_validate_contact_urn(contact_urn):
    """
    Normalises and validates the given URN.

    Fails with an AssertionError if the given URN is invalid.

    :param contact_urn: URN to de-identify.
    :type contact_urn: str
    :return: Normalised contact urn.
    :rtype: str
    """
    if contact_urn.startswith("tel:"):
        # TODO: This is known to fail for golis numbers via Shaqodoon. Leaving as a fail-safe for now
        #       until we're ready to test with golis numbers.
        assert contact_urn.startswith("tel:+")

    if contact_urn.startswith("telegram:"):
        # Sometimes a telegram urn ends with an optional #<username> e.g. telegram:123456#testuser
        # To ensure we always get the same urn for the same telegram user, normalise telegram urns to exclude
        # this #<username>
        contact_urn = contact_urn.split("#")[0]

    return contact_urn

def _init_uuid_table_client(google_cloud_credentials_file_path, firebase_credentials_file_url, firebase_table_name):

    log.info("Initialising uuid table client...")
    credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, firebase_credentials_file_url
    ))

    uuid_table = FirestoreUuidTable.init_from_credentials(credentials, firebase_table_name, "avf-participant-uuid-")
    log.info(f"Initialised {firebase_table_name} uuid table client")

    return uuid_table


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="De-identifies a CSV by converting the phone numbers in "
                                                 "the specified column to avf phone ids")

    parser.add_argument("csv_input_path", metavar="recovered-csv-input-url",
                        help="Path to a CSV file to de-identify a column of")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("firebase_credentials_file_url", metavar="firebase-credentials-file-url",
                        help="GS URL to the private credentials file for the Firebase account where the phone "
                             "number <-> uuid table is stored.")
    parser.add_argument("firebase_table_name", metavar="firebase-table-name",
                        help="Name of the data <-> uuid table in Firebase to use.")
    parser.add_argument("column_to_de_identify", metavar="column-to-de-identify",
                        help="Name of the column containing participants URNs to be de-identified")
    parser.add_argument("de_identified_csv_output_path", metavar="de-identified-csv-output-path",
                        help="Path to write the de-identified CSV to")

    args = parser.parse_args()

    csv_input_path = args.csv_input_path
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    firebase_credentials_file_url = args.firebase_credentials_file_url
    firebase_table_name = args.firebase_table_name
    column_to_de_identify = args.column_to_de_identify
    de_identified_csv_output_path = args.de_identified_csv_output_path

    log.info("Downloading Firestore UUID Table credentials...")

    participants_uuid_table = _init_uuid_table_client(google_cloud_credentials_file_path, firebase_credentials_file_url,
                                                     firebase_table_name)

    log.info(f"Loading csv from '{csv_input_path}'...")
    with open(csv_input_path, "r", encoding='utf-8-sig') as f:
        raw_data = list(csv.DictReader(f))
    log.info(f"Loaded {len(raw_data)} rows")

    log.info(f"Normalising phone numbers in column '{column_to_de_identify}'...")
    for row in raw_data:
        row[column_to_de_identify] = _normalise_and_validate_contact_urn(row[column_to_de_identify])

    log.info(f"De-identifying column '{column_to_de_identify}'...")
    urns = [row[column_to_de_identify] for row in raw_data]

    participant_to_uuid_lut = participants_uuid_table.data_to_uuid_batch(urns)
    for row in raw_data:
        row[column_to_de_identify] = participant_to_uuid_lut[row[column_to_de_identify]]

    log.info(f"Exporting {len(raw_data)} de-identified rows to {de_identified_csv_output_path}...")
    with open(de_identified_csv_output_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=raw_data[0].keys())
        writer.writeheader()

        for row in raw_data:
            writer.writerow(row)
    log.info(f"Exported de-identified csv")
