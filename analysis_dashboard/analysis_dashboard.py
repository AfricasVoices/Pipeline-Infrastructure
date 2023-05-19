from core_data_modules.logging import Logger
from firebase_admin import firestore, storage

from analysis_dashboard.data_models import AnalysisSnapshot
from util.firebase_utils import initialize_firebase_app

log = Logger(__name__)


class AnalysisDashboard:
    def __init__(self, firebase_app):
        """
        Client for accessing an Analysis Dashboard Firebase project.

        :param firebase_app: Firebase app.
        :type firebase_app: firebase_admin.App
        """
        self._firebase_app = firebase_app

    @classmethod
    def init_from_credentials(cls, cert, app_name="AnalysisDashboard"):
        """
        :param cert: Firestore service account certificate, as a path to a file or a dictionary.
        :type cert: str | dict
        :param app_name: Name to give the Firebase app instance used to connect.
        :type app_name: str
        :return:
        :rtype: AnalysisDashboard
        """
        return cls(initialize_firebase_app(cert, app_name))

    def create_snapshot(self, series_id, files, bucket_name):
        """
        Creates a new analysis snapshot in Firebase.

        :param series_id: Id of the series the snapshot is for.
        :type series_id: str
        :param files: Files to upload as part of the snapshot, as a dictionary of (local file path) -> (blob name).
        :type files: dict of str -> str
        :param bucket_name: Name of bucket to upload files to e.g. 'analysis-dashboard.appspot.com'
        :type bucket_name: str
        """
        snapshot = AnalysisSnapshot(
            datasets=list(files.values())
        )

        log.info(f"Creating new analysis snapshot with id {snapshot.snapshot_id}...")
        for i, (local_file_path, blob_name) in enumerate(files.items()):
            log.info(f"Uploading file {i + 1}/{len(files)} to storage")
            self.upload_file_to_storage(
                file_path=local_file_path,
                blob_name=f"series/{series_id}/snapshots/{snapshot.snapshot_id}/files/{blob_name}",
                bucket_name=bucket_name
            )

        log.info(f"Writing analysis snapshot document to Firestore...")
        self.creat_snapshot_doc_in_firestore(series_id, snapshot)

    def creat_snapshot_doc_in_firestore(self, series_id, analysis_snapshot):
        """
        Writes a snapshot document to the AnalysisDashboard firestore in 'create' mode.

        If a snapshot with this snapshot id and series id already exists, this function will fail.

        :param series_id: Id of the series this snapshot is for.
        :type series_id: str
        :param analysis_snapshot: Analysis snapshot document to write.
        :type analysis_snapshot: analysis_dashboard.data_models.AnalysisSnapshot
        """
        firestore_client = firestore.client(self._firebase_app)
        firestore_client \
            .document(f"series/{series_id}/snapshots/{analysis_snapshot.snapshot_id}") \
            .create(analysis_snapshot.to_dict())

    def upload_file_to_storage(self, file_path, blob_name, bucket_name):
        """
        Uploads a file from the local disk to an Analysis Dashboard storage bucket.

        :param file_path: Path on local disk to the file to upload.
        :type file_path: str
        :param blob_name: Name to give the blob in storage.
        :type blob_name: str
        :param bucket_name: Name of the bucket to upload the file to.
        :type bucket_name: str
        """
        bucket = storage.bucket(bucket_name, app=self._firebase_app)
        blob = bucket.blob(blob_name)
        log.info(f"Uploading '{file_path}' -> '{blob.public_url}'...")
        blob.upload_from_filename(file_path)
