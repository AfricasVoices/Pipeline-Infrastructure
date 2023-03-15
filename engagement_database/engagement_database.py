import uuid

from google.cloud import firestore
from google.cloud.firestore_v1 import DocumentReference

from engagement_database.data_models import Message, HistoryEntry, CommandLogEntry
from util.firestore_utils import make_firestore_client


class EngagementDatabase(object):
    def __init__(self, client, database_path):
        """
        :param client: Firebase client.
        :type client: firebase_admin.auth.Client
        :param database_path: Path to the parent database document e.g. "databases/test-project"
        :type database_path: str
        """
        self._client = client
        self._database_path = database_path

        # Make sure the database we're connecting to exists so it shows when listing available databases
        self._database_ref().set({"database_path": database_path}, merge=True)

    @classmethod
    def init_from_credentials(cls, cert, database_path, app_name="EngagementDatabase"):
        """
        :param cert: Firestore service account certificate, as a path to a file or a dictionary.
        :type cert: str | dict
        :param database_path: Path to the parent database document e.g. "databases/test-project"
        :type database_path: str
        :param app_name: Name to give the Firestore app instance we'll use to connect.
        :type app_name: str
        :return: EngagementDatabase instance
        :rtype: EngagementDatabase
        """
        return cls(make_firestore_client(cert, app_name), database_path)

    def _database_ref(self):
        return self._client.document(self._database_path)

    def _local_path_for_ref(self, ref):
        assert ref.path.startswith(f"{self._database_path}/")
        return ref.path.replace(f"{self._database_path}/", "")

    def _history_ref(self):
        return self._database_ref().collection("history")

    def _history_entry_ref(self, history_entry_id):
        return self._history_ref().document(history_entry_id)

    def _messages_ref(self):
        return self._database_ref().collection("messages")

    def _message_ref(self, message_id):
        return self._messages_ref().document(message_id)

    def _command_logs_ref(self):
        return self._database_ref().collection("command_logs")

    def _command_log_entry_ref(self, command_log_entry_id):
        return self._command_logs_ref().document(command_log_entry_id)

    def get_history(self, firestore_query_filter=lambda q: q, transaction=None, batch_size=None):
        """
        Gets all the history entries in the database.

        Note that requesting large numbers of messages is expensive and this function doesn't guarantee that all
        messages will be downloaded. Use of where and limit filters is strongly encouraged.

        :param firestore_query_filter: Filter to apply to the underlying Firestore query.
        :type firestore_query_filter: Callable of google.cloud.firestore.Query -> google.cloud.firestore.Query
        :param transaction: Transaction to run this get in or None.
        :type transaction: google.cloud.firestore.Transaction | None
        :param batch_size: If set, internally fetches the messages in batches of `batch_size`. This allows
                           `get_history` to handle datasets larger than what can be downloaded from Firestore within
                           1 minute, and is recommended for queries that retrieve a large number of documents.
                           Note that enabling batching may require an additional index to be constructed, and will
                           slow down very small queries so isn't recommended in all circumstances.
                           Note also that batch-fetch mode assumes history_entries are only ever created, not modified.
                           If None, fetches all history entries in a single request from Firestore.
        :type batch_size: int | None
        :return: History entries for the requested message.
        :rtype: list of engagement_database.data_models.HistoryEntry
        """
        query = self._history_ref()
        query = firestore_query_filter(query)

        if batch_size is None:
            data = query.get(transaction=transaction)
            return [HistoryEntry.from_dict(d.to_dict()) for d in data]

        # Order by timestamp here in order to get a consistent fetch of history across all the batches.
        # (A consistent fetch is one which contains all the history entries up to the latest one in the fetch.
        #  If we ordered by default (document id), then we might miss some documents with low ids that are written
        #  while batch-fetching the later ids).
        query = query.order_by("timestamp").limit(batch_size)
        data = query.get(transaction=transaction)
        history_entries = [HistoryEntry.from_dict(d.to_dict()) for d in data]

        last_history_entry = None if len(history_entries) == 0 else history_entries[-1]
        while last_history_entry is not None:
            batch = query.start_after(last_history_entry.to_dict()).get(transaction=transaction)
            history_entries.extend([HistoryEntry.from_dict(d.to_dict()) for d in batch])
            last_history_entry = None if len(batch) == 0 else batch[-1]

        return history_entries

    def get_history_for_message(self, message_id, firestore_query_filter=lambda q: q, transaction=None):
        """
        Gets all the history entries for a message.

        :param message_id: Id of message to get history for.
        :type message_id: str
        :param firestore_query_filter: Filter to apply to the underlying Firestore query.
        :type firestore_query_filter: Callable of google.cloud.firestore.Query -> google.cloud.firestore.Query
        :param transaction: Transaction to run this get in or None.
        :type transaction: google.cloud.firestore.Transaction | None
        :return: History entries for the requested message.
        :rtype: list of engagement_database.data_models.HistoryEntry
        """
        message_ref = self._message_ref(message_id)
        query = self._history_ref().where("update_path", "==", message_ref.path)
        query = firestore_query_filter(query)
        data = query.get(transaction=transaction)
        return [HistoryEntry.from_dict(d.to_dict()) for d in data]

    def get_message(self, message_id, transaction=None):
        """
        Gets a message by id from the database.

        :param message_id: Id of message to get.
        :type message_id: str
        :param transaction: Transaction to run this get in or None.
        :type transaction: google.cloud.firestore.Transaction | None
        :return: Message with id `message_id`, if it exists in the database, otherwise None.
        :rtype: engagement_database.data_models.Message | None
        """
        doc = self._message_ref(message_id).get(transaction=transaction)
        if not doc.exists:
            return None
        return Message.from_dict(doc.to_dict())

    def get_messages(self, firestore_query_filter=lambda q: q, transaction=None, batch_size=None):
        """
        Gets messages from the database.

        Note that requesting large numbers of messages is expensive and this function doesn't guarantee that all
        messages will be downloaded. Use of where and limit filters, as well as the batch_size parameter, are strongly
        encouraged.

        Note also that providing a transaction for a query that matches a lot of documents will lock a large number
        of documents, causing performance issues.

        :param firestore_query_filter: Filter to apply to the underlying Firestore query.
        :type firestore_query_filter: Callable of google.cloud.firestore.Query -> google.cloud.firestore.Query
        :param transaction: Transaction to run this get in or None.
        :type transaction: google.cloud.firestore.Transaction | None
        :param batch_size: If set, internally fetches the messages in batches of `batch_size`. This allows
                           `get_messages` to handle datasets larger than what can be downloaded from Firestore within
                           1 minute, and is recommended for queries that retrieve a large number of documents.
                           Note that enabling batching may require an additional index to be constructed, and will
                           slow down very small queries so isn't recommended in all circumstances.
                           If None, fetches all messages in a single request from Firestore.
        :type batch_size: int | None
        :return: Messages downloaded from the database.
        :rtype: list of engagement_database.data_models.Message
        """
        messages_ref = self._messages_ref()
        query = firestore_query_filter(messages_ref)

        if batch_size is None:
            data = query.get(transaction=transaction)
            return [Message.from_dict(d.to_dict()) for d in data]

        query = query.order_by("last_updated").order_by("message_id")
        query = query.limit(batch_size)

        data = query.get(transaction=transaction)
        messages = [Message.from_dict(d.to_dict()) for d in data]

        last_msg = None if len(messages) == 0 else messages[-1]
        while last_msg is not None:
            batch = query.start_after(last_msg.to_dict()).get(transaction=transaction)
            messages.extend([Message.from_dict(d.to_dict()) for d in batch])
            last_msg = None if len(batch) == 0 else batch[-1]

        # By fetching in batches while writes are happening, it's possible to download multiple different versions of
        # the same message, e.g. the versions of that message before and after a concurrent write.
        # Just in case this happened, de-duplicate the fetched messages by message id, keeping only the latest version
        # of each message.
        messages = list(messages)
        messages.sort(key=lambda msg: msg.last_updated, reverse=True)  # sort messages, most recently updated first

        de_duplicated_messages = []
        seen_ids = set()
        for msg in messages:
            if msg.message_id in seen_ids:
                continue

            seen_ids.add(msg.message_id)
            de_duplicated_messages.append(msg)

        # Ensure no duplicates remain
        assert len(de_duplicated_messages) == len({msg.message_id for msg in de_duplicated_messages})

        return de_duplicated_messages

    def set_message(self, message, origin, transaction=None):
        """
        Sets a message in the database.

        :param message: Message to write to the database.
        :type message: engagement_database.data_models.Message
        :param origin: Origin details for this update.
        :type origin: engagement_database.data_models.HistoryEntryOrigin
        :param transaction: Transaction to run this update in or None.
                            If None, writes immediately, otherwise adds the updates to a transaction that will need
                            to be explicitly committed elsewhere.
        :type transaction: google.cloud.firestore.Transaction | None
        """
        message = message.copy()
        message.last_updated = firestore.SERVER_TIMESTAMP

        if transaction is None:
            # If no transaction was given, run all the updates in a new batched-write transaction and flag that
            # this transaction needs to be committed before returning from this function.
            transaction = self._client.batch()
            commit_before_returning = True
        else:
            commit_before_returning = False

        # Set the message
        transaction.set(
            self._message_ref(message.message_id),
            message.to_dict()
        )

        # Log a history event for this update
        history_entry = HistoryEntry(
            db_update_path=self._local_path_for_ref(self._message_ref(message.message_id)),
            origin=origin,
            updated_doc=message,
            timestamp=firestore.SERVER_TIMESTAMP
        )
        transaction.set(
            self._history_entry_ref(history_entry.history_entry_id),
            history_entry.to_dict()
        )

        if commit_before_returning:
            transaction.commit()

    def get_command_log_entries(self, firestore_query_filter=lambda q: q, transaction=None):
        """
        Gets all the command log entries in the database.

        Note that requesting large numbers of messages is expensive and this function doesn't guarantee that all
        messages will be downloaded. Use of where and limit filters is strongly encouraged.

        :param firestore_query_filter: Filter to apply to the underlying Firestore query.
        :type firestore_query_filter: Callable of google.cloud.firestore.Query -> google.cloud.firestore.Query
        :param transaction: Transaction to run this get in or None.
        :type transaction: google.cloud.firestore.Transaction | None
        :return: Command log entries.
        :rtype: list of engagement_database.data_models.CommandLogEntry
        """
        query = self._command_logs_ref()
        query = firestore_query_filter(query)
        data = query.get(transaction=transaction)

        return [CommandLogEntry.from_dict(d.to_dict()) for d in data]

    def set_command_log_entry(self, command_log_entry, transaction=None):
        command_log_entry = command_log_entry.copy()

        if command_log_entry.timestamp is None:
            command_log_entry.timestamp = firestore.SERVER_TIMESTAMP

        ref = self._command_log_entry_ref(command_log_entry.command_log_entry_id)
        if transaction is None:
            ref.set(command_log_entry.to_dict())
        else:
            transaction.set(ref, command_log_entry.to_dict())

    def restore_doc(self, doc, path, transaction=None):
        """
        Restores a document to the database without setting history.

        :param doc: Document to write.
        :type doc: any
        :param path: Path to write the document to, relative to the engagement database root document.
        :type path: str
        :param transaction: Transaction to run this update in or None.
                            If None, writes immediately, otherwise adds the updates to a transaction that will need
                            to be explicitly committed elsewhere.
        :type transaction: google.cloud.firestore.Transaction | None
        """
        ref = self._client.document(f"{self._database_path}/{path}")
        if transaction is None:
            ref.set(doc.to_dict())
        else:
            transaction.set(ref, doc.to_dict())

    def delete_doc(self, path, transaction=None):
        """
        Deletes a doc from the database.

        CAUTION: If used without care, this can break history. To archive messages while correctly preserving history,
        use `set_message` instead.

        :param path: Path to the document to delete, relative to the engagement database root document.
        :type path: str
        :param transaction: Transaction to run this update in or None.
                            If None, writes immediately, otherwise adds the updates to a transaction that will need
                            to be explicitly committed elsewhere.
        :type transaction: google.cloud.firestore.Transaction | None
        """
        ref = self._client.document(f"{self._database_path}/{path}")
        if transaction is None:
            ref.delete()
        else:
            transaction.delete(ref)

    def delete_message_and_history(self, message_id, transaction=None):
        """
        Deletes a message and its history entries.

        :param message_id: Id of message to delete.
        :type message_id: str
        :param transaction: Transaction to run this update in or None.
                            If None, writes immediately, otherwise adds the updates to a transaction that will need
                            to be explicitly committed elsewhere.
        :type transaction: google.cloud.firestore.Transaction | None
        """
        message = self.get_message(message_id, transaction=transaction)
        history_entries = self.get_history_for_message(message_id, transaction=transaction)
        print(message_id, history_entries)
        
        if transaction is None:
            # If no transaction was given, run all the updates in a new batched-write transaction and flag that
            # this transaction needs to be committed before returning from this function.
            transaction = self._client.batch()
            commit_before_returning = True
        else:
            commit_before_returning = False

        if message is not None:
            self.delete_doc(f"messages/{message_id}", transaction=transaction)
        
        for history_entry in history_entries:
            self.delete_doc(f"history/{history_entry.history_entry_id}", transaction=transaction)

        if commit_before_returning:
            transaction.commit()

    def restore_history_entry(self, history_entry, transaction=None):
        """
        Restores a history entry to the database.

        Restores the history entry only, and not the doc it records history for.

        :param history_entry: History entry to restore.
        :type history_entry: engagement_database.data_models.HistoryEntry
        :param transaction: Transaction to run this update in or None.
                            If None, writes immediately, otherwise adds the updates to a transaction that will need
                            to be explicitly committed elsewhere.
        :type transaction: google.cloud.firestore.Transaction | None
        """
        if transaction is None:
            self._history_entry_ref(history_entry.history_entry_id).set(history_entry.to_dict())
        else:
            transaction.set(self._history_entry_ref(history_entry.history_entry_id), history_entry.to_dict())

    def batch(self):
        return self._client.batch()

    def transaction(self):
        return self._client.transaction()
