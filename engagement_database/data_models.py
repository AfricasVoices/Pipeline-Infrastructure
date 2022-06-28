import json
import uuid
from datetime import datetime

from core_data_modules.data_models import Label
from core_data_modules.data_models.message import get_latest_labels
from core_data_modules.traced_data import Metadata


class MessageStatuses(object):
    LIVE = "live"              # Message is part of the 'live' data, and should be used in analysis.
    STALE = "stale"            # Message is stale and we should attempt to get a newer answer if we can. This message
                               # can be used until we get a newer answer.
    ARCHIVED = "archived"      # This message is no longer relevant and should be ignored by analysis.
    ANONYMISED = "anonymised"  # Message is anonymised, so personally identifiable details have been removed.

    VALUES = {LIVE, STALE, ARCHIVED, ANONYMISED}


class MessageDirections(object):
    IN = "in"                  # Message sent from a participant to AVF.
    OUT = "out"                # Message sent from AVF to a participant.

    VALUES = {IN, OUT}


class MessageOrigin:
    def __init__(self, origin_id, origin_type):
        """
        Represents a message's origin.

        :param origin_id: Unique identifier for this message in the origin dataset.
                          The same message in the origin dataset should always be assigned the same id.
        :type origin_id: str
        :param origin_type: Origin type e.g. "rapid_pro", "recovery_csv"
        :type origin_type: str
        """
        self.origin_id = origin_id
        self.origin_type = origin_type

    def to_dict(self):
        return {
            "origin_id": self.origin_id,
            "origin_type": self.origin_type
        }

    @classmethod
    def from_dict(cls, d):
        return MessageOrigin(
            d["origin_id"],
            d["origin_type"]
        )


class Message:
    DOC_TYPE = "message"

    def __init__(self, text, timestamp, participant_uuid, direction, channel_operator, status, dataset, labels,
                 origin, message_id=None, coda_id=None, last_updated=None, previous_datasets=None):
        """
        Represents a message sent to or received from a participant.

        :param text: Raw text of this message.
        :type text: str
        :param timestamp: Time that this message was sent/received.
        :type timestamp: datetime.datetime
        :param participant_uuid: Id of the participant who sent this message
        :type participant_uuid: str
        :param direction: One of `MessageDirections.VALUES`.
        :type direction: str
        :param channel_operator: Operator of the channel that wired this message e.g. hormuud, telegram.
        :type channel_operator: str
        :param status: One of `MessageStatus.VALUES`.
        :type status: str
        :param dataset: Dataset that this message belongs to e.g. "age", "healthcare_s01e01"
        :type dataset: str
        :param labels: Labels assigned to this message.
        :type labels: list of core_data_modules.data_models.Label
        :param origin: Origin of this message.
        :type origin: MessageOrigin
        :param message_id: Id of this message. If None, a message id will automatically be generated in the constructor.
        :type message_id: str | None
        :param coda_id: Id to use to look-up this message in Coda, optional.
        :type coda_id: str | None
        :param last_updated: Timestamp this message was last updated in Firestore, or None if it does not yet exist.
        :type last_updated: datetime.datetime | None
        :param previous_datasets: Datasets which this message originally belonged to/moved from. If None, initialises with an empty list
        :type previous_datasets: list of strings | None
        """

        if message_id is None:
            message_id = str(uuid.uuid4())

        assert status in MessageStatuses.VALUES, status
        assert direction in MessageDirections.VALUES, direction

        if previous_datasets is None:
            previous_datasets = []

        self.text = text
        self.timestamp = timestamp
        self.participant_uuid = participant_uuid
        self.direction = direction
        self.channel_operator = channel_operator
        self.status = status
        self.dataset = dataset
        self.labels = labels
        self.origin = origin
        self.message_id = message_id
        self.coda_id = coda_id
        self.last_updated = last_updated
        self.previous_datasets = previous_datasets

    def get_latest_labels(self):
        """
        Returns the latest label assigned to each code scheme.
        """
        return get_latest_labels(self.labels)

    def to_dict(self, serialize_datetimes_to_str=False):
        """
        Serializes this message to a dict.

        :param serialize_datetimes_to_str: Whether to serialize timestamps to strings instead of leaving as Python
                                           datetime objects.
        :type serialize_datetimes_to_str: bool
        :return: This message serialized to a dict.
        :rtype: dict
        """
        message_dict = {
            "text": self.text,
            "timestamp": self.timestamp,
            "participant_uuid": self.participant_uuid,
            "direction": self.direction,
            "channel_operator": self.channel_operator,
            "status": self.status,
            "dataset": self.dataset,
            "labels": [label.to_dict() for label in self.labels],
            "origin": self.origin.to_dict(),
            "message_id": self.message_id,
            "last_updated": self.last_updated,
            "previous_datasets": self.previous_datasets,
        }

        if self.coda_id is not None:
            message_dict["coda_id"] = self.coda_id

        if serialize_datetimes_to_str:
            message_dict["timestamp"] = message_dict["timestamp"].isoformat()
            message_dict["last_updated"] = message_dict["last_updated"].isoformat()

        return message_dict

    @classmethod
    def from_dict(cls, d):
        """
        Deserializes a dict to a Message.

        Converts timestamps that were serialized to strings by `cls.to_dict` back to Python datetime objects.

        :param d: Dict to deserialize.
        :type d: dict
        :return: Deserialized message
        :rtype: Message
        """
        # Handle variables that may have been serialized to strings by `cls.to_dict()`.
        timestamp = d["timestamp"]
        if type(timestamp) == str:
            timestamp = datetime.fromisoformat(timestamp)

        last_updated = d["last_updated"]
        if type(last_updated) == str:
            last_updated = datetime.fromisoformat(last_updated)

        return cls(
            text=d["text"],
            timestamp=timestamp,
            participant_uuid=d["participant_uuid"],
            direction=d["direction"],
            channel_operator=d["channel_operator"],
            status=d["status"],
            dataset=d["dataset"],
            labels=[Label.from_dict(label) for label in d["labels"]],
            origin=MessageOrigin.from_dict(d["origin"]),
            previous_datasets=d["previous_datasets"],
            message_id=d.get("message_id"),
            coda_id=d.get("coda_id"),
            last_updated=last_updated
        )

    def copy(self):
        return Message.from_dict(self.to_dict())


class HistoryEntry(object):
    DOC_TYPE = "history_entry"

    def __init__(self, db_update_path, updated_doc, origin, timestamp, history_entry_id=None, doc_type=None):
        """
        Represents an entry in the database's history, describing an update to one of the documents.

        :param db_update_path: Path in Firestore to the document that was updated, relative to the engagement database
                               root document.
        :type db_update_path: str
        :param updated_doc: Snapshot of the updated document at the time the update was made.
                            The document object requires a `to_dict()` method so it can be serialized.
        :type updated_doc: dict | obj with to_dict() method.
        :param origin: Origin of this update.
        :type origin: HistoryEntryOrigin
        :param timestamp: Timestamp this entry was made in Firestore, or None if it hasn't yet been written to Firestore
        :type timestamp: datetime.datetime | None
        :param history_entry_id: Id of this history entry. If None, an id will automatically be generated in the
                                 constructor.
        :type history_entry_id: str | None
        :param doc_type: Type of the document this is recording history for.
                         If None, attempts to read the doc_type from the `updated_doc`'s `DOC_TYPE` field.
        :type doc_type: str | None
        """
        if history_entry_id is None:
            history_entry_id = str(uuid.uuid4())

        self.history_entry_id = history_entry_id
        self.db_update_path = db_update_path
        self.updated_doc = updated_doc
        self.origin = origin
        self.timestamp = timestamp
        self.doc_type = updated_doc.DOC_TYPE

    def to_dict(self, serialize_datetimes_to_str=False):
        """
        Serializes this history entry to a dict.

        :param serialize_datetimes_to_str: Whether to serialize timestamps to strings instead of leaving as Python
                                           datetime objects. Applies recursively to all nested objects in this entry.
        :type serialize_datetimes_to_str: bool
        :return: This message serialized to a dict.
        :rtype: dict
       """
        history_entry_dict = {
            "history_entry_id": self.history_entry_id,
            "db_update_path": self.db_update_path,
            "updated_doc": self.updated_doc.to_dict(serialize_datetimes_to_str),
            "origin": self.origin.to_dict(),
            "timestamp": self.timestamp,
            "doc_type": self.doc_type
        }

        if serialize_datetimes_to_str:
            history_entry_dict["timestamp"] = history_entry_dict["timestamp"].isoformat()

        return history_entry_dict

    @classmethod
    def from_dict(cls, d):
        """
        :param d: Dictionary to initialise from.
        :type d: dict
        :return: HistoryEntry instance
        :rtype: HistoryEntry
        """
        timestamp = d["timestamp"]
        if type(timestamp) == str:
            timestamp = datetime.fromisoformat(timestamp)

        if d["doc_type"] == Message.DOC_TYPE:
            doc_class = Message
        else:
            raise ValueError(f"Unknown `doc_type` {d['doc_type']}")

        return HistoryEntry(
            history_entry_id=d["history_entry_id"],
            db_update_path=d["db_update_path"],
            updated_doc=doc_class.from_dict(d["updated_doc"]),
            origin=HistoryEntryOrigin.from_dict(d["origin"]),
            timestamp=timestamp,
            doc_type=d["doc_type"]
        )


class HistoryEntryOrigin(object):
    # Default settings to use for properties that are very unlikely to change while a script is running.
    # Set these with cls.set_defaults()
    _default_user = None
    _default_project = None
    _default_pipeline = None
    _default_commit = None

    def __init__(self, origin_name, details, user=None, project=None, pipeline=None, commit=None, line=None):
        """
        Represents the origin description for a history event.

        :param origin_name: Human-friendly name describing the origin of the update e.g. "Rapid Pro -> Database Sync"
        :type origin_name: str
        :param user: Id of the user who ran the program that created the update e.g. user@domain.com.
                     If None, attempts to use the global default set by cls.set_defaults if it exists, otherwise fails.
        :type user: str | None
        :param project: Name of the project that created the update, ideally as the repository origin url.
                        If None, attempts to use the global default set by cls.set_defaults if it exists, otherwise fails.
        :type project: str
        :param commit: Id of the vcs commit for the version of code that created the update.
                       If None, attempts to use the global default set by cls.set_defaults if it exists, otherwise fails.
        :type commit: str
        :param pipeline: Name of the pipeline that created the update.
                         If None, attempts to use the global default set by cls.set_defaults if it exists, otherwise fails.
        :type pipeline: str
        :param details: Dictionary containing any update-specific details that help to explain/justify the update.
                        This is to aid with manual debugging, and would typically include a copy of source data and
                        a description of its original location.
                        For example:
                         - When importing messages from Rapid Pro, include the Rapid Pro workspace name and run/message.
                         - When importing participants from a listening group csv, include the csv's name and hash.
                         - When updating labels from Coda, include the dataset id and message in Coda.
        :type details: dict
        :param line: Line of code that created the update. If None, automatically sets to the line that called this
                     constructor.
        :type line: str | None
        """
        if line is None:
            line = Metadata.get_call_location(depth=2)

        if user is None:
            assert HistoryEntryOrigin._default_user is not None, \
                "No default user set. Set one with HistoryEventOrigin.set_defaults"
            user = HistoryEntryOrigin._default_user

        if project is None:
            assert HistoryEntryOrigin._default_project is not None, \
                "No default project set. Set one with HistoryEventOrigin.set_defaults"
            project = HistoryEntryOrigin._default_project

        if pipeline is None:
            assert HistoryEntryOrigin._default_pipeline is not None, \
                "No default pipeline set. Set one with HistoryEventOrigin.set_defaults"
            pipeline = HistoryEntryOrigin._default_pipeline

        if commit is None:
            assert HistoryEntryOrigin._default_commit is not None, \
                "No default commit set. Set one with HistoryEventOrigin.set_defaults"
            commit = HistoryEntryOrigin._default_commit

        self.origin_name = origin_name
        self.user = user
        self.project = project
        self.commit = commit
        self.pipeline = pipeline
        self.line = line
        self.details = details

        self.validate_details()

    @classmethod
    def set_defaults(cls, user, project, pipeline, commit):
        """
        Sets default options for parameters that are very unlikely to change over the run of a single script.
        If set, these will automatically be used when constructing future HistoryEventOrigins.

        :param user: Id of the user who ran the program that created the update e.g. user@domain.com.
        :type user: str | None
        :param project: Name of the project that created the update, ideally as the repository origin url.
        :type project: str
        :param commit: Id of the vcs commit for the version of code that created the update.
        :type commit: str
        :param pipeline: Name of the pipeline that created the update.
        :type pipeline: str
        """
        cls._default_user = user
        cls._default_project = project
        cls._default_pipeline = pipeline
        cls._default_commit = commit

    def to_dict(self):
        self.validate_details()

        return {
            "origin_name": self.origin_name,
            "user": self.user,
            "project": self.project,
            "commit": self.commit,
            "pipeline": self.pipeline,
            "line": self.line,
            "details": self.details
        }

    @classmethod
    def from_dict(cls, d):
        return HistoryEntryOrigin(
            origin_name=d["origin_name"],
            user=d["user"],
            project=d["project"],
            pipeline=d["pipeline"],
            details=d["details"],
            commit=d["commit"],
            line=d["line"]
        )

    def validate_details(self):
        """
        Ensure the origin details consist of only primitives that can be easily serialised.

        Raises an error if there is a problem, returns void otherwise.
        """
        json.dumps(self.details)


class CommandLogEntry:
    DOC_TYPE = "command_log_entry"

    def __init__(self, command, user, project, commit, line, command_log_entry_id=None):
        if command_log_entry_id is None:
            command_log_entry_id = str(uuid.uuid4())

        self.command_log_entry_id = command_log_entry_id
        self.command = command
        self.user = user
        self.project = project
        self.commit = commit
        self.line = line

    def to_dict(self):
        return {
            "command_log_entry_id": self.command_log_entry_id,
            "command": self.command,
            "user": self.user,
            "project": self.project,
            "commit": self.commit,
            "line": self.line
        }

    @classmethod
    def from_dict(cls, d):
        pass
