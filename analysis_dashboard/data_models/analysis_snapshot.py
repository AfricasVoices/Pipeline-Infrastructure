import uuid


class AnalysisSnapshot:
    """
    Represents a single version of a piece of analysis, describing which datasets are available and how those
    datasets were generated.
    """

    def __init__(self, files, snapshot_id=None):
        """
        :param files: List of files available.
        :type files: list of str
        :param snapshot_id: Id of this analysis snapshot. If None, a message id will automatically be generated in
                           the constructor.
        :type snapshot_id: str | None
        TODO: Support tags
        """
        if snapshot_id is None:
            snapshot_id = str(uuid.uuid4())

        self.snapshot_id = snapshot_id
        self.files = files

    def to_dict(self):
        """
        Serializes this snapshot to a dictionary.

        :return: Serialized snapshot.
        :rtype: dict
        """
        return {
            "snapshot_id": self.snapshot_id,
            "files": self.files,
            "tags": [],
            "tag_categories": []
        }

    @classmethod
    def from_dict(cls, d):
        """
        Initialises an AnalysisSnapshot from a serialized dictionary.

        :param d: Dictionary to deserialize.
        :type d: dict
        :return: Deserialized snapshot.
        :rtype: AnalysisSnapshot
        """
        return cls(
            d["snapshot_id"],
            d["files"]
        )
