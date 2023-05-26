import uuid


class AnalysisSnapshot:
    def __init__(self, files, snapshot_id=None):
        """
        Represents a single version of a piece of analysis, describing which datasets are available and how those
        datasets were generated.

        :param files: List of files available.
        :type files: list of str
        :param snapshot_id: Id of this analysis snapshot. If None, a message id will automatically be generated in
                           the constructor.
        :type snapshot_id: str | None
        """
        if snapshot_id is None:
            snapshot_id = str(uuid.uuid4())

        self.snapshot_id = snapshot_id
        self.datasets = files

    def to_dict(self):
        return {
            "snapshot_id": self.snapshot_id,
            "files": self.datasets
        }

    @classmethod
    def from_dict(cls, d):
        return cls(
            d["snapshot_id"],
            d["files"]
        )
