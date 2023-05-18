class SnapshotPermissions:
    def __init__(self, read_all, read_tag_types):
        self.read_all = read_all
        self.read_tag_types = read_tag_types

    def to_dict(self):
        return {
            "read_all": self.read_all,
            "read_tag_types": self.read_tag_types,
        }

    @classmethod
    def from_dict(cls, d):
        return SnapshotPermissions(
            d["read_all"],
            d["read_tag_types"]
        )


class SeriesUser:
    def __init__(self, user_id, snapshot_permissions, file_permissions):
        self.user_id = user_id
        self.snapshot_permissions = snapshot_permissions
        self.file_permissions = file_permissions

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "snapshot_permissions": self.snapshot_permissions.to_dict(),
            "file_permissions": self.file_permissions
        }

    @classmethod
    def from_dict(cls, d):
        return SeriesUser(
            d["user_id"],
            SnapshotPermissions.from_dict(d["snapshot_permissions"]),
            d["file_permissions"]
        )
