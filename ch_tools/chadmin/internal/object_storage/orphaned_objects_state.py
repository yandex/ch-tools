import json
from dataclasses import asdict, dataclass


@dataclass
class OrphanedObjectsState:
    orphaned_objects_size: int
    error_msg: str

    @classmethod
    def from_json(cls, json_str: str) -> "OrphanedObjectsState":
        data = json.loads(json_str)
        return cls(
            orphaned_objects_size=data["orphaned_objects_size"],
            error_msg=data["error_msg"],
        )

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=4)
