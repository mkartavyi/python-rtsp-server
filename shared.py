from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class CameraState:
    key: str
    source: str
    camera: Optional["Camera"] = None
    clients: Dict[str, "Client"] = field(default_factory=dict)


class Shared:
    data: Dict[str, CameraState] = {}
