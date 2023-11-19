import os

from .tools.text import str2bool

# Workers
STOPPING_GRACE_PERIOD: int = int(os.getenv("STOPPING_GRACE_PERIOD") or "10")

# Database
DBPOOL_MAX_SIZE: int = int(os.getenv("DBPOOL_MAX_SIZE") or "10")
DBPOOL_MAX_OVERFLOW: int = int(os.getenv("DBPOOL_MAX_OVERFLOW") or "10")
DBECHO: bool = str2bool(os.getenv("DBECHO") or "no")
