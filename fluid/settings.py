import os

from .tools.text import to_bool

# Workers
STOPPING_GRACE_PERIOD: int = int(os.getenv("STOPPING_GRACE_PERIOD") or "10")

# Redis

# Database
DBPOOL_MAX_SIZE: int = int(os.getenv("DBPOOL_MAX_SIZE") or "10")
DBPOOL_MAX_OVERFLOW: int = int(os.getenv("DBPOOL_MAX_OVERFLOW") or "10")
DBECHO: bool = to_bool(os.getenv("DBECHO") or "no")


# Flamegraph
STACK_SAMPLER_PERIOD: int = int(os.getenv("STACK_SAMPLER_PERIOD", "60"))
FLAMEGRAPH_EXECUTABLE: str = os.getenv("FLAMEGRAPH_EXECUTABLE") or "/bin/flamegraph.pl"
FLAMEGRAPH_DATA_BUCKET, FLAMEGRAPH_DATA_PATH = os.getenv(
    "FLAMEGRAPH_DATA_BUCKET_PATH", "replace/me"
).split("/")
