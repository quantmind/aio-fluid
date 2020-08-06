import os

from openapi.tz import utcnow

__version__ = os.environ.get("GIT_SHA", "unknown")
__timestamp__ = os.environ.get("TIMESTAMP", utcnow().isoformat())
