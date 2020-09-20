try:
    import ujson as json
except ImportError:
    import json  # type: ignore

load = json.load
loads = json.loads
dumps = json.dumps
