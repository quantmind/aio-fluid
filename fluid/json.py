try:
    import ujson as json
except ImportError:
    import json  # type: ignore

loads = json.loads
dumps = json.dumps
