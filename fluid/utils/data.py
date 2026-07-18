from typing import Any, Hashable, Iterable

DEFAULT_SKIP_VALUES = frozenset((None,))


def reveal_secrets(value: Any) -> Any:
    """Recursively replace pydantic secret values with the values they wrap"""
    if hasattr(value, "get_secret_value"):
        return reveal_secrets(value.get_secret_value())
    if isinstance(value, dict):
        return {key: reveal_secrets(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [reveal_secrets(item) for item in value]
    return value


def compact_dict(
    *args: Iterable[Any],
    skip_values: set[Any] | frozenset[Any] = DEFAULT_SKIP_VALUES,
    **kwargs: Any,
) -> dict[str, Any]:
    return {
        k: v
        for k, v in dict(*args, **kwargs).items()
        if isinstance(v, bool) or not isinstance(v, Hashable) or v not in skip_values
    }
