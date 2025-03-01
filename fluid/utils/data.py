from typing import Any, Hashable, Iterable

DEFAULT_SKIP_VALUES = frozenset((None,))


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
