from typing import Any, Hashable, Iterable

DEFAULT_SKIP_VALUES = frozenset((None,))


def reversed_dict(d: dict) -> dict:  # type: ignore [type-arg]
    return {v: k for k, v in d.items()}


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
