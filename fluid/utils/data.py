from typing import Any, FrozenSet, Hashable, Iterable, Set

DEFAULT_SKIP_VALUES = frozenset((None,))


def reversed_dict(d: dict) -> dict:  # type: ignore [type-arg]
    return {v: k for k, v in d.items()}


def compact_dict(
    *args: Iterable,
    skip_values: Set | FrozenSet = DEFAULT_SKIP_VALUES,
    **kwargs: Any,
) -> dict:
    return {
        k: v
        for k, v in dict(*args, **kwargs).items()
        if isinstance(v, bool) or not isinstance(v, Hashable) or v not in skip_values
    }
