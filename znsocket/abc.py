import typing as t

try:
    from typing import NotRequired
except ImportError:
    from typing_extensions import NotRequired


class ZnSocketObject:
    """Base class for all znsocket objects."""


class RefreshTypeDict(t.TypedDict):
    start: NotRequired[int | None]
    stop: NotRequired[int | None]
    step: NotRequired[int | None]
    indices: NotRequired[list[int]]
    keys: NotRequired[list[str | int | float]]


class RefreshDataTypeDict(t.TypedDict):
    target: str
    data: RefreshTypeDict


class ListCallbackTypedDict(t.TypedDict):
    setitem: t.Callable[[list[int], t.Any], None]
    delitem: t.Callable[[list[int], t.Any], None]
    insert: t.Callable[[int, t.Any], None]
    append: t.Callable[[t.Any], None]


class DictCallbackTypedDict(t.TypedDict):
    setitem: t.Callable[[str, t.Any], None]
    delitem: t.Callable[[str, t.Any], None]


DictRepr = t.Union[t.Literal["full"], t.Literal["keys"], t.Literal["minimal"]]
ListRepr = t.Union[t.Literal["full"], t.Literal["length"], t.Literal["minimal"]]
