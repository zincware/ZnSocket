import typing as t


class RefreshTypeDict(t.TypedDict):
    start: t.NotRequired[int | None]
    stop: t.NotRequired[int | None]
    step: t.NotRequired[int | None]
    indices: t.NotRequired[list[int]]
    keys: t.NotRequired[list[str]]


class RefreshDataTypeDict(t.TypedDict):
    target: str
    data: RefreshTypeDict
