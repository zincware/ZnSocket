import typing as t
from collections.abc import MutableSequence

import redis.exceptions
import znjson
import json

from znsocket.abc import (
    ListCallbackTypedDict,
    ListRepr,
    RefreshDataTypeDict,
    RefreshTypeDict,
    ZnSocketObject,
)
from znsocket.client import Client
from znsocket.exceptions import FrozenStorageError
from znsocket.utils import decode, encode, handle_error

if t.TYPE_CHECKING:
    from znsocket import List


class Segments(ZnSocketObject, ):  # MutableSequence
    """Copy of a list object with piece table segments.

    This copy for large objects is used to avoid the need to copy the entire object
    when a small part of it is changed.

    The data is stored in segments of tubles like:
    (start, end, data), where start and end are the start and end indices of the part
    taken from the data list.
    """

    def __init__(self, r: Client | t.Any, origin: "List", key: str) -> None:
        self.redis: redis.Redis = r
        self._origin = origin
        self._key = key

        # create a new segments list
        self.redis.lpush(f"segments:{key}", json.dumps((0, len(origin), origin.key)))

    def get_raw(self) -> t.Any:
        """Get the raw data of the segments list."""
        # get all segments
        segments = self.redis.lrange(f"segments:{self._key}", 0, -1)
        # decode the segments
        return [json.loads(segment) for segment in segments]

    @classmethod
    def from_list(cls, origin: "List", key: str) -> "Segments":
        """Create a Segments object from a list."""
        r = origin.redis
        return cls(r, origin, key)

    def __len__(self) -> int:
        """Return the length of the segments list."""
        # get all segments and sum their lengths
        segments = self.redis.lrange(f"segments:{self._key}", 0, -1)
        length = 0
        for segment in segments:
            segment = json.loads(segment)
            start, end, _ = segment
            length += end - start
        return length

    def __getitem__(self, index: int) -> t.Any:
        """Get an item from the segments list."""
        from znsocket import List

        single_item = isinstance(index, int)
        if single_item:
            index = [index]
            # TODO: only quqery self.redis.lrange once with the length here as well
        if isinstance(index, slice):
            index = list(range(*index.indices(len(self))))
        # get all segments
        segments = self.redis.lrange(f"segments:{self._key}", 0, -1)
        items = []
        size = 0
        for segment in segments:
            segment = json.loads(segment)
            start, end, target = segment
            # TODO: converter and stuff
            lst = List(r=self.redis, key=target)
            # append to items in range
            for i in index:
                if size <= i < end - start + size:
                    # get the item from the list
                    items.append(lst[i - size + start])
            size += end - start
        if single_item:
            return items[0]
        return items

    def __setitem__(self, index: int, value: t.Any) -> None:
        """Set an item in the segments list."""
        from znsocket import List

        # list to store the values at
        lst = List(r=self.redis, key=self._key)
        lst.append(value)

        # TODO: update the segements, append / insert into the lst
        # [1, 2, 3, 4] -> |insert, x, 2| [1, 2, X, 3, 4]
        # (0, 3, target) -> (0, 2, target), (0, 1, new), (3, 4, target)


        # update the segments
        segments = self.redis.lrange(f"segments:{self._key}", 0, -1)
        size = 0
        for idx, segment in enumerate(segments):
            segment = json.loads(segment)
            start, end, target = segment

            if size <= index < end - start + size:
                # we are in the segment
                pos_in_segment = index - size + start
                self.redis.lset(
                    f"segments:{self._key}", idx, "__SETITEM_IDENTIFIER__"
                )
                if start < pos_in_segment:
                    self.redis.linsert(
                        f"segments:{self._key}",
                        "BEFORE",
                        "__SETITEM_IDENTIFIER__",
                        json.dumps((start, pos_in_segment, target)),
                    )
                self.redis.linsert(
                    f"segments:{self._key}",
                    "BEFORE",
                    "__SETITEM_IDENTIFIER__",
                    json.dumps((len(lst) - 1, len(lst), self._key)),
                )
                if pos_in_segment + 1 < end:
                    self.redis.linsert(
                        f"segments:{self._key}",
                        "BEFORE",
                        "__SETITEM_IDENTIFIER__",
                        json.dumps((pos_in_segment + 1, end, target)),
                    )
                self.redis.lrem(
                    f"segments:{self._key}", 0, "__SETITEM_IDENTIFIER__"
                )
                break

            size += end - start