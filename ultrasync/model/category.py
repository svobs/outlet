import itertools
from enum import IntEnum

# See: https://www.notinventedhere.org/articles/python/how-to-use-strings-as-name-aliases-in-python-enums.html
_CATEGORIES = {
    0: ['Nada', 'NA'],
    1: ['Ignored', 'IGNORED'],
    2: ['Added', 'ADDED'],
    3: ['Deleted', 'DELETED'],
    4: ['Updated', 'UPDATED'],
    5: ['Moved', 'MOVED'],
}
Category = IntEnum(
    value='Category',
    names=itertools.chain.from_iterable(
        itertools.product(v, [k]) for k, v in _CATEGORIES.items()
    )
)
