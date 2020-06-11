from enum import IntEnum


class DirConflictStrategy(IntEnum):
    MERGE = 1
    SKIP = 2
    RENAME = 3
    PROMPT_FOR_EACH = 4


class FileConflictStrategy(IntEnum):
    OVERWRITE = 1
    SKIP = 2
    RENAME = 3
    PROMPT_FOR_EACH = 4
