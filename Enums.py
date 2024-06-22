from enum import Enum


class DataStatus(Enum):
    NEW = 'NEW'
    IGNORE = 'IGNORE'
    IN_PROGRESS = 'IN PROGRESS'
    COMPLETED = 'COMPLETED'
    IN_ERROR = 'IN ERROR'
