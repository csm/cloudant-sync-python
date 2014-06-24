from enum import Enum


class State(Enum):
    PENDING = 1
    STARTED = 2
    STOPPED = 3
    STOPPING = 4
    COMPLETE = 5
    ERROR = 6