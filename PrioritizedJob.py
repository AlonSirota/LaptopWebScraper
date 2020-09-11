import asyncio
from dataclasses import dataclass, field
import typing

@dataclass(order=True)
class PrioritizedJob:
    """Represents a certain function that should be run eventually,
    contains the function to be called and it's parameters.
    the priority is used to execute bottlenecking jobs first.
    """
    def __init__(self, priority, function, *args):
        self.priority = priority
        self.future = asyncio.get_event_loop().create_future()
        self.function = function
        self.args = args

    HIGH_PRIORITY = 1
    LOW_PRIOTITY = 2

    # Only compare priority when comparing two instances of this class
    priority: int
    future: asyncio.Future = field(compare=False)
    function: typing.Callable = field(compare=False)
    args: typing.List[typing.Any] = field(compare=False)
