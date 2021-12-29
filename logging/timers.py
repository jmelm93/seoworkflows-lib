from dataclasses import dataclass, field
import time
from typing import Callable, ClassVar, Dict, Optional

class TimerError(Exception):
    """A custom exception used to report errors in use of Timer class"""

@dataclass 
class Timer:
    # Below are attributes on Timer - have default vals but can be specified when creating Timer 
    timers: ClassVar[Dict[str, float]] = dict() # Creates dict of all timers in single run (can use to send the dict to backend for storage - need a run ID to match up with other metrics in backend though!)
    num_format="{:0.4f}" # :0.4f is a format specifier: For toc - tic, print as decimal num w/ 4 decimals.
    text: str = f"Elapsed time for run: {num_format} seconds"
    logger: Optional[Callable[[str], None]] = print
    _start_time: Optional[float] = field(default=None, init=False, repr=False)

    # def __post_init__(self) -> None: # method for any initialization you need to do apart from setting the instance attributes
    #     """Add timer to dict of timers after initialization"""
    #     if self.name is not None: # Appends timer name to timers dict
    #         self.timers.setdefault(self.name, 0)

    def start(self, name) -> None:
        """Start a new timer"""
        self.timers.setdefault(name, 0) # Appends timer name to timers dict
        if self._start_time is not None: # If timer is already started - raise error
            raise TimerError(f"Timer is running. Use .stop() to stop it")
        self._start_time = time.perf_counter() # uses some undefined point in time as its epoch - i.e., works with smaller numbers / is more accurate

    def stop(self, name, output_len=None) -> float:
        """Stop the timer, and report the elapsed time"""
        if self._start_time is None:
            raise TimerError(f"Timer is not running. Use .start() to start it")

        length=''
        if output_len is not None: # [ Optional ] Return length of output in log if specified
            length = f' - Output Length: {output_len}'

        elapsed_time = time.perf_counter() - self._start_time # Calculate elapsed time
        self._start_time = None # set start time back to None
        if self.logger:  # Report elapsed time
            logger_value = f'{name}: {self.text}{length}'.format(elapsed_time)
            self.logger(logger_value)
            # print(self.timers) # prints all the timers in a dict object
        self.timers[name] += elapsed_time # Appends time to timer name in dict

        return elapsed_time


t = Timer()
t.start(name="Test Scripts")
var = 'hi'
t.stop(name="Test Scripts", output_len=len(var))

# t = Timer()
# t.start(name="Test Script2")
# print('hi')
# t.stop(name="Test Script2")