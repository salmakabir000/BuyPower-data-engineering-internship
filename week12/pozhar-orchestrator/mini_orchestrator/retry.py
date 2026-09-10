"""
retry.py - decides whether a task failure is worth retrying.

Two ways a task's error can be classified as "not worth retrying":
  1. It's an instance of NonRetryableError - task authors can raise this
     directly when they know a failure is permanent (e.g. bad input data,
     a 404 from an API that will never succeed no matter how many times
     you ask).
  2. It's one of a small set of exception types that almost always mean
     a logic/programming error, not a transient failure (ValueError,
     TypeError, KeyError) - retrying these just wastes time since the
     bug won't fix itself between attempts.

Everything else (ConnectionError, TimeoutError, OSError, and generic
requests exceptions) is treated as retryable by default, since those
usually represent transient problems - a flaky network, a downstream
service that's briefly overloaded, etc.
"""


class NonRetryableError(Exception):
    """Raise this in a task when you know retrying won't help."""
    pass


_NON_RETRYABLE_TYPES = (ValueError, TypeError, KeyError, NonRetryableError)


def is_retryable(exc):
    return not isinstance(exc, _NON_RETRYABLE_TYPES)
