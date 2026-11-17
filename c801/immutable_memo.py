"""Bounded reuse of deterministic immutable results, never state validity.

Callers must reread every current input and still perform all chain, membership,
clock and materialized-state checks. No decoded mutable object is retained.
"""
import sys
from threading import RLock


class ImmutableMemo:
    def __init__(self, maximum_bytes):
        if type(maximum_bytes) is not int or maximum_bytes < 0:
            raise ValueError('invalid immutable memo capacity')
        self.maximum_bytes = maximum_bytes
        self._lock = RLock()
        self._policy = None
        self._values = {}
        self._object_bytes = 0

    def clear(self):
        with self._lock:
            self._values = {}
            self._object_bytes = 0
            self._policy = None

    def retained_bytes(self):
        with self._lock:
            return self._object_bytes + (sys.getsizeof(self._values) if self._values else 0)

    def value(self, key, compute, *, policy):
        # Strict scalar types prevent Python's bool/int/float equality from
        # reusing a result whose original serialization would be different.
        if type(key) is not tuple or any(type(v) not in (int, str, bytes) for v in key):
            return compute()
        with self._lock:
            if self._policy != policy:
                self.clear()
                self._policy = policy
            if key in self._values:
                return self._values[key]
        result = compute()  # exceptions are never retained
        if type(result) not in (str, bytes):
            return result
        charge = sys.getsizeof(key) + sum(sys.getsizeof(v) for v in key) + sys.getsizeof(result)
        with self._lock:
            if self._policy != policy:
                return result
            if key in self._values:
                return self._values[key]
            # Keep the useful prefix when full. Include dictionary growth,
            # then verify its actual retained size after insertion as well.
            if self._object_bytes + charge + 2 * sys.getsizeof(self._values) + 512 > self.maximum_bytes:
                return result
            self._values[key] = result
            self._object_bytes += charge
            if self.retained_bytes() > self.maximum_bytes:
                self.clear()
        return result
