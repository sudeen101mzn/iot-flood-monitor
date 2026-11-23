"""Exponential reconnect backoff with jitter.

delay(n) = min(max_s, min_s * factor**n), randomised by +/- jitter so that many
nodes that lost the network together do not reconnect in the same second.

Why the limits matter on a battery node: every attempt costs a modem attach plus a
TLS handshake (Task 2 budget: roughly 16-19 s of active modem time). A short
minimum recovers quickly after a brief glitch; a long maximum caps the energy spent
on attempts during a long outage. Demo values (1 s / 30 s) are scaled down so that
recovery is visible in a 15-minute demonstration; field values are in the manuscript.
"""
import random


class Backoff:
    def __init__(self, min_s=1.0, max_s=30.0, factor=2.0, jitter=0.2, rng=None):
        self.min_s, self.max_s, self.factor, self.jitter = min_s, max_s, factor, jitter
        self.attempt = 0                      # number of consecutive failures so far
        self._rng = rng or random.Random()

    def next_delay(self):
        """Delay before the next attempt; call once per failed attempt."""
        base = min(self.max_s, self.min_s * (self.factor ** self.attempt))
        self.attempt += 1
        return max(0.0, base * (1 + self._rng.uniform(-self.jitter, self.jitter)))

    def reset(self):
        self.attempt = 0
