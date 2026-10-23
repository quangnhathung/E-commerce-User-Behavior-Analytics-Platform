"""Bounded host observation buffers for sessions with no wall-clock timeout."""
from streaming_monitor import Monitor


class Tail(list):
    def __init__(self, limit):
        super().__init__()
        self.limit, self.total = limit, 0

    def append(self, value):
        self.total += 1
        super().append(value)
        if len(self) > self.limit:
            del self[:len(self) - self.limit]


class LiveMonitor(Monitor):
    def __init__(self):
        super().__init__()
        self.samples, self.errors = Tail(720), Tail(100)

    def finish(self):
        report = super().finish()
        return {**report, "total_samples": self.samples.total, "total_observation_errors": self.errors.total,
                "samples_retained": len(self.samples), "bounded_history": True}
