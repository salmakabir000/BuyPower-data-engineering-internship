import json
from collections import Counter
from types import SimpleNamespace
import consumer as c

T = "sentences"

def rec(partition, offset, sentence):
    return SimpleNamespace(topic=T, partition=partition, offset=offset,
                           value=sentence if isinstance(sentence, bytes) else json.dumps({"sentence": sentence}).encode())

def batch(*records):
    d = {}
    for r in records:
        d.setdefault((r.topic, r.partition), []).append(r)
    return d

class FakeSink:
    def __init__(self): self.writes, self.fail = [], False
    def write_window(self, start, end, rows, instance):
        if self.fail: raise RuntimeError("postgres is down")
        self.writes.append((start, end, rows, instance))

class Commits:
    def __init__(self, ok=True): self.calls, self.ok = [], ok
    def __call__(self, counted):
        self.calls.append(dict(counted)); return self.ok

def make(sink=None, commits=None, lines=None, top_n=10):
    return c.WindowedWordCounter(sink or FakeSink(), 1, commits or Commits(), 30, top_n,
                                 out=(lines.append if lines is not None else (lambda s: None)))

# ---------------------------------------------------------------- 1. window math
w = c.TumblingWindow(30)
assert [w.start_for(t) for t in (3000, 3029.9, 3030, 3059)] == [3000, 3000, 3030, 3030]
assert w.advance(3001) is None and w.start == 3000          # first call just opens a window
assert w.advance(3029) is None                              # still inside it
w.add(["a", "b"]); w.add(["a"])
closed = w.advance(3030)
assert closed == (3000, 3030, Counter({"a": 2, "b": 1}), 2) and w.start == 3030
skipped = w.advance(3200)                                   # long gap: empty windows are not reported
assert skipped[3] == 0 and w.start == 3180
print("1. window boundaries, closing, long gaps: OK")

# ---------------------------------------------------------------- 2. top words
assert c.top_words(Counter({"b": 2, "a": 2, "c": 5, "d": 1}), 3) == [("c", 5), ("a", 2), ("b", 2)]
print("2. top-N with alphabetical tie-break: OK")

# ---------------------------------------------------------------- 3. the key invariant
sink, commits = FakeSink(), Commits()
k = make(sink, commits)
k.process(batch(rec(0, 0, "the cat"), rec(1, 0, "the dog")), 3001)
k.process(batch(rec(0, 1, "the end")), 3010)
k.process(batch(rec(1, 1, "new window words")), 3031)       # closes window 1, THEN counts this batch
assert sink.writes[0] == (3000, 3030, [("the", 3), ("cat", 1), ("dog", 1), ("end", 1)], 1)
assert commits.calls[0] == {(T, 0): 2, (T, 1): 1}, commits.calls[0]
print("   commit after window 1 =", commits.calls[0], "(partition 1's offset 1 arrived after the boundary, so it is NOT included)")
k.process({}, 3062)                                          # quiet poll still closes window 2
assert sink.writes[1][:2] == (3030, 3060) and [r[0] for r in sink.writes[1][2]] == ["new", "window", "words"]
assert commits.calls[1] == {(T, 0): 2, (T, 1): 2}
print("3. offsets committed only for sentences inside the closed window; quiet polls still close windows: OK")

# ---------------------------------------------------------------- 4. crash + restart: no double-counting, nothing missing
class Broker:
    def __init__(self):
        self.logs = {p: [f"w{(p * 12 + i) % 4} common" for i in range(12)] for p in range(3)}
        self.committed = {}
class FakeConsumer:
    def __init__(self, broker):
        self.b = broker
        self.pos = {p: broker.committed.get((T, p), 0) for p in broker.logs}   # resume from the last commit
    def poll(self, per_partition=2):
        out = []
        for p, log in self.b.logs.items():
            for _ in range(per_partition):
                if self.pos[p] < len(log):
                    out.append(rec(p, self.pos[p], log[self.pos[p]])); self.pos[p] += 1
        return batch(*out)

def commit_to(broker):
    return lambda counted: (broker.committed.update({k_: v for k_, v in counted.items()}) or True)

expected = Counter()
for log in Broker().logs.values():
    for s in log: expected.update(c.tokenize(s))

broker = Broker()
sink1, sink2 = FakeSink(), FakeSink()
fc1 = FakeConsumer(broker)
run1 = c.WindowedWordCounter(sink1, 1, commit_to(broker), 30, 10, out=lambda s: None)
run1.process(fc1.poll(), 6001)                  # window A fills
run1.process(fc1.poll(), 6031)                  # closes A (saved + committed), window B fills
sink1.fail = True
try:
    run1.process(fc1.poll(), 6061)              # closing B: Postgres is down -> crash
    raise AssertionError("expected a crash")
except RuntimeError:
    pass
assert broker.committed == {(T, 0): 2, (T, 1): 2, (T, 2): 2}, broker.committed
print("   after the crash, only window A is committed:", broker.committed)

fc2 = FakeConsumer(broker)                       # restart: Kafka resumes from the committed offsets
run2 = c.WindowedWordCounter(sink2, 1, commit_to(broker), 30, 10, out=lambda s: None)
t = 7001
while any(fc2.pos[p] < len(broker.logs[p]) for p in broker.logs):
    run2.process(fc2.poll(), t); t += 31
run2.process({}, t + 31)                         # close the last window

total = Counter()
keys = Counter()
for (s, e, rows, inst) in sink1.writes + sink2.writes:
    for word, n in rows:
        total[word] += n; keys[(s, inst, word)] += 1
assert total == expected, (total, expected)
assert max(keys.values()) == 1
assert broker.committed == {(T, p): 12 for p in range(3)}
print("4. crash + restart: counts across all windows == counts in the topic; no duplicates; all offsets committed: OK")

# ---------------------------------------------------------------- 5. rebalance mid-window
sink, commits, lines = FakeSink(), Commits(), []
k = make(sink, commits, lines)
msgs = [rec(0, 0, "the cat"), rec(0, 1, "the dog"), rec(1, 0, "the bird")]
k.process(batch(*msgs), 9001)
k.on_revoked([0, 1, 2])                          # another consumer joined: Kafka takes everything back
assert k.window.sentences == 0 and k.counted == {} and k.committed == {}
k.process(batch(*msgs), 9005)                    # Kafka re-delivers from the last commit (nothing committed yet)
k.process({}, 9031)
assert sink.writes[0][2][0] == ("the", 3), sink.writes[0]          # counted once, not twice
assert commits.calls == [{(T, 0): 2, (T, 1): 1}]
assert any("Rebalance" in l for l in lines)
print("5. rebalance drops the unfinished window; re-read sentences are counted once; no stale commits: OK")

# ---------------------------------------------------------------- 6. malformed messages
sink, commits = FakeSink(), Commits()
k = make(sink, commits)
k.process(batch(rec(0, 0, "good one"), rec(0, 1, b"not json"), rec(0, 2, b'{"nope": 1}'), rec(0, 3, b"[1]")), 3001)
k.process({}, 3031)
assert k.skipped == 3 and sink.writes[0][2] == [("good", 1), ("one", 1)]
assert commits.calls[0] == {(T, 0): 4}           # skipped messages still move the offset forward
print("6. malformed messages skipped but offsets still advance: OK")

# ---------------------------------------------------------------- 7. empty window
sink, commits = FakeSink(), Commits()
k = make(sink, commits)
k.process({}, 3001); k.process({}, 3031); k.process({}, 3062)
assert sink.writes == [] and commits.calls == []
print("7. empty windows write nothing and don't crash: OK")

# ---------------------------------------------------------------- 8. postgres failure
sink, commits = FakeSink(), Commits()
k = make(sink, commits)
k.process(batch(rec(0, 0, "hello world")), 3001)
sink.fail = True
try:
    k.process({}, 3031); raise AssertionError("should have raised")
except RuntimeError:
    pass
assert commits.calls == []                       # nothing committed, so a restart re-reads the sentences
print("8. Postgres failure: error surfaces and NOTHING is committed: OK")

# ---------------------------------------------------------------- 9. commit failure
sink, commits, lines = FakeSink(), Commits(ok=False), []
k = make(sink, commits, lines)
k.process(batch(rec(0, 0, "one two")), 3001)
k.process(batch(rec(0, 1, "three")), 3031)       # window 1 saved, commit fails
assert len(sink.writes) == 1 and k.committed == {}
commits.ok = True
k.process({}, 3062)                              # window 2 saved, commit now works and covers both
assert len(sink.writes) == 2 and k.committed == {(T, 0): 2}
print("9. failed commit leaves state honest; the next commit catches up; no window written twice: OK")

# ---------------------------------------------------------------- 10. run loop + live report + shutdown
lines = []
clock = {"t": 4000.0, "m": 0.0}
class Polls:
    n = 0
    def poll(self, timeout_ms=0):
        Polls.n += 1
        if Polls.n > 5: raise KeyboardInterrupt
        return batch(rec(0, Polls.n, "alpha beta alpha")) if Polls.n <= 3 else {}
def tick(): clock["t"] += 1.0; return clock["t"]
def mono(): clock["m"] += 4.0; return clock["m"]
k = make(FakeSink(), Commits(), lines)
c.run(k, Polls(), report_interval=10.0, clock_time=tick, mono=mono)
assert any("so far" in l for l in lines) and any("Stopping" in l for l in lines)
print("10. run loop: live reports fire, Ctrl+C discards the unfinished window cleanly: OK")

print("\nALL WINDOW TESTS PASSED")
