import contextlib
import linecache
import tracemalloc
import sys


def display_top(snapshot, key_type='lineno', limit=10):
    snapshot = snapshot.filter_traces((
        tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
        tracemalloc.Filter(False, "<unknown>"),
    ))
    top_stats = snapshot.statistics(key_type)

    sys.stderr.write("Top %s lines\n" % limit)
    for index, stat in enumerate(top_stats[:limit], 1):
        frame = stat.traceback[0]
        sys.stderr.write("#%s: %s:%s: %.1f KiB\n"
              % (index, frame.filename, frame.lineno, stat.size / 1024))
        line = linecache.getline(frame.filename, frame.lineno).strip()
        if line:
            sys.stderr.write('    %s\n' % line)

    other = top_stats[limit:]
    if other:
        size = sum(stat.size for stat in other)
        sys.stderr.write("%s other: %.1f KiB\n" % (len(other), size / 1024))
    total = sum(stat.size for stat in top_stats)
    sys.stderr.write("Total allocated size: %.1f KiB\n" % (total / 1024))


@contextlib.contextmanager
def trace():
    tracemalloc.start()
    try:
        yield
    finally:
        display_top(tracemalloc.take_snapshot())
        tracemalloc.stop()