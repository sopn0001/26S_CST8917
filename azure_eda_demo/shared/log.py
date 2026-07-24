"""Console output helpers, so the demo is readable from the back row."""
import sys
import threading
import time

_LOCK = threading.Lock()

RESET = "\033[0m"
DIM = "\033[2m"
BOLD = "\033[1m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
CYAN = "\033[36m"
MAGENTA = "\033[35m"


def _emit(colour: str, tag: str, message: str) -> None:
    with _LOCK:
        stamp = time.strftime("%H:%M:%S")
        sys.stdout.write(f"{DIM}{stamp}{RESET} {colour}{BOLD}{tag:<9}{RESET} {message}\n")
        sys.stdout.flush()


def sent(message: str) -> None:
    _emit(CYAN, "SENT", message)


def received(message: str) -> None:
    _emit(GREEN, "RECEIVED", message)


def done(message: str) -> None:
    _emit(GREEN, "COMPLETE", message)


def warn(message: str) -> None:
    _emit(YELLOW, "WARN", message)


def fail(message: str) -> None:
    _emit(RED, "FAILED", message)


def dead(message: str) -> None:
    _emit(RED, "DEADLTR", message)


def skip(message: str) -> None:
    _emit(MAGENTA, "SKIPPED", message)


def info(message: str) -> None:
    _emit(RESET, "INFO", message)


def banner(title: str, subtitle: str = "") -> None:
    with _LOCK:
        print()
        print(f"{BOLD}{'=' * 68}{RESET}")
        print(f"{BOLD}  {title}{RESET}")
        if subtitle:
            print(f"{DIM}  {subtitle}{RESET}")
        print(f"{BOLD}{'=' * 68}{RESET}")
        print()


class SeenStore:
    """The simplest possible idempotency guard.

    In production this is a table with a unique constraint, or a Redis SET
    with a TTL, written in the SAME transaction as the business change.
    An in-memory set is enough to demonstrate the shape.
    """

    def __init__(self) -> None:
        self._seen: set[str] = set()

    def already_processed(self, key: str) -> bool:
        return key in self._seen

    def mark(self, key: str) -> None:
        self._seen.add(key)
