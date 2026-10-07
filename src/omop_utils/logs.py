import contextlib
import functools
import logging
from collections.abc import Callable, Iterator
from datetime import datetime
from os import PathLike
from pathlib import Path

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


@contextlib.contextmanager
def log_to_file(
    directory: PathLike,
    name: str,
    *,
    logger: logging.Logger | None = None,
    level: int = logging.INFO,
) -> Iterator[Path]:
    """Also write ``logger``'s records to ``<directory>/<name>_<timestamp>.log``.

    ``logger`` defaults to the root logger.
    Each call gets its own timestamped file, so reruns and resumed downloads keep earlier logs.
    The logger's level is lowered to ``level`` if needed for the duration, and the handler is removed on exit.
    Yields the log file path.
    """
    logger = logger or logging.getLogger()
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    path = directory / f"{name}_{stamp}.log"

    handler = logging.FileHandler(path)
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    old_level = logger.level
    if logger.getEffectiveLevel() > level:
        logger.setLevel(level)
    logger.addHandler(handler)
    try:
        yield path
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)
        handler.close()


def logs_to_output_dir[**P, R](fn: Callable[P, R]) -> Callable[P, R]:
    """Decorate ``fn`` to also write its module's log records to ``<output_dir>/<table>_<timestamp>.log``.

    ``fn`` must take ``output_dir`` and ``table`` as keyword arguments.
    """
    logger = logging.getLogger(fn.__module__)

    @functools.wraps(fn)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        with log_to_file(kwargs["output_dir"], kwargs["table"], logger=logger):
            return fn(*args, **kwargs)

    return wrapper
