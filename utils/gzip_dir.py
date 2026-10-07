"""Gzip the files in a directory in place, e.g. CSVs from a ``--no-gzip`` download.

Each ``<name>`` matching the pattern (recursively) is compressed to ``<name>.gz``
and the original is deleted unless ``--keep`` is given. Files are written to
``<name>.gz.tmp`` and renamed into place, so an interrupted run never leaves a
truncated ``.gz``; re-running picks up where it left off.

    python utils/gzip_dir.py out/
    python utils/gzip_dir.py out/ --pattern '*.txt' --keep
"""

import argparse
import gzip
import os
import shutil
from pathlib import Path


def gzip_file(src: Path, *, compresslevel: int = 6, keep: bool = False) -> Path:
    final = src.with_name(src.name + ".gz")
    tmp = final.with_name(final.name + ".tmp")
    with open(src, "rb") as fin, open(tmp, "wb") as f:
        # GzipFile doesn't close f, so it can still be fsynced below
        with gzip.GzipFile(
            filename=src.name, fileobj=f, mode="wb", compresslevel=compresslevel
        ) as fout:
            shutil.copyfileobj(fin, fout, length=1 << 20)
        # Without this, a machine crash after the rename could leave a
        # truncated file under the final name.
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, final)  # atomic: the file appears whole or not at all
    if not keep:
        src.unlink()
    return final


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("directory", type=Path)
    parser.add_argument(
        "--pattern",
        default="*.csv",
        help="Glob for files to compress, matched recursively (default: %(default)s)",
    )
    parser.add_argument("--compresslevel", type=int, default=6, choices=range(1, 10))
    parser.add_argument(
        "--keep", action="store_true", help="Keep the uncompressed originals"
    )
    args = parser.parse_args()

    # stale output from an interrupted run
    for tmp in args.directory.rglob("*.gz.tmp"):
        tmp.unlink()

    srcs = sorted(
        p
        for p in args.directory.rglob(args.pattern)
        if p.is_file() and p.suffix != ".gz"
    )
    for i, src in enumerate(srcs, 1):
        final = gzip_file(src, compresslevel=args.compresslevel, keep=args.keep)
        print(f"[{i}/{len(srcs)}] {final}")
    print(f"Compressed {len(srcs)} files")


if __name__ == "__main__":
    main()
