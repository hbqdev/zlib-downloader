#!/usr/bin/env python3
"""
swap_rename.py — Swap the two segments of an ebook filename.

Usage:
    python3 swap_rename.py <filepath> [<filepath2> ...]

Each file must be named  "Seg1 - Seg2.ext".
The script renames it to "Seg2 - Seg1.ext" in the same directory.

Pass --dry-run to preview without renaming.
"""

import os
import sys


def swap_file(filepath: str, dry_run: bool = False) -> None:
    directory = os.path.dirname(filepath) or '.'
    filename = os.path.basename(filepath)
    stem, ext = os.path.splitext(filename)

    if ' - ' not in stem:
        print(f"  ⚠️  SKIP (no ' - ' separator): {filename}")
        return

    parts = stem.split(' - ', 1)
    seg1, seg2 = parts[0].strip(), parts[1].strip()
    new_stem = f"{seg2} - {seg1}"
    new_filename = new_stem + ext
    new_filepath = os.path.join(directory, new_filename)

    if new_filename == filename:
        print(f"  ℹ️  Already symmetric: {filename}")
        return

    print(f"  📝 FROM: {filename}")
    print(f"     TO:   {new_filename}")

    if not dry_run:
        if os.path.exists(new_filepath):
            print(f"     ⚠️  SKIPPED — target already exists.")
        else:
            try:
                os.rename(filepath, new_filepath)
                print(f"     ✅ Renamed.")
            except OSError as e:
                print(f"     ❌ Error: {e}")
    else:
        print(f"     (dry run)")


def main():
    args = sys.argv[1:]
    dry_run = '--dry-run' in args
    paths = [a for a in args if not a.startswith('--')]

    if not paths:
        print("Usage: python3 swap_rename.py [--dry-run] <filepath> [<filepath2> ...]")
        sys.exit(1)

    for path in paths:
        if not os.path.isfile(path):
            print(f"  ❌ Not a file: {path}")
            continue
        swap_file(path, dry_run=dry_run)


if __name__ == '__main__':
    main()
