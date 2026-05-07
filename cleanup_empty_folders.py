#!/usr/bin/env python3
r"""
cleanup_empty_folders.py — Remove empty subdirectories under the Calibre library.

Removes depth-1 and depth-2 empty folders under B:\LitServer\Ebooks.
Never removes the library root itself.

Usage (on serenity):
    python3 cleanup_empty_folders.py            # dry run
    python3 cleanup_empty_folders.py --apply    # actually delete
"""

import argparse
import os
import sys

CALIBRE_LIBRARY = "/mnt/b/LitServer/Ebooks"


def main():
    parser = argparse.ArgumentParser(description="Remove empty folders under Calibre library.")
    parser.add_argument("--apply", action="store_true", help="Actually delete folders (default: dry run)")
    args = parser.parse_args()
    apply = args.apply

    if not os.path.isdir(CALIBRE_LIBRARY):
        print(f"❌ Directory not found: {CALIBRE_LIBRARY}")
        sys.exit(1)

    if not apply:
        print("ℹ️  DRY RUN — pass --apply to actually delete folders.\n")

    removed = 0
    would_remove = 0

    # Two passes: depth 2 first, then depth 1 (so parents are re-evaluated after children gone)
    for depth in (2, 1):
        for entry in os.scandir(CALIBRE_LIBRARY):
            if not entry.is_dir():
                continue
            if depth == 1:
                target = entry.path
                label = entry.name
                if not os.listdir(target):
                    print(f"  🗑️  {'Removing' if apply else 'Would remove'}: {label}/")
                    if apply:
                        try:
                            os.rmdir(target)
                            removed += 1
                        except Exception as e:
                            print(f"     ⚠️  Failed: {e}")
                    else:
                        would_remove += 1
            elif depth == 2:
                try:
                    for sub in os.scandir(entry.path):
                        if sub.is_dir() and not os.listdir(sub.path):
                            label = f"{entry.name}/{sub.name}"
                            print(f"  🗑️  {'Removing' if apply else 'Would remove'}: {label}/")
                            if apply:
                                try:
                                    os.rmdir(sub.path)
                                    removed += 1
                                except Exception as e:
                                    print(f"     ⚠️  Failed: {e}")
                            else:
                                would_remove += 1
                except PermissionError:
                    pass

    print()
    if apply:
        print(f"✅ Done. Removed {removed} empty folder(s).")
    else:
        print(f"📋 Dry run complete. Would remove {would_remove} empty folder(s).")
        if would_remove:
            print("   Run with --apply to delete them.")


if __name__ == "__main__":
    main()
