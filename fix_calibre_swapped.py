#!/usr/bin/env python3
"""
fix_calibre_swapped.py — Move books with swapped author/title out of Calibre library,
remove them from Calibre's database, so they can be re-added cleanly after fixing filenames.

Targets the latest 3120 books added (IDs 7665–10784) which have title/author swapped.

Usage (on serenity):
    python3 fix_calibre_swapped.py            # dry run — shows what would happen
    python3 fix_calibre_swapped.py --apply    # actually move files and remove DB records

After running with --apply:
    1. Run fix_filenames.py on /mnt/y/bookstofix to correct filenames
    2. Re-add the fixed files to Calibre
"""

import argparse
import os
import shutil
import sqlite3
import sys

CALIBRE_LIBRARY = "/mnt/b/LitServer/Ebooks"
DEST_DIR        = "/mnt/y/bookstofix"
DB_PATH         = os.path.join(CALIBRE_LIBRARY, "metadata.db")

# The first book ID with swapped metadata (confirmed by inspection)
FIRST_BAD_ID    = 7665


def main():
    parser = argparse.ArgumentParser(
        description="Move Calibre books with swapped author/title to bookstofix and remove from DB."
    )
    parser.add_argument("--apply", action="store_true", help="Actually move files and update DB (default: dry run)")
    args = parser.parse_args()
    apply = args.apply

    if not os.path.exists(DB_PATH):
        print(f"❌ Calibre database not found at: {DB_PATH}")
        sys.exit(1)

    if apply and not os.path.isdir(DEST_DIR):
        print(f"❌ Destination directory not found: {DEST_DIR}")
        sys.exit(1)

    if not apply:
        print("ℹ️  DRY RUN — pass --apply to actually move files and remove DB records.\n")

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # Fetch all affected books with their file info
    cur.execute("""
        SELECT b.id, b.title, b.path,
               d.format, d.name AS data_name
        FROM books b
        LEFT JOIN data d ON d.book = b.id
        WHERE b.id >= ?
        ORDER BY b.id
    """, (FIRST_BAD_ID,))
    rows = cur.fetchall()

    # Group by book id
    books = {}
    for row in rows:
        bid = row["id"]
        if bid not in books:
            books[bid] = {
                "id":    bid,
                "title": row["title"],
                "path":  row["path"],
                "files": [],
            }
        if row["format"]:
            books[bid]["files"].append({
                "format":    row["format"],
                "data_name": row["data_name"],
            })

    print(f"📚 Found {len(books)} books to process (IDs {FIRST_BAD_ID}–{max(books.keys())})\n")

    moved = 0
    already_missing = 0
    errors = 0
    ids_to_delete = []

    for bid, book in sorted(books.items()):
        book_folder = os.path.join(CALIBRE_LIBRARY, book["path"])
        files_moved_for_book = []

        for f in book["files"]:
            fname = f"{f['data_name']}.{f['format'].lower()}"
            src = os.path.join(book_folder, fname)
            dst = os.path.join(DEST_DIR, fname)

            if not os.path.exists(src):
                # Try case-insensitive fallback
                if os.path.isdir(book_folder):
                    matches = [x for x in os.listdir(book_folder) if x.lower() == fname.lower()]
                    if matches:
                        src = os.path.join(book_folder, matches[0])
                        dst = os.path.join(DEST_DIR, matches[0])

            if not os.path.exists(src):
                print(f"  ⚠️  [{bid}] File not found: {src}")
                already_missing += 1
                continue

            # Handle destination name collision
            if os.path.exists(dst):
                base, ext = os.path.splitext(os.path.basename(dst))
                dst = os.path.join(DEST_DIR, f"{base}_{bid}{ext}")

            print(f"  📦 [{bid}] {os.path.basename(src)}")
            print(f"         → {dst}")

            if apply:
                try:
                    shutil.move(src, dst)
                    files_moved_for_book.append(src)
                    moved += 1
                except Exception as e:
                    print(f"         ❌ Move failed: {e}")
                    errors += 1
                    continue
            else:
                moved += 1

        # Remove the now-empty book folder, then its parent if also empty
        if apply and os.path.isdir(book_folder):
            remaining = os.listdir(book_folder)
            ebook_remaining = [x for x in remaining if any(
                x.lower().endswith(ext) for ext in
                ['.epub','.pdf','.mobi','.azw','.azw3','.djvu','.fb2','.lit','.cbz','.cbr']
            )]
            if not ebook_remaining:
                try:
                    shutil.rmtree(book_folder)
                    print(f"         🗑️  Removed folder: {book_folder}")
                    # Remove parent folder too if it's now empty
                    parent_folder = os.path.dirname(book_folder)
                    if parent_folder != CALIBRE_LIBRARY and os.path.isdir(parent_folder) and not os.listdir(parent_folder):
                        os.rmdir(parent_folder)
                        print(f"         🗑️  Removed empty parent: {os.path.basename(parent_folder)}")
                except Exception as e:
                    print(f"         ⚠️  Could not remove folder {book_folder}: {e}")
            else:
                print(f"         ⚠️  Folder still has ebook files, not removing: {book_folder}")

        ids_to_delete.append(bid)

    print()

    if apply and ids_to_delete:
        print(f"🗄️  Removing {len(ids_to_delete)} book records from Calibre database...")
        try:
            # Calibre's delete trigger cascades to all related tables automatically
            placeholders = ",".join("?" * len(ids_to_delete))
            cur.execute(f"DELETE FROM books WHERE id IN ({placeholders})", ids_to_delete)
            con.commit()
            print(f"✅ Removed {cur.rowcount} records from database.")
        except Exception as e:
            print(f"❌ Database delete failed: {e}")
            con.rollback()
            errors += 1
    elif not apply:
        print(f"📋 Would remove {len(ids_to_delete)} book records from database.")

    con.close()

    print()
    if apply:
        print(f"✅ Done. Files moved: {moved} | Missing/skipped: {already_missing} | Errors: {errors}")
        if moved > 0:
            print(f"\nNext steps:")
            print(f"  1. Run: python3 fix_filenames.py {DEST_DIR} --apply")
            print(f"  2. Re-add the fixed files to Calibre from {DEST_DIR}")
    else:
        print(f"📋 Dry run complete. Would move: {moved} files | Already missing: {already_missing}")
        print("   Run with --apply to proceed.")


if __name__ == "__main__":
    main()
