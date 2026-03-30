"""
Batch cleanup script for Z-Library downloaded files.

- Strips "(z-library.sk, ...)" domain suffixes from filenames
- Renames files to "Authors - Title.ext" format
- Embeds clean title/author metadata into PDF and EPUB files

Usage:
    python rename_books.py /path/to/books
    python rename_books.py /path/to/books --dry-run     # preview only
    python rename_books.py /path/to/books --no-metadata  # rename only, skip embedding
"""

import argparse
import os
import re
import sys


# ──────────────────────────────────────────────────────────────────────────────
# Filename parsing
# ──────────────────────────────────────────────────────────────────────────────

# Matches the trailing "(z-library.sk, 1lib.sk, ...)" or similar domain group
_DOMAIN_SUFFIX_RE = re.compile(
    r'\s*\([^)]*(?:z-library|z-lib|1lib|singlelogin|zlibrary)[^)]*\)\s*$',
    re.IGNORECASE,
)

# Matches the author group: last parenthesised block that looks like names
# e.g. "(Helen J Chatterjee, Guy Noble)"
_AUTHOR_GROUP_RE = re.compile(r'\(([^()]+)\)\s*$')


def parse_zlib_filename(stem: str) -> tuple[str, str]:
    """Return (title, authors) parsed from a Z-Library filename stem.

    Input:  "Museums, Health and Well-Being (Helen J Chatterjee, Guy Noble) (z-library.sk, 1lib.sk, z-lib.sk)"
    Output: ("Museums, Health and Well-Being", "Helen J Chatterjee, Guy Noble")
    """
    # 1. Strip domain suffix(es) — may appear multiple times
    cleaned = stem
    while True:
        new = _DOMAIN_SUFFIX_RE.sub('', cleaned).rstrip()
        if new == cleaned:
            break
        cleaned = new

    # 2. Try to extract author group (last parenthesised block)
    m = _AUTHOR_GROUP_RE.search(cleaned)
    if m:
        authors = m.group(1).strip()
        title = cleaned[:m.start()].strip().rstrip(',').strip()
    else:
        authors = ""
        title = cleaned.strip()

    return title, authors


def safe_filename(name: str, max_len: int = 180) -> str:
    """Sanitize a string for use as a filename component."""
    # Remove characters illegal on Windows/Linux/macOS
    name = re.sub(r'[\\/:*?"<>|]', '', name)
    # Collapse multiple spaces/dashes
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:max_len]


def build_new_stem(title: str, authors: str) -> str:
    """Build 'Authors - Title' or just 'Title' if no authors."""
    title_safe = safe_filename(title)
    authors_safe = safe_filename(authors)
    if authors_safe:
        return f"{authors_safe} - {title_safe}"
    return title_safe


# ──────────────────────────────────────────────────────────────────────────────
# Metadata embedding
# ──────────────────────────────────────────────────────────────────────────────

def embed_pdf_metadata(filepath: str, title: str, authors: str) -> bool:
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(filepath)
        doc.set_metadata({
            "title": title,
            "author": authors,
        })
        doc.saveIncr()
        doc.close()
        return True
    except Exception as e:
        print(f"    ⚠️  PDF metadata embed failed: {e}")
        return False


def embed_epub_metadata(filepath: str, title: str, authors: str) -> bool:
    try:
        from ebooklib import epub
        import zipfile, shutil, tempfile
        from lxml import etree

        # ebooklib's write_epub requires a full round-trip; patch the OPF directly
        # by editing the zip in-place which is safer for large files.
        tmp = filepath + ".tmp"
        shutil.copy2(filepath, tmp)

        with zipfile.ZipFile(tmp, 'r') as zin, \
             zipfile.ZipFile(filepath, 'w', zipfile.ZIP_DEFLATED) as zout:
            # Find the OPF file path from META-INF/container.xml
            opf_path = None
            try:
                container = etree.fromstring(zin.read("META-INF/container.xml"))
                ns = {"c": "urn:oasis:names:tc:opendocument:xmlns:container"}
                rootfile = container.find(".//c:rootfile", ns)
                if rootfile is not None:
                    opf_path = rootfile.get("full-path")
            except Exception:
                pass

            for item in zin.infolist():
                data = zin.read(item.filename)
                if opf_path and item.filename == opf_path:
                    # Patch title and creator in OPF XML
                    try:
                        tree = etree.fromstring(data)
                        ns_dc = "http://purl.org/dc/elements/1.1/"
                        # Update or create dc:title
                        t_el = tree.find(f".//{{{ns_dc}}}title")
                        if t_el is not None:
                            t_el.text = title
                        # Update or create dc:creator
                        c_el = tree.find(f".//{{{ns_dc}}}creator")
                        if c_el is not None:
                            c_el.text = authors
                        data = etree.tostring(tree, xml_declaration=True,
                                              encoding="utf-8", pretty_print=False)
                    except Exception:
                        pass  # leave data unchanged if parse fails
                zout.writestr(item, data)

        os.remove(tmp)
        return True
    except Exception as e:
        print(f"    ⚠️  EPUB metadata embed failed: {e}")
        if os.path.exists(tmp):
            os.remove(tmp)
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Main processing
# ──────────────────────────────────────────────────────────────────────────────

SUPPORTED_EXTENSIONS = {".pdf", ".epub", ".djvu", ".mobi", ".azw", ".azw3", ".fb2", ".cbz", ".cbr"}
METADATA_EXTENSIONS = {".pdf", ".epub"}


def needs_cleaning(stem: str) -> bool:
    """Return True if this filename looks like a Z-Library mangled name."""
    return bool(_DOMAIN_SUFFIX_RE.search(stem))


def process_file(filepath: str, dry_run: bool, embed_metadata: bool) -> dict:
    directory = os.path.dirname(filepath)
    basename = os.path.basename(filepath)
    stem, ext = os.path.splitext(basename)
    ext_lower = ext.lower()

    if ext_lower not in SUPPORTED_EXTENSIONS:
        return {"action": "skipped", "reason": "unsupported extension"}

    if not needs_cleaning(stem):
        return {"action": "skipped", "reason": "no domain suffix found"}

    title, authors = parse_zlib_filename(stem)

    if not title:
        return {"action": "skipped", "reason": "could not parse title"}

    new_stem = build_new_stem(title, authors)
    new_basename = new_stem + ext
    new_filepath = os.path.join(directory, new_basename)

    # Avoid collisions
    if new_filepath != filepath and os.path.exists(new_filepath):
        counter = 1
        while os.path.exists(new_filepath):
            new_filepath = os.path.join(directory, f"{new_stem} ({counter}){ext}")
            counter += 1
        new_basename = os.path.basename(new_filepath)

    result = {
        "action": "renamed",
        "old": basename,
        "new": new_basename,
        "title": title,
        "authors": authors,
        "metadata_embedded": False,
    }

    if dry_run:
        result["action"] = "dry_run"
        return result

    # Rename
    if new_filepath != filepath:
        os.rename(filepath, new_filepath)
        filepath = new_filepath

    # Embed metadata
    if embed_metadata and ext_lower in METADATA_EXTENSIONS:
        if ext_lower == ".pdf":
            result["metadata_embedded"] = embed_pdf_metadata(filepath, title, authors)
        elif ext_lower == ".epub":
            result["metadata_embedded"] = embed_epub_metadata(filepath, title, authors)

    return result


def process_directory(root_dir: str, dry_run: bool, embed_metadata: bool, recursive: bool):
    if not os.path.isdir(root_dir):
        print(f"❌ Directory not found: {root_dir}")
        sys.exit(1)

    renamed = skipped = errors = meta_ok = 0
    file_list = []

    if recursive:
        for dirpath, _, filenames in os.walk(root_dir):
            for f in filenames:
                file_list.append(os.path.join(dirpath, f))
    else:
        file_list = [os.path.join(root_dir, f) for f in os.listdir(root_dir)
                     if os.path.isfile(os.path.join(root_dir, f))]

    file_list.sort()
    print(f"{'[DRY RUN] ' if dry_run else ''}Processing {len(file_list)} files in: {root_dir}\n")

    for filepath in file_list:
        try:
            res = process_file(filepath, dry_run, embed_metadata)
            action = res["action"]
            if action in ("renamed", "dry_run"):
                tag = "📝" if action == "dry_run" else "✅"
                meta_tag = " + metadata" if res.get("metadata_embedded") else ""
                print(f"  {tag} {res['old']}")
                print(f"     → {res['new']}{meta_tag}")
                renamed += 1
                if res.get("metadata_embedded"):
                    meta_ok += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"  ❌ Error processing {os.path.basename(filepath)}: {e}")
            errors += 1

    print(f"\n{'─'*60}")
    mode = "Would rename" if dry_run else "Renamed"
    print(f"  {mode}:  {renamed}")
    print(f"  Skipped: {skipped}")
    if embed_metadata and not dry_run:
        print(f"  Metadata embedded: {meta_ok}/{renamed}")
    if errors:
        print(f"  Errors:  {errors}")


def main():
    parser = argparse.ArgumentParser(
        description="Rename and fix metadata for Z-Library downloaded books."
    )
    parser.add_argument("directory", help="Directory containing downloaded books")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without renaming anything")
    parser.add_argument("--no-metadata", action="store_true",
                        help="Skip embedding metadata into files (rename only)")
    parser.add_argument("--recursive", action="store_true",
                        help="Process subdirectories recursively")
    args = parser.parse_args()

    process_directory(
        root_dir=args.directory,
        dry_run=args.dry_run,
        embed_metadata=not args.no_metadata,
        recursive=args.recursive,
    )


if __name__ == "__main__":
    main()
