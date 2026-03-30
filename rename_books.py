"""
Batch cleanup script for Z-Library downloaded files.

- Strips "(z-library.sk, ...)" domain suffixes from filenames
- Renames files to "Authors - Title.ext" format
- Embeds clean title/author metadata into PDF and EPUB files

Usage:
    python rename_books.py /path/to/books
    python rename_books.py /path/to/books --dry-run       # preview only
    python rename_books.py /path/to/books --no-metadata   # rename only, skip embedding
    python rename_books.py /path/to/books --metadata-only # re-embed metadata on clean files
    python rename_books.py /path/to/books --workers 8     # parallel workers (default: 4)
"""

import argparse
import os
import re
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed, TimeoutError as FutureTimeout


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

FULL_SAVE_SIZE_LIMIT_MB = 30  # Skip full PDF rewrites above this size


def embed_pdf_metadata(filepath: str, title: str, authors: str) -> tuple[bool, str]:
    """Returns (success, warning_message). warning_message is empty on success."""
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(filepath)
        existing = doc.metadata or {}
        if (existing.get("title", "").strip() == title.strip() and
                existing.get("author", "").strip() == authors.strip()):
            doc.close()
            return True, ""

        doc.set_metadata({})
        doc.set_metadata({"title": title, "author": authors})

        try:
            doc.saveIncr()
            doc.close()
            return True, ""
        except Exception:
            doc.close()

        file_mb = os.path.getsize(filepath) / (1024 * 1024)
        if file_mb > FULL_SAVE_SIZE_LIMIT_MB:
            return False, f"PDF too large for full rewrite ({file_mb:.0f} MB > {FULL_SAVE_SIZE_LIMIT_MB} MB)"

        import shutil
        tmp = filepath + ".pdftmp"
        try:
            doc2 = fitz.open(filepath)
            doc2.set_metadata({})
            doc2.set_metadata({"title": title, "author": authors})
            doc2.save(tmp, garbage=4, deflate=True)
            doc2.close()
            shutil.move(tmp, filepath)
            return True, ""
        except Exception as e2:
            if os.path.exists(tmp):
                os.remove(tmp)
            return False, str(e2)

    except Exception as e:
        return False, str(e)


def embed_epub_metadata(filepath: str, title: str, authors: str) -> tuple[bool, str]:
    """Returns (success, warning_message). warning_message is empty on success."""
    tmp = None
    try:
        from ebooklib import epub  # noqa: F401
        import zipfile, shutil
        from lxml import etree

        with zipfile.ZipFile(filepath, 'r') as zcheck:
            opf_path_check = None
            try:
                container = etree.fromstring(zcheck.read("META-INF/container.xml"))
                ns = {"c": "urn:oasis:names:tc:opendocument:xmlns:container"}
                rootfile = container.find(".//c:rootfile", ns)
                if rootfile is not None:
                    opf_path_check = rootfile.get("full-path")
            except Exception:
                pass
            if opf_path_check:
                try:
                    tree = etree.fromstring(zcheck.read(opf_path_check))
                    ns_dc = "http://purl.org/dc/elements/1.1/"
                    existing_title = (tree.findtext(f"{{{ns_dc}}}title") or
                                      tree.findtext(f".//{{{ns_dc}}}title") or "").strip()
                    existing_author = (tree.findtext(f"{{{ns_dc}}}creator") or
                                       tree.findtext(f".//{{{ns_dc}}}creator") or "").strip()
                    if existing_title == title.strip() and existing_author == authors.strip():
                        return True, ""
                except Exception:
                    pass

        tmp = filepath + ".tmp"
        shutil.copy2(filepath, tmp)

        with zipfile.ZipFile(tmp, 'r') as zin, \
             zipfile.ZipFile(filepath, 'w', zipfile.ZIP_DEFLATED) as zout:
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
                    try:
                        tree = etree.fromstring(data)
                        ns_dc = "http://purl.org/dc/elements/1.1/"
                        t_el = tree.find(f".//{{{ns_dc}}}title")
                        if t_el is not None:
                            t_el.text = title
                        c_el = tree.find(f".//{{{ns_dc}}}creator")
                        if c_el is not None:
                            c_el.text = authors
                        data = etree.tostring(tree, xml_declaration=True,
                                              encoding="utf-8", pretty_print=False)
                    except Exception:
                        pass
                zout.writestr(item, data)

        os.remove(tmp)
        return True, ""
    except Exception as e:
        if tmp and os.path.exists(tmp):
            os.remove(tmp)
        return False, str(e)


# ──────────────────────────────────────────────────────────────────────────────
# Main processing
# ──────────────────────────────────────────────────────────────────────────────

SUPPORTED_EXTENSIONS = {".pdf", ".epub", ".djvu", ".mobi", ".azw", ".azw3", ".fb2", ".cbz", ".cbr"}
METADATA_EXTENSIONS = {".pdf", ".epub"}


def needs_cleaning(stem: str) -> bool:
    """Return True if this filename looks like a Z-Library mangled name."""
    return bool(_DOMAIN_SUFFIX_RE.search(stem))


def process_file(filepath: str, dry_run: bool, embed_metadata: bool,
                 metadata_only: bool = False) -> dict:
    directory = os.path.dirname(filepath)
    basename = os.path.basename(filepath)
    stem, ext = os.path.splitext(basename)
    ext_lower = ext.lower()

    if ext_lower not in SUPPORTED_EXTENSIONS:
        return {"action": "skipped", "reason": "unsupported extension"}

    if metadata_only:
        if ext_lower not in METADATA_EXTENSIONS:
            return {"action": "skipped", "reason": "not a PDF/EPUB"}
        if " - " in stem:
            authors, _, title = stem.partition(" - ")
        else:
            title, authors = stem, ""
        if not title:
            return {"action": "skipped", "reason": "could not parse title"}
        result = {"action": "metadata_only", "file": basename,
                  "title": title, "authors": authors, "metadata_embedded": False, "warn": ""}
        if not dry_run:
            if ext_lower == ".pdf":
                ok, warn = embed_pdf_metadata(filepath, title, authors)
            else:
                ok, warn = embed_epub_metadata(filepath, title, authors)
            result["metadata_embedded"] = ok
            result["warn"] = warn
        return result

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
            ok, warn = embed_pdf_metadata(filepath, title, authors)
        else:
            ok, warn = embed_epub_metadata(filepath, title, authors)
        result["metadata_embedded"] = ok
        result["warn"] = warn
    else:
        result["warn"] = ""

    return result


def _worker(args):
    """Module-level wrapper so ProcessPoolExecutor can pickle it."""
    filepath, dry_run, embed_metadata, metadata_only = args
    return filepath, process_file(filepath, dry_run, embed_metadata, metadata_only)


def process_directory(root_dir: str, dry_run: bool, embed_metadata: bool,
                      recursive: bool, metadata_only: bool = False, workers: int = 8,
                      file_timeout: int = 60):
    if not os.path.isdir(root_dir):
        print(f"❌ Directory not found: {root_dir}")
        sys.exit(1)

    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = None

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
    mode_label = "[DRY RUN] " if dry_run else ""
    if metadata_only:
        mode_label += "[METADATA ONLY] "
    print(f"{mode_label}Processing {len(file_list)} files in: {root_dir} (workers={workers}, timeout={file_timeout}s)\n")

    progress = tqdm(total=len(file_list), unit="file", dynamic_ncols=True) if tqdm else None

    def _write(msg):
        if progress:
            progress.write(msg)
        else:
            print(msg)

    job_args = [(fp, dry_run, embed_metadata, metadata_only) for fp in file_list]

    # ProcessPoolExecutor gives true isolation (each process has its own PyMuPDF state)
    # and allows enforcing per-file timeouts via future.result(timeout=N)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_worker, a): a[0] for a in job_args}
        for future in as_completed(futures):
            fp = futures[future]
            try:
                filepath, res = future.result(timeout=file_timeout)
                action = res["action"]
                warn = res.get("warn", "")
                if action in ("renamed", "dry_run"):
                    meta_tag = " + metadata" if res.get("metadata_embedded") else ""
                    tag = "📝" if action == "dry_run" else "✅"
                    _write(f"  {tag} {res['old']} → {res['new']}{meta_tag}")
                    if warn:
                        _write(f"    ⚠️  {warn}")
                    renamed += 1
                    if res.get("metadata_embedded"):
                        meta_ok += 1
                elif action == "metadata_only":
                    renamed += 1
                    if res.get("metadata_embedded"):
                        meta_ok += 1
                    elif warn and "zip file" not in warn:
                        _write(f"  ⚠️  {os.path.basename(fp)}: {warn}")
                else:
                    skipped += 1
            except FutureTimeout:
                future.cancel()
                _write(f"  ⏱️  Timeout ({file_timeout}s): {os.path.basename(fp)} — skipped")
                errors += 1
            except Exception as e:
                _write(f"  ❌ {os.path.basename(fp)}: {e}")
                errors += 1
            finally:
                if progress:
                    progress.set_postfix(renamed=renamed, meta=meta_ok, errors=errors, refresh=False)
                    progress.update(1)

    if progress:
        progress.close()

    print(f"\n{'─'*60}")
    if metadata_only:
        print(f"  Processed: {renamed}")
        print(f"  Metadata embedded: {meta_ok}/{renamed}")
    else:
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
    parser.add_argument("--metadata-only", action="store_true",
                        help="Re-embed metadata into already-renamed PDF/EPUB files "
                             "(use this if a previous run renamed but failed to embed metadata)")
    parser.add_argument("--workers", type=int, default=8,
                        help="Number of parallel workers (default: 8)")
    parser.add_argument("--timeout", type=int, default=60,
                        help="Per-file timeout in seconds; files that hang are skipped (default: 60)")
    parser.add_argument("--max-pdf-size", type=int, default=30,
                        help="Skip full PDF rewrite (fallback path) for files larger than "
                             "this many MB (default: 30). Incremental saves are always tried first.")
    args = parser.parse_args()

    global FULL_SAVE_SIZE_LIMIT_MB
    FULL_SAVE_SIZE_LIMIT_MB = args.max_pdf_size

    process_directory(
        root_dir=args.directory,
        dry_run=args.dry_run,
        embed_metadata=not args.no_metadata,
        recursive=args.recursive,
        metadata_only=args.metadata_only,
        workers=args.workers,
        file_timeout=args.timeout,
    )


if __name__ == "__main__":
    main()
