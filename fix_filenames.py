#!/usr/bin/env python3
"""
fix_filenames.py — Rename ebook files from 'Title - Author.ext' to 'Author - Title.ext'.

Uses embedded metadata (epub OPF / PDF info) to identify which segment of the filename
is the author, then swaps the segments if the file is in 'Title - Author' order.
The actual filename text is preserved (not replaced with potentially-messy metadata text).

Usage:
    python fix_filenames.py [directory] [--apply]

    directory   Directory to scan (defaults to output_dir from config.json)
    --apply     Actually rename files (without this flag, runs in dry-run mode)
"""

import argparse
import json
import os
import re
import sys
import zipfile
import xml.etree.ElementTree as ET

import fitz  # PyMuPDF


SUPPORTED_EXTENSIONS = {'.epub', '.pdf', '.mobi', '.azw', '.azw3', '.djvu', '.fb2', '.lit', '.txt'}

MAX_STEM_BYTES = 180


# ---------------------------------------------------------------------------
# Metadata extraction
# ---------------------------------------------------------------------------

def _read_epub_metadata(filepath: str) -> tuple[str | None, str | None]:
    """Return (title, author) from an epub's OPF metadata, or (None, None) on failure."""
    try:
        with zipfile.ZipFile(filepath, 'r') as zf:
            names = zf.namelist()

            # Find the OPF file via META-INF/container.xml
            opf_path = None
            if 'META-INF/container.xml' in names:
                container_xml = zf.read('META-INF/container.xml').decode('utf-8', errors='replace')
                root = ET.fromstring(container_xml)
                for elem in root.iter():
                    if elem.tag.endswith('rootfile'):
                        opf_path = elem.get('full-path')
                        break

            # Fallback: find any .opf file
            if not opf_path:
                opf_path = next((n for n in names if n.endswith('.opf')), None)

            if not opf_path or opf_path not in names:
                return None, None

            opf_xml = zf.read(opf_path).decode('utf-8', errors='replace')
            root = ET.fromstring(opf_xml)

            ns = {
                'dc': 'http://purl.org/dc/elements/1.1/',
                'opf': 'http://www.idpf.org/2007/opf',
            }

            title = None
            author = None

            title_elem = root.find('.//dc:title', ns)
            if title_elem is not None and title_elem.text:
                title = title_elem.text.strip()

            # Prefer the first 'aut' role creator; fall back to the first creator
            creators = root.findall('.//dc:creator', ns)
            for c in creators:
                role = c.get('{http://www.idpf.org/2007/opf}role', '')
                if role == 'aut' and c.text:
                    author = c.text.strip()
                    break
            if not author and creators and creators[0].text:
                author = creators[0].text.strip()

            return title or None, author or None

    except Exception:
        return None, None


def _read_pdf_metadata(filepath: str) -> tuple[str | None, str | None]:
    """Return (title, author) from a PDF's metadata, or (None, None) on failure."""
    try:
        doc = fitz.open(filepath)
        meta = doc.metadata
        doc.close()
        title = (meta.get('title') or '').strip() or None
        author = (meta.get('author') or '').strip() or None
        return title, author
    except Exception:
        return None, None


def get_metadata(filepath: str) -> tuple[str | None, str | None]:
    """Return (title, author) from embedded metadata based on file extension."""
    ext = os.path.splitext(filepath)[1].lower()
    if ext == '.epub':
        return _read_epub_metadata(filepath)
    if ext == '.pdf':
        return _read_pdf_metadata(filepath)
    return None, None


# ---------------------------------------------------------------------------
# Author normalization (for comparison only — we keep the original filename text)
# ---------------------------------------------------------------------------

def _normalize_author_for_cmp(author: str) -> str:
    """Normalize an author string for fuzzy matching (comparison only, not for filenames)."""
    # ALL CAPS → Title Case
    if author == author.upper() and len(author.replace(' ', '')) > 3:
        author = author.title()
    # "Last, First [extra]" → "First Last"
    if ',' in author:
        parts = author.split(',', 1)
        last = parts[0].strip()
        rest = parts[1].strip()
        # Strip year/role suffixes: "1976-", "author", "editor", etc.
        rest = re.sub(r'\b\d{4}[-–]?\b.*', '', rest).strip()
        rest = re.sub(r'\b(author|editor|illustrator|translator|compiler)\b', '', rest,
                      flags=re.IGNORECASE).strip()
        first = rest.split()[0] if rest.split() else ''
        author = f"{first} {last}".strip() if first else last
    # Lowercase, strip punctuation for word-set comparison
    author = author.lower()
    author = re.sub(r'[^\w\s]', ' ', author)
    return ' '.join(author.split())


def _segment_matches_author(segment: str, author_meta: str) -> bool:
    """Return True if a filename segment (e.g. 'Alan Belkin') matches an author from metadata.

    Requires bidirectional overlap:
    - All author words must appear in the segment (author is fully represented).
    - The segment has at most 1 word not in the author (prevents long title segments
      from matching a short author name like a single word).
    """
    seg_words = set(_normalize_author_for_cmp(segment).split())
    auth_words = set(_normalize_author_for_cmp(author_meta).split())
    if not seg_words or not auth_words:
        return False
    overlap = len(seg_words & auth_words)
    return overlap >= len(auth_words) and overlap >= len(seg_words) - 1


_TITLE_STOP_WORDS = {
    'a', 'an', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
    'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'be', 'it', 'its',
}

_GARBAGE_SEGMENT_RE = re.compile(
    r'^(\d|[0-9a-fA-F]{8,}|.*\.(indd|tmp|qxd|pdf|epub|mobi)$)', re.IGNORECASE
)


def _looks_like_name(segment: str) -> bool:
    """Return True if a segment could plausibly be a person's name (not a number, hash, or title)."""
    if not segment or _GARBAGE_SEGMENT_RE.match(segment):
        return False
    words = segment.split()
    if len(words) > 6:
        return False
    # Reject if the majority of words are stop/title words
    content_words = [w for w in words if w.lower() not in _TITLE_STOP_WORDS]
    return len(content_words) >= max(1, len(words) // 2)


def _stem_word(w: str) -> str:
    return w[:-1] if w.endswith('s') and len(w) > 3 else w


def _segment_matches_title_partial(segment: str, title_meta: str) -> bool:
    """Return True if all of the segment's content words appear in the title (handles truncation).

    Uses a subset check: every content word in the segment must exist in the title,
    and the title must be at least as long (in content words) as the segment.
    Simple 's'-stripping handles truncated plurals (e.g. 'circuit' vs 'circuits').
    """
    seg_norm = _normalize_author_for_cmp(segment)
    title_norm = _normalize_author_for_cmp(title_meta)
    seg_words = {_stem_word(w) for w in seg_norm.split() if w not in _TITLE_STOP_WORDS}
    title_words = {_stem_word(w) for w in title_norm.split() if w not in _TITLE_STOP_WORDS}
    if not title_words or not seg_words:
        return False
    return seg_words.issubset(title_words) and len(title_words) >= len(seg_words)


# ---------------------------------------------------------------------------
# Filename construction
# ---------------------------------------------------------------------------

def _swap_segments(seg1: str, seg2: str, ext: str) -> str:
    """Build 'seg2 - seg1.ext', truncating to filesystem byte limit."""
    stem = f"{seg1} - {seg2}"
    if len(stem.encode('utf-8')) > MAX_STEM_BYTES:
        stem = stem.encode('utf-8')[:MAX_STEM_BYTES].decode('utf-8', errors='ignore').strip()
    return stem + ext


# ---------------------------------------------------------------------------
# Main rename logic
# ---------------------------------------------------------------------------

def process_directory(directory: str, apply: bool) -> None:
    if not os.path.isdir(directory):
        print(f"❌ Directory not found: {directory}")
        sys.exit(1)

    files = sorted(
        f for f in os.listdir(directory)
        if os.path.isfile(os.path.join(directory, f))
        and os.path.splitext(f)[1].lower() in SUPPORTED_EXTENSIONS
    )

    if not files:
        print(f"ℹ️  No supported ebook files found in: {directory}")
        return

    print(f"🔍 Scanning {len(files)} file(s) in: {directory}")
    if not apply:
        print("ℹ️  DRY RUN — pass --apply to actually rename files.\n")
    else:
        print()

    renamed = 0
    skipped_ok = 0
    skipped_no_meta = 0
    skipped_uncertain = 0
    errors = 0

    for filename in files:
        filepath = os.path.join(directory, filename)
        stem, ext = os.path.splitext(filename)

        title_meta, author_meta = get_metadata(filepath)

        if not author_meta:
            print(f"  ⚠️  SKIP (no metadata): {filename}")
            skipped_no_meta += 1
            continue

        # Split on first ' - ' only
        parts = stem.split(' - ', 1)
        if len(parts) != 2:
            print(f"  ⚠️  SKIP (can't parse as 'X - Y'): {filename}")
            skipped_no_meta += 1
            continue

        seg1, seg2 = parts[0].strip(), parts[1].strip()
        seg1_is_author = _segment_matches_author(seg1, author_meta)
        seg2_is_author = _segment_matches_author(seg2, author_meta)

        if seg1_is_author and not seg2_is_author:
            # Already 'Author - Title' — correct
            skipped_ok += 1
            continue
        elif seg2_is_author and not seg1_is_author:
            # 'Title - Author' — needs swap
            correct_filename = _swap_segments(seg2, seg1, ext)
        else:
            # Author matching ambiguous — fallback: use title to identify which segment is the title.
            # Many PDFs have the book title in the author field; use title_meta (or author_meta as
            # last resort) to find the title segment, then infer the author segment.
            title_candidates = [t for t in [title_meta, author_meta] if t]
            resolved = False
            for title_text in title_candidates:
                seg1_is_title = _segment_matches_title_partial(seg1, title_text)
                seg2_is_title = _segment_matches_title_partial(seg2, title_text)
                if seg2_is_title and not seg1_is_title and _looks_like_name(seg1):
                    # seg2 is the title, seg1 looks like a name → already Author-Title
                    skipped_ok += 1
                    resolved = True
                    break
                elif seg1_is_title and not seg2_is_title and _looks_like_name(seg2):
                    # seg1 is the title, seg2 looks like a name → needs swap
                    correct_filename = _swap_segments(seg2, seg1, ext)
                    resolved = True
                    break
            if not resolved:
                print(f"  ❓ UNCERTAIN: {filename}")
                print(f"     metadata author: {author_meta}")
                skipped_uncertain += 1
                continue

        if filename == correct_filename:
            skipped_ok += 1
            continue

        new_filepath = os.path.join(directory, correct_filename)

        print(f"  📝 FROM: {filename}")
        print(f"     TO:   {correct_filename}")

        if apply:
            if os.path.exists(new_filepath) and new_filepath != filepath:
                print(f"     ⚠️  SKIPPED — target already exists.")
                errors += 1
            else:
                try:
                    os.rename(filepath, new_filepath)
                    print(f"     ✅ Renamed.")
                    renamed += 1
                except OSError as e:
                    print(f"     ❌ Error: {e}")
                    errors += 1
        else:
            renamed += 1

    print()
    if apply:
        print(f"✅ Done. Renamed: {renamed} | Already correct: {skipped_ok} | "
              f"Uncertain: {skipped_uncertain} | No metadata: {skipped_no_meta} | Errors: {errors}")
    else:
        print(f"📋 Dry run. Would rename: {renamed} | Already correct: {skipped_ok} | "
              f"Uncertain: {skipped_uncertain} | No metadata: {skipped_no_meta}")
        if renamed > 0:
            print("   Run with --apply to perform the renames.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Rename ebooks from 'Title - Author' to 'Author - Title'.")
    parser.add_argument('directory', nargs='?', help='Directory to scan (default: output_dir from config.json)')
    parser.add_argument('--apply', action='store_true', help='Actually rename files (default is dry-run)')
    args = parser.parse_args()

    directory = args.directory
    if not directory:
        config_path = os.path.join(os.path.dirname(__file__), 'config.json')
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            directory = config.get('output_dir')
            if not directory:
                print("❌ 'output_dir' not found in config.json. Pass a directory argument.")
                sys.exit(1)
            print(f"📂 Using output_dir from config.json: {directory}")
        except FileNotFoundError:
            print(f"❌ config.json not found at {config_path}. Pass a directory argument.")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"❌ Error parsing config.json: {e}")
            sys.exit(1)

    process_directory(directory, apply=args.apply)


if __name__ == '__main__':
    main()
