#!/usr/bin/env python3
"""
fix_book.py — Rename an ebook file and fix its embedded metadata.

Usage:
    python3 fix_book.py <filepath> <author> <title> [--dry-run]

Renames the file to "Author - Title.ext" and updates the embedded
metadata (epub OPF title/creator, or PDF title/author fields).
"""

import os
import re
import sys
import shutil
import zipfile
import xml.etree.ElementTree as ET

try:
    import fitz
    HAS_FITZ = True
except ImportError:
    HAS_FITZ = False

MAX_STEM_BYTES = 180

INVALID_CHARS_RE = re.compile(r'[\\/:*?"<>|]')


def _safe_filename(s: str) -> str:
    """Strip characters that are invalid in filenames."""
    return INVALID_CHARS_RE.sub('', s).strip()


def _build_filename(author: str, title: str, ext: str) -> str:
    author_safe = _safe_filename(author)
    title_safe = _safe_filename(title)
    stem = f"{author_safe} - {title_safe}"
    # Truncate to filesystem byte limit
    encoded = stem.encode('utf-8')
    if len(encoded) > MAX_STEM_BYTES:
        stem = encoded[:MAX_STEM_BYTES].decode('utf-8', errors='ignore').strip()
    return stem + ext


def _fix_epub_metadata(filepath: str, author: str, title: str) -> bool:
    """Update title and creator fields in the epub OPF. Returns True on success."""
    try:
        tmp_path = filepath + '.tmp_fix'
        with zipfile.ZipFile(filepath, 'r') as zin:
            names = zin.namelist()

            # Find OPF path
            opf_path = None
            if 'META-INF/container.xml' in names:
                root = ET.fromstring(zin.read('META-INF/container.xml').decode('utf-8', 'replace'))
                for e in root.iter():
                    if e.tag.endswith('rootfile'):
                        opf_path = e.get('full-path')
                        break
            if not opf_path:
                opf_path = next((n for n in names if n.endswith('.opf')), None)
            if not opf_path:
                return False

            opf_bytes = zin.read(opf_path).decode('utf-8', 'replace')

            # Parse and update OPF
            # Register namespaces to avoid ns0: prefix mangling
            ET.register_namespace('', 'http://www.idpf.org/2007/opf')
            ET.register_namespace('dc', 'http://purl.org/dc/elements/1.1/')
            ET.register_namespace('opf', 'http://www.idpf.org/2007/opf')

            root = ET.fromstring(opf_bytes)
            ns_dc = 'http://purl.org/dc/elements/1.1/'

            t_el = root.find(f'.//{{{ns_dc}}}title')
            a_el = root.find(f'.//{{{ns_dc}}}creator')

            if t_el is not None:
                t_el.text = title
            if a_el is not None:
                a_el.text = author

            new_opf = ET.tostring(root, encoding='unicode', xml_declaration=False)
            # Restore xml declaration if original had one
            if opf_bytes.strip().startswith('<?xml'):
                new_opf = '<?xml version=\'1.0\' encoding=\'utf-8\'?>\n' + new_opf

            with zipfile.ZipFile(filepath, 'r') as zin, \
                 zipfile.ZipFile(tmp_path, 'w', zipfile.ZIP_DEFLATED) as zout:
                for item in zin.infolist():
                    if item.filename == opf_path:
                        zout.writestr(item, new_opf.encode('utf-8'))
                    else:
                        zout.writestr(item, zin.read(item.filename))

        os.replace(tmp_path, filepath)
        return True
    except Exception as e:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        print(f"     ⚠️  epub metadata update failed: {e}")
        return False


def _fix_pdf_metadata(filepath: str, author: str, title: str) -> bool:
    """Update title and author fields in a PDF. Returns True on success."""
    if not HAS_FITZ:
        return False
    try:
        doc = fitz.open(filepath)
        doc.set_metadata({'title': title, 'author': author})
        doc.saveIncr()
        doc.close()
        return True
    except Exception as e:
        try:
            doc.close()
        except Exception:
            pass
        print(f"     ⚠️  PDF metadata update failed: {e}")
        return False


def fix_book(filepath: str, author: str, title: str, dry_run: bool = False) -> None:
    if not os.path.isfile(filepath):
        print(f"  ❌ File not found: {filepath}")
        return

    directory = os.path.dirname(filepath) or '.'
    old_filename = os.path.basename(filepath)
    ext = os.path.splitext(old_filename)[1]

    new_filename = _build_filename(author, title, ext)
    new_filepath = os.path.join(directory, new_filename)

    print(f"  📝 FROM: {old_filename}")
    print(f"     TO:   {new_filename}")
    print(f"     AUTH: {author}")
    print(f"     TITL: {title}")

    if dry_run:
        print(f"     (dry run — no changes made)")
        return

    # Rename file first
    if old_filename != new_filename:
        if os.path.exists(new_filepath) and new_filepath != filepath:
            print(f"     ⚠️  SKIPPED rename — target already exists.")
            return
        try:
            os.rename(filepath, new_filepath)
            print(f"     ✅ Renamed.")
        except OSError as e:
            print(f"     ❌ Rename failed: {e}")
            return
    else:
        print(f"     ℹ️  Filename already correct.")

    # Fix metadata
    ext_lower = ext.lower()
    if ext_lower == '.epub':
        ok = _fix_epub_metadata(new_filepath, author, title)
        if ok:
            print(f"     ✅ epub metadata updated.")
    elif ext_lower == '.pdf':
        ok = _fix_pdf_metadata(new_filepath, author, title)
        if ok:
            print(f"     ✅ PDF metadata updated.")
    else:
        print(f"     ℹ️  Metadata update not supported for {ext_lower}")


def main():
    args = sys.argv[1:]
    dry_run = '--dry-run' in args
    args = [a for a in args if not a.startswith('--')]

    if len(args) < 3:
        print("Usage: python3 fix_book.py [--dry-run] <filepath> <author> <title>")
        sys.exit(1)

    filepath, author, title = args[0], args[1], args[2]
    fix_book(filepath, author, title, dry_run=dry_run)


if __name__ == '__main__':
    main()
