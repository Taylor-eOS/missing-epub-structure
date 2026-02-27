import sys
import zipfile
from pathlib import Path, PurePosixPath
from lxml import etree
from urllib.parse import unquote
import last_folder_helper
from complex_scan import find_opf_path

print_all = False

def parse_opf(z, opf_path):
    with z.open(opf_path) as f:
        parser = etree.XMLParser(recover=True)
        tree = etree.parse(f, parser)
        root = tree.getroot()
        opf_ns = None
        for ns in (root.nsmap or {}).values():
            if ns and 'opf' in ns:
                opf_ns = ns
                break
        if opf_ns is None:
            opf_ns = 'http://www.idpf.org/2007/opf'
        ns = {'opf': opf_ns}
        manifest = {}
        manifest_el = root.find('opf:manifest', ns)
        if manifest_el is not None:
            for item in manifest_el.findall('opf:item', ns):
                iid = item.get('id')
                href = item.get('href')
                media = item.get('media-type')
                if iid and href:
                    manifest[iid] = {'href': href, 'media-type': media}
        spine = []
        spine_el = root.find('opf:spine', ns)
        if spine_el is not None:
            for itemref in spine_el.findall('opf:itemref', ns):
                idref = itemref.get('idref')
                if idref:
                    spine.append(idref)
        opf_dir = PurePosixPath(opf_path).parent.as_posix()
        return manifest, spine, opf_dir

def resolve_href(opf_dir, href):
    decoded = unquote(href)
    if not opf_dir:
        return PurePosixPath(decoded).as_posix()
    return (PurePosixPath(opf_dir) / PurePosixPath(decoded)).as_posix()

def get_spine_xhtml_paths(z, manifest, spine, opf_dir):
    paths = []
    for idref in spine:
        item = manifest.get(idref)
        if not item:
            continue
        mt = item.get('media-type') or ''
        if mt not in ('application/xhtml+xml', 'text/html'):
            continue
        href = resolve_href(opf_dir, item['href'])
        if href in z.namelist():
            paths.append(href)
    return paths

def extract_text_from_xhtml(z, zip_path):
    try:
        with z.open(zip_path) as f:
            parser = etree.HTMLParser(recover=True)
            tree = etree.parse(f, parser)
        body = tree.find('.//{http://www.w3.org/1999/xhtml}body') or tree.find('.//body')
        if body is None:
            return ''
        return ' '.join(body.itertext())
    except Exception:
        return ''

COPYRIGHT_TEXT_SIGNALS = [
    'all rights reserved',
    'published by',
    'library of congress',
    'isbn',
    'printed in',
    'first published',
    'first edition',
    'cataloging-in-publication',
    'cataloguing in publication',
    'no part of this',
    'reproduction prohibited',
    'without written permission',
    'imprint of',
    'division of',
    'trade paperback',
    'hardcover',
    'originally published',
]

COPYRIGHT_FILENAME_SIGNALS = [
    'copyright',
    'copyrights',
    'legal',
    'rights',
    'colophon',
]

def score_file(zip_path, text):
    score = 0
    filename = PurePosixPath(zip_path).name.lower()
    stem = PurePosixPath(filename).stem
    for kw in COPYRIGHT_FILENAME_SIGNALS:
        if kw in stem:
            score += 5
            break
    text_lower = text.lower()
    if '©' in text:
        score += 4
    if 'copyright' in text_lower:
        score += 3
    matched_signals = sum(1 for sig in COPYRIGHT_TEXT_SIGNALS if sig in text_lower)
    score += matched_signals * 2
    word_count = len(text.split())
    if word_count > 0 and matched_signals > 0:
        density = matched_signals / max(word_count / 50, 1)
        if density >= 1.5:
            score += 3
    return score

CONFIDENCE_THRESHOLD = 8

def find_copyright_page(epub_path):
    try:
        with zipfile.ZipFile(epub_path, 'r') as z:
            opf_path = find_opf_path(z)
            if opf_path is None:
                return None, ('no_opf', None)
            manifest, spine, opf_dir = parse_opf(z, opf_path)
            xhtml_paths = get_spine_xhtml_paths(z, manifest, spine, opf_dir)
            if not xhtml_paths:
                return None, ('no_xhtml', None)
            best_index = None
            best_score = 0
            second_score = 0
            for i, zip_path in enumerate(xhtml_paths):
                text = extract_text_from_xhtml(z, zip_path)
                score = score_file(zip_path, text)
                if score > best_score:
                    second_score = best_score
                    best_score = score
                    best_index = i
                elif score > second_score:
                    second_score = score
            if best_score < CONFIDENCE_THRESHOLD:
                return None, ('not_found', None)
            if best_score > 0 and second_score > 0 and best_score < second_score * 1.5:
                return None, ('ambiguous', None)
            return best_index + 1, (xhtml_paths[best_index], len(xhtml_paths))
    except Exception as e:
        return None, (f'error: {e}', None)

def main(folder):
    p = Path(folder).expanduser().resolve()
    if not p.is_dir():
        print(f"Folder not found: {p}")
        sys.exit(1)
    epub_paths = sorted(p.rglob('*.epub'))
    if not epub_paths:
        print("No EPUB files found")
        return
    for epub_path in epub_paths:
        page_num, detail = find_copyright_page(str(epub_path))
        name = epub_path.name.replace('.epub', '')
        if page_num is not None:
            total = detail[1]
            if print_all or page_num > 4:
                print(f"{name}: {page_num} of {total}")
        else:
            print(f"{name}: {detail[0]}")

if __name__ == "__main__":
    default = last_folder_helper.get_last_folder()
    user_input = input(f'Input folder ({default}): ').strip()
    folder = user_input or default
    if not folder:
        folder = '.'
    last_folder_helper.save_last_folder(folder)
    main(folder)

