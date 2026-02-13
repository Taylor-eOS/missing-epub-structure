import os
import sys
from zipfile import ZipFile
from pathlib import Path, PurePosixPath
from lxml import etree
from urllib.parse import unquote
import last_folder_helper
from complex_scan import find_opf_path

def parse_opf(z, opf_path):
    with z.open(opf_path) as f:
        parser = etree.XMLParser(recover=True)
        tree = etree.parse(f, parser)
        root = tree.getroot()
        nsmap = {k if k is not None else '': v for k, v in root.nsmap.items()}
        opf_ns = None
        for ns in root.nsmap.values():
            if ns and 'opf' in ns:
                opf_ns = ns
                break
        if opf_ns is None:
            opf_ns = 'http://www.idpf.org/2007/opf'
        ns = {'opf': opf_ns}
        manifest_el = root.find('opf:manifest', ns)
        manifest = {}
        if manifest_el is not None:
            for item in manifest_el.findall('opf:item', ns):
                iid = item.get('id')
                href = item.get('href')
                media = item.get('media-type')
                props = item.get('properties') or ''
                if iid and href:
                    manifest[iid] = {'href': href, 'media-type': media, 'properties': props}
        spine_el = root.find('opf:spine', ns)
        spine = []
        spine_toc = None
        if spine_el is not None:
            spine_toc = spine_el.get('toc')
            for itemref in spine_el.findall('opf:itemref', ns):
                idref = itemref.get('idref')
                if idref:
                    spine.append(idref)
        opf_dir = PurePosixPath(opf_path).parent.as_posix()
        return manifest, spine, opf_dir, spine_toc

def resolve_href(opf_dir, href):
    decoded_href = unquote(href)
    if not opf_dir:
        return PurePosixPath(decoded_href).as_posix()
    return (PurePosixPath(opf_dir) / PurePosixPath(decoded_href)).as_posix()

def normalize_path(base_path, href):
    decoded_href = unquote(href)
    if not base_path:
        return PurePosixPath(decoded_href).as_posix()
    resolved = (PurePosixPath(base_path).parent / PurePosixPath(decoded_href)).as_posix()
    parts = PurePosixPath(resolved).parts
    normalized_parts = []
    for part in parts:
        if part == '..':
            if normalized_parts:
                normalized_parts.pop()
        elif part != '.':
            normalized_parts.append(part)
    return '/'.join(normalized_parts) if normalized_parts else ''

def strip_fragment(href):
    return href.split('#', 1)[0]

def extract_nav_entries(z, opf_dir, manifest):
    entries = []
    for item in manifest.values():
        props = (item.get('properties') or '')
        if 'nav' in props.split():
            nav_path = resolve_href(opf_dir, item['href'])
            if nav_path in z.namelist():
                try:
                    with z.open(nav_path) as f:
                        parser = etree.HTMLParser(recover=True)
                        tree = etree.parse(f, parser)
                        root = tree.getroot()
                        navs = root.findall('.//{http://www.w3.org/1999/xhtml}nav') or root.findall('.//nav')
                        for nav in navs:
                            epub_type = nav.get('{http://www.idpf.org/2007/ops}type') or nav.get('epub:type') or ''
                            if 'toc' in epub_type or 'toc' in (nav.get('id') or '').lower():
                                list_items = nav.findall('.//{http://www.w3.org/1999/xhtml}li') or nav.findall('.//li')
                                for li in list_items:
                                    anchors = li.findall('.//{http://www.w3.org/1999/xhtml}a') or li.findall('.//a')
                                    if anchors:
                                        a = anchors[0]
                                        href = a.get('href')
                                        text = ''.join(a.itertext()).strip()
                                        if href:
                                            entries.append({'href': href, 'text': text, 'source': nav_path})
                                return entries
                        list_items = root.findall('.//{http://www.w3.org/1999/xhtml}li') or root.findall('.//li')
                        for li in list_items:
                            anchors = li.findall('.//{http://www.w3.org/1999/xhtml}a') or li.findall('.//a')
                            if anchors:
                                a = anchors[0]
                                href = a.get('href')
                                text = ''.join(a.itertext()).strip()
                                if href:
                                    entries.append({'href': href, 'text': text, 'source': nav_path})
                        return entries
                except Exception:
                    continue
    return entries

def extract_ncx_entries(z, opf_dir, manifest, spine_toc):
    ncx_id = None
    if spine_toc and spine_toc in manifest:
        ncx_id = spine_toc
    else:
        for iid, item in manifest.items():
            if (item.get('media-type') or '') == 'application/x-dtbncx+xml':
                ncx_id = iid
                break
    if not ncx_id:
        return []
    ncx_href = resolve_href(opf_dir, manifest[ncx_id]['href'])
    if ncx_href not in z.namelist():
        return []
    try:
        with z.open(ncx_href) as f:
            parser = etree.XMLParser(recover=True)
            tree = etree.parse(f, parser)
            root = tree.getroot()
            ns = None
            if root.nsmap and None in root.nsmap:
                ns = root.nsmap[None]
            if ns:
                navpoints = tree.findall(f'.//{{{ns}}}navPoint')
            else:
                navpoints = tree.findall('.//navPoint')
            entries = []
            for np in navpoints:
                if ns:
                    text_elem = np.find(f'.//{{{ns}}}text')
                    content_elem = np.find(f'.//{{{ns}}}content')
                else:
                    text_elem = np.find('.//text')
                    content_elem = np.find('.//content')
                text = text_elem.text.strip() if text_elem is not None and text_elem.text else ''
                href = content_elem.get('src') if content_elem is not None else None
                if href:
                    entries.append({'href': href, 'text': text, 'source': ncx_href})
            return entries
    except Exception:
        return []

def get_content_files(z, manifest, spine, opf_dir):
    files = []
    for idref in spine:
        item = manifest.get(idref)
        if not item:
            continue
        href = resolve_href(opf_dir, item['href'])
        lower_href = href.lower()
        if lower_href.endswith(('.xhtml', '.html', '.htm', '.xml')):
            filename = PurePosixPath(href).name.lower()
            if 'cover' in filename or 'title' in filename or 'copyright' in filename or 'toc' in filename:
                continue
            files.append(href)
    return files

def count_headings_in_file(z, filepath):
    try:
        with z.open(filepath) as f:
            parser = etree.HTMLParser(recover=True)
            tree = etree.parse(f, parser)
            body = tree.find('.//{http://www.w3.org/1999/xhtml}body') or tree.find('.//body')
            if body is None:
                return 0
            headings = []
            for tag in ('h1', 'h2', 'h3', 'h4', 'h5', 'h6'):
                found = body.findall(f'.//{{{http://www.w3.org/1999/xhtml}}}{tag}') or body.findall(f'.//{tag}')
                headings.extend(found)
            return len(headings)
    except Exception:
        return 0

def get_text_length(z, filepath):
    try:
        with z.open(filepath) as f:
            parser = etree.HTMLParser(recover=True)
            tree = etree.parse(f, parser)
            body = tree.find('.//{http://www.w3.org/1999/xhtml}body') or tree.find('.//body')
            if body is None:
                return 0
            text = ''.join(body.itertext())
            return len(text.strip())
    except Exception:
        return 0

def analyze_toc_structure(toc_entries, content_files, z):
    if not toc_entries:
        return {
            'has_toc': False,
            'content_entries': 0,
            'unique_targets': 0,
            'single_file_target': None
        }
    boilerplate_keywords = {
        'cover', 'title', 'title page', 'copyright', 'table of contents', 
        'toc', 'contents', 'frontmatter', 'front matter', 'titlepage',
        'dedication', 'epigraph', 'about the author', 'also by',
        'books by', 'acknowledgments', 'acknowledgements'
    }
    content_entries = []
    for entry in toc_entries:
        text = entry['text'].lower().strip()
        base_href = strip_fragment(entry['href'])
        normalized = normalize_path(entry['source'], base_href)
        filename = PurePosixPath(normalized).name.lower()
        is_boilerplate_text = text in boilerplate_keywords or any(keyword in text for keyword in ['cover', 'title page', 'copyright'])
        is_boilerplate_file = any(keyword in filename for keyword in ['cover', 'title', 'copyright', 'toc'])
        is_in_content = normalized in content_files
        if not is_boilerplate_text and not is_boilerplate_file and is_in_content:
            content_entries.append(entry)
    unique_targets = set()
    for entry in content_entries:
        base = strip_fragment(entry['href'])
        normalized = normalize_path(entry['source'], base)
        if normalized in content_files:
            unique_targets.add(normalized)
    single_file = None
    if len(unique_targets) == 1:
        single_file = list(unique_targets)[0]
    return {
        'has_toc': True,
        'content_entries': len(content_entries),
        'unique_targets': len(unique_targets),
        'single_file_target': single_file
    }

def analyze_epub_single_chapter(path, debug=False):
    reasons = []
    try:
        with ZipFile(path, 'r') as z:
            opf_path = find_opf_path(z)
            if not opf_path:
                return ['no_opf']
            manifest, spine, opf_dir, spine_toc = parse_opf(z, opf_path)
            content_files = get_content_files(z, manifest, spine, opf_dir)
            if not content_files:
                return ['no_content_files']
            nav_entries = extract_nav_entries(z, opf_dir, manifest)
            ncx_entries = extract_ncx_entries(z, opf_dir, manifest, spine_toc)
            toc_entries = nav_entries if nav_entries else ncx_entries
            if debug:
                print(f"\nDEBUG {Path(path).name}:")
                print(f"  NAV entries: {len(nav_entries)}")
                print(f"  NCX entries: {len(ncx_entries)}")
                print(f"  Content files: {len(content_files)}")
                if toc_entries:
                    print(f"  TOC entries sample: {toc_entries[:3]}")
            toc_analysis = analyze_toc_structure(toc_entries, content_files, z)
            if debug:
                print(f"  TOC analysis: {toc_analysis}")
            num_content_files = len(content_files)
            if not toc_analysis['has_toc']:
                reasons.append('no_toc')
                return reasons
            content_entry_count = toc_analysis['content_entries']
            if content_entry_count == 0:
                reasons.append('toc_has_no_content_entries')
            elif content_entry_count == 1:
                reasons.append('single_toc_entry')
            return reasons if reasons else []
    except Exception as e:
        return ['error_parsing_epub']

def main(folder, debug=False):
    print(f'Detecting EPUBs with single-chapter issues...')
    p = Path(folder).expanduser().resolve()
    if not p.is_dir():
        print(f"Folder not found: {p}")
        sys.exit(1)
    epub_paths = sorted(p.rglob('*.epub'))
    if not epub_paths:
        print("No EPUB files found")
        return
    found_issues = 0
    for epub in epub_paths:
        reasons = analyze_epub_single_chapter(str(epub), debug=debug)
        if reasons:
            found_issues += 1
            print(f"{epub.name.replace('.epub', '')}: {', '.join(reasons)}")
    if found_issues == 0:
        print("No single-chapter issues detected")

if __name__ == "__main__":
    default = last_folder_helper.get_last_folder()
    user_input = input(f'Input folder ({default}): ').strip()
    folder = user_input or default
    if not folder:
        folder = '.'
    last_folder_helper.save_last_folder(folder)
    debug_mode = '--debug' in sys.argv
    main(folder, debug=debug_mode)

