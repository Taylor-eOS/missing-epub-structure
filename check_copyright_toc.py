import sys
import zipfile
from pathlib import Path, PurePosixPath
from lxml import etree
from urllib.parse import unquote
import last_folder_helper
from complex_scan import find_opf_path
from check_copyright import resolve_href, get_spine_xhtml_paths, extract_text_from_xhtml, score_file, CONFIDENCE_THRESHOLD

def normalize_path(base_path, href, namelist):
    decoded = unquote(href)
    if decoded in namelist:
        return decoded
    if not base_path:
        return PurePosixPath(decoded).as_posix()
    resolved = (PurePosixPath(base_path).parent / PurePosixPath(decoded)).as_posix()
    parts = PurePosixPath(resolved).parts
    normalized = []
    for part in parts:
        if part == '..':
            if normalized:
                normalized.pop()
        elif part != '.':
            normalized.append(part)
    return '/'.join(normalized) if normalized else ''

def strip_fragment(href):
    return href.split('#', 1)[0]

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
                props = item.get('properties') or ''
                if iid and href:
                    manifest[iid] = {'href': href, 'media-type': media, 'properties': props}
        spine = []
        spine_toc = None
        spine_el = root.find('opf:spine', ns)
        if spine_el is not None:
            spine_toc = spine_el.get('toc')
            for itemref in spine_el.findall('opf:itemref', ns):
                idref = itemref.get('idref')
                if idref:
                    spine.append(idref)
        opf_dir = PurePosixPath(opf_path).parent.as_posix()
        return manifest, spine, opf_dir, spine_toc

def find_copyright_path(z, manifest, spine, opf_dir):
    xhtml_paths = get_spine_xhtml_paths(z, manifest, spine, opf_dir)
    if not xhtml_paths:
        return None
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
        return None
    if best_score > 0 and second_score > 0 and best_score < second_score * 1.5:
        return None
    return xhtml_paths[best_index]

def extract_ncx_hrefs(z, opf_dir, manifest, spine_toc):
    ncx_id = None
    if spine_toc and spine_toc in manifest:
        ncx_id = spine_toc
    else:
        for iid, item in manifest.items():
            if (item.get('media-type') or '') == 'application/x-dtbncx+xml':
                ncx_id = iid
                break
    if not ncx_id:
        return None, 'NO NCX FOUND'
    ncx_href = resolve_href(opf_dir, manifest[ncx_id]['href'])
    if ncx_href not in z.namelist():
        return None, f'NCX file missing from zip: {ncx_href}'
    try:
        with z.open(ncx_href) as f:
            parser = etree.XMLParser(recover=True)
            tree = etree.parse(f, parser)
            root = tree.getroot()
            ncx_ns = (root.nsmap or {}).get(None, '')
            if ncx_ns:
                content_elems = tree.findall(f'.//{{{ncx_ns}}}content')
            else:
                content_elems = tree.findall('.//content')
            return [(c.get('src'), ncx_href) for c in content_elems if c.get('src')], None
    except Exception as e:
        return None, f'NCX parse error: {e}'

def extract_human_toc_hrefs(z, manifest, spine, opf_dir):
    results = []
    for idref in spine:
        item = manifest.get(idref)
        if not item:
            continue
        href = resolve_href(opf_dir, item['href'])
        filename = PurePosixPath(href).name.lower()
        if 'toc' not in filename and 'contents' not in filename:
            continue
        if href not in z.namelist():
            continue
        try:
            with z.open(href) as f:
                parser = etree.HTMLParser(recover=True)
                tree = etree.parse(f, parser)
                anchors = tree.findall('.//{http://www.w3.org/1999/xhtml}a') or tree.findall('.//a')
                for a in anchors:
                    link = a.get('href')
                    if link:
                        results.append((link, href))
        except Exception:
            continue
    return results

def hrefs_contain_path(hrefs, copyright_path, namelist):
    target_parts = PurePosixPath(copyright_path).parts
    for href, source_path in hrefs:
        base = strip_fragment(href)
        normalized = normalize_path(source_path, base, namelist)
        if PurePosixPath(normalized).parts == target_parts:
            return True
    return False

def analyze_epub(epub_path):
    warnings = []
    try:
        with zipfile.ZipFile(epub_path, 'r') as z:
            opf_path = find_opf_path(z)
            if opf_path is None:
                return None, ['OPF not found']
            manifest, spine, opf_dir, spine_toc = parse_opf(z, opf_path)
            ncx_hrefs, ncx_error = extract_ncx_hrefs(z, opf_dir, manifest, spine_toc)
            if ncx_error:
                warnings.append(ncx_error)
            copyright_path = find_copyright_path(z, manifest, spine, opf_dir)
            if copyright_path is None:
                return None, warnings
            namelist = set(z.namelist())
            hits = []
            if ncx_hrefs and hrefs_contain_path(ncx_hrefs, copyright_path, namelist):
                hits.append('in ncx')
            human_hrefs = extract_human_toc_hrefs(z, manifest, spine, opf_dir)
            if hrefs_contain_path(human_hrefs, copyright_path, namelist):
                hits.append('in human toc page')
            return (hits if hits else None), warnings
    except Exception as e:
        return None, [f'error: {e}']

def main(folder):
    p = Path(folder).expanduser().resolve()
    if not p.is_dir():
        print(f"Folder not found: {p}")
        sys.exit(1)
    epub_paths = sorted(p.rglob('*.epub'))
    if not epub_paths:
        print("No EPUB files found")
        return
    found = 0
    for epub_path in epub_paths:
        result, warnings = analyze_epub(str(epub_path))
        name = epub_path.name.replace('.epub', '')
        for w in warnings:
            print(f"{name[:30]}: {w}")
        if result:
            found += 1
            print(f"{name[:30]}: {', '.join(result)}")
    if found == 0:
        print("No copyright pages found in any TOC")

if __name__ == "__main__":
    default = last_folder_helper.get_last_folder()
    user_input = input(f'Input folder ({default}): ').strip()
    folder = user_input or default
    if not folder:
        folder = '.'
    last_folder_helper.save_last_folder(folder)
    main(folder)
