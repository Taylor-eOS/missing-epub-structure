import os
import sys
from zipfile import ZipFile
from pathlib import Path, PurePosixPath
from lxml import etree
import last_folder_helper

def find_opf_path(z):
    try:
        with z.open('META-INF/container.xml') as f:
            tree = etree.parse(f)
            rootfile = tree.find('.//{urn:oasis:names:tc:opendocument:xmlns:container}rootfile')
            if rootfile is not None:
                return rootfile.get('full-path')
    except Exception:
        pass
    for name in z.namelist():
        if name.lower().endswith('.opf'):
            return name
    return None

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
    if not opf_dir:
        return PurePosixPath(href).as_posix()
    return (PurePosixPath(opf_dir) / PurePosixPath(href)).as_posix()

def extract_nav_targets(z, opf_dir, manifest):
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
                                anchors = nav.findall('.//{http://www.w3.org/1999/xhtml}a') or nav.findall('.//a')
                                hrefs = [a.get('href') for a in anchors if a.get('href')]
                                return hrefs
                        anchors = root.findall('.//{http://www.w3.org/1999/xhtml}a') or root.findall('.//a')
                        return [a.get('href') for a in anchors if a.get('href')]
                except Exception:
                    continue
    return []

def extract_ncx_targets(z, opf_dir, manifest, spine_toc):
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
            content_elems = tree.findall('.//content')
            srcs = [c.get('src') for c in content_elems if c.get('src')]
            return srcs
    except Exception:
        return []

def strip_fragment(href):
    return href.split('#', 1)[0]

def analyze_dom_repetition(z, candidate_path):
    try:
        with z.open(candidate_path) as f:
            parser = etree.HTMLParser(recover=True)
            tree = etree.parse(f, parser)
            body = tree.find('.//{http://www.w3.org/1999/xhtml}body') or tree.find('.//body')
            if body is None:
                return False, 0, 0
            blocks = []
            for child in body:
                tag = etree.QName(child.tag).localname if isinstance(child.tag, str) else str(child.tag)
                cls = (child.get('class') or '').strip()
                blocks.append(f"{tag}:{cls}")
            total = len(blocks)
            if total < 30:
                return False, total, 0
            unique = len(set(blocks))
            ratio = unique / total if total else 0
            return ratio < 0.3, total, unique
    except Exception:
        return False, 0, 0

def analyze_epub(path):
    reasons = []
    diagnostics = []
    try:
        with ZipFile(path, 'r') as z:
            opf_path = find_opf_path(z)
            if not opf_path:
                return ['no_opf']
            manifest, spine, opf_dir, spine_toc = parse_opf(z, opf_path)
            nav_hrefs = extract_nav_targets(z, opf_dir, manifest)
            ncx_hrefs = extract_ncx_targets(z, opf_dir, manifest, spine_toc)
            has_machine_toc = bool(nav_hrefs or ncx_hrefs)
            spine_files = []
            for idref in spine:
                item = manifest.get(idref)
                if not item:
                    continue
                href = resolve_href(opf_dir, item['href'])
                lower_href = href.lower()
                if lower_href.endswith(('.xhtml', '.html', '.htm')):
                    filename = PurePosixPath(href).name.lower()
                    if 'cover' in filename or 'title' in filename or 'copyright' in filename or 'toc' in filename:
                        continue
                    spine_files.append(href)
            if not spine_files:
                return ['no_spine_xhtml_files']
            sizes = {}
            total_size = 0
            for href in spine_files:
                try:
                    info = z.getinfo(href)
                    sizes[href] = info.file_size
                    total_size += info.file_size
                except KeyError:
                    sizes[href] = 0
            largest_file, largest_size = max(sizes.items(), key=lambda x: x[1])
            flat_spine = (len(spine_files) <= 2 or largest_size > 300 * 1024 or (total_size and largest_size / total_size > 0.7))
            toc_targets = nav_hrefs if nav_hrefs else ncx_hrefs
            distinct_target_files = set()
            target_count_per_file = {}
            for t in toc_targets:
                base = strip_fragment(resolve_href(opf_dir, t))
                matched = False
                for s in spine_files:
                    if PurePosixPath(s).name == PurePosixPath(base).name:
                        distinct_target_files.add(s)
                        target_count_per_file[s] = target_count_per_file.get(s, 0) + 1
                        matched = True
                        break
                if not matched:
                    continue
            covered_files = len(distinct_target_files)
            total_spine_xhtml = len(spine_files)
            if total_spine_xhtml == 0 or not toc_targets:
                toc_collapses = False
            else:
                coverage_ratio = covered_files / total_spine_xhtml
                has_low_coverage = coverage_ratio < 0.20
                has_very_few_targets = covered_files <= 3
                has_one_heavily_nested = False
                if covered_files > 0:
                    max_targets_in_one_file = max(target_count_per_file.values())
                    if max_targets_in_one_file >= 8 and max_targets_in_one_file >= 0.40 * len(toc_targets):
                        has_one_heavily_nested = True
                toc_collapses = (has_low_coverage or has_very_few_targets) and not has_one_heavily_nested
            mid = spine_files[len(spine_files) // 2]
            dom = analyze_dom_structure(z, mid)
            if not has_machine_toc and flat_spine and not dom['has_headings']:
                reasons.append('no_toc_and_no_segmentation_signal')
            if toc_collapses and flat_spine:
                reasons.append('toc_collapses_to_single_file')
            if reasons:
                return reasons + diagnostics
            return []
    except Exception:
        return ['error_parsing_epub']

def analyze_dom_structure(z, candidate_path):
    try:
        with z.open(candidate_path) as f:
            parser = etree.HTMLParser(recover=True)
            tree = etree.parse(f, parser)
            body = tree.find('.//{http://www.w3.org/1999/xhtml}body') or tree.find('.//body')
            if body is None:
                return {'has_headings': False}
            for child in body:
                if not isinstance(child.tag, str):
                    continue
                tag = etree.QName(child.tag).localname.lower()
                if tag in ('h1', 'h2', 'h3'):
                    return {'has_headings': True}
            return {'has_headings': False}
    except Exception:
        return {'has_headings': False}

def main(folder):
    p = Path(folder).expanduser().resolve()
    if not p.is_dir():
        print(f"Folder not found: {p}")
        sys.exit(1)
    epub_paths = sorted(p.rglob('*.epub'))
    if not epub_paths:
        print("No EPUB files found")
        return
    for epub in epub_paths:
        reasons = analyze_epub(str(epub))
        if reasons and reasons != ['ok']:
            print(f"{epub.name.strip('.epub')}: {', '.join(reasons)}")

if __name__ == "__main__":
    default = last_folder_helper.get_last_folder()
    user_input = input(f'Input folder ({default}): ').strip()
    folder = user_input or default
    if not folder:
        folder = '.'
    last_folder_helper.save_last_folder(folder)
    main(folder)

