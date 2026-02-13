import zipfile
from pathlib import Path, PurePosixPath
from lxml import etree
import last_folder_helper
from complex_scan import find_opf_path

size_threshold = 400.0
print_size = True
print_png = True

def resolve_href(opf_dir, href):
    return (PurePosixPath(opf_dir) / PurePosixPath(href)).as_posix()

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
        opf_dir = PurePosixPath(opf_path).parent.as_posix()
        return manifest, opf_dir, root, ns

def find_cover_path(z, manifest, opf_dir, root, ns):
    version = root.get('version') or '2.0'
    cover_zip_path = None
    cover_media = None
    if version.startswith('3'):
        for iid, item in manifest.items():
            props = item.get('properties', '')
            if props and 'cover-image' in props.split():
                cover_zip_path = resolve_href(opf_dir, item['href'])
                cover_media = item['media-type']
                break
    if not cover_zip_path:
        meta_cover = root.find('.//opf:meta[@name="cover"]', ns)
        if meta_cover is not None:
            cid = meta_cover.get('content')
            if cid and cid in manifest:
                cover_zip_path = resolve_href(opf_dir, manifest[cid]['href'])
                cover_media = manifest[cid]['media-type']
    if not cover_zip_path:
        guide = root.find('opf:guide', ns)
        if guide is not None:
            for ref in guide.findall('opf:reference', ns):
                if ref.get('type') == 'cover':
                    href = ref.get('href')
                    if href:
                        cover_zip_path = resolve_href(opf_dir, href.split('#')[0])
                        break
    if not cover_zip_path:
        candidates = []
        for name in z.namelist():
            p = PurePosixPath(name)
            if p.suffix.lower() in ['.jpg', '.jpeg', '.png', '.gif']:
                if p.name.lower().startswith('cover.'):
                    candidates.append(name)
        if candidates:
            candidates.sort(key=len)
            cover_zip_path = candidates[0]
    return cover_zip_path, cover_media

def main(folder):
    p = Path(folder).expanduser().resolve()
    if not p.is_dir():
        print(f"Folder not found: {p}")
        return
    epub_paths = sorted(p.rglob('*.epub'))
    if not epub_paths:
        print("No EPUB files found")
        return
    for epub_path in epub_paths:
        try:
            with zipfile.ZipFile(epub_path) as z:
                opf_path = find_opf_path(z)
                if opf_path is None:
                    continue
                manifest, opf_dir, root, ns = parse_opf(z, opf_path)
                cover_zip_path, _ = find_cover_path(z, manifest, opf_dir, root, ns)
                if cover_zip_path is None:
                    continue
                file_size_bytes = z.getinfo(cover_zip_path).file_size
                file_size_kb = file_size_bytes / 1024.0
                is_png = cover_zip_path.lower().endswith('.png')
                if file_size_kb > size_threshold:
                    if print_size:
                        print(f"{epub_path.name[:-5]} size: {file_size_kb:.0f}KB")
                    else:
                        print(f"{epub_path.name[:-5]}")
                elif is_png and print_png:
                    print(f"{epub_path.name[:-5]}: is PNG")
        except Exception:
            pass

if __name__ == "__main__":
    print(f"Current size threshold: {size_threshold:.0f}KB. Change in file.")
    default = last_folder_helper.get_last_folder()
    user_input = input(f'Input folder ({default}): ').strip()
    folder = user_input or default
    if not folder:
        folder = '.'
    last_folder_helper.save_last_folder(folder)
    print()
    main(folder)

