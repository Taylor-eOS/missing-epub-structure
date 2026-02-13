from zipfile import ZipFile
from lxml import etree
from pathlib import Path, PurePosixPath
from collections import Counter
import last_folder_helper
from complex_scan import find_opf_path

SEARCH_STRINGS = ["oceanofpdf", "steelrat", "are belong to us", "gescannt von", "lol.to", "invisibleorder.com"]
printKeyError = False
reportnooccurrences = False
print_warnings = False

def parse_opf(z, opf_path):
    with z.open(opf_path) as f:
        parser = etree.XMLParser(recover=True)
        tree = etree.parse(f, parser)
        root = tree.getroot()
        opf_ns = 'http://www.idpf.org/2007/opf'
        if root.nsmap:
            for prefix, ns in root.nsmap.items():
                if ns and ns.endswith('/opf'):
                    opf_ns = ns
                    break
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
        spine_el = root.find('opf:spine', ns)
        if spine_el is not None:
            for itemref in spine_el.findall('opf:itemref', ns):
                idref = itemref.get('idref')
                if idref:
                    spine.append(idref)
        opf_dir = PurePosixPath(opf_path).parent.as_posix()
        return manifest, spine, opf_dir

def resolve_href(opf_dir, href):
    clean_href = PurePosixPath(href)
    if '..' in clean_href.parts:
        if print_warnings: print(f"Warning: Skipping path with '..' components: {href}")
        return None
    if not opf_dir:
        return clean_href.as_posix()
    return (PurePosixPath(opf_dir) / clean_href).as_posix()

def extract_clean_text(data_bytes):
    try:
        parser = etree.HTMLParser(recover=True)
        tree = etree.fromstring(data_bytes, parser)
        xhtml_body = tree.find('.//{http://www.w3.org/1999/xhtml}body')
        html_body = tree.find('.//body')
        if xhtml_body is not None:
            body = xhtml_body
        elif html_body is not None:
            body = html_body
        else:
            body = tree
        raw_text = ''.join(body.itertext())
        clean_text = raw_text.replace('\xa0', ' ')
        clean_text = ' '.join(clean_text.split())
        return clean_text.lower()
    except Exception as e:
        if print_warnings: print(f"Warning: Error parsing content: {e}")
        try:
            return data_bytes.decode('utf-8', errors='ignore').lower()
        except:
            return ''

def analyze_epub_strings(epub_path):
    findings = Counter()
    try:
        with ZipFile(epub_path, 'r') as z:
            opf_path = find_opf_path(z)
            if not opf_path:
                if print_warnings: print(f"Warning: No OPF file found in {epub_path}")
                return findings
            manifest, spine, opf_dir = parse_opf(z, opf_path)
            content_files = []
            for item in manifest.values():
                href = resolve_href(opf_dir, item['href'])
                if href is None:
                    continue
                media_type = item.get('media-type', '').lower()
                if (media_type.startswith('text/') or
                    media_type == 'application/xhtml+xml' or
                    media_type == 'image/svg+xml' or
                    media_type == 'application/x-dtbncx+xml'):
                    content_files.append(href)
            for cf in content_files:
                try:
                    with z.open(cf) as fh:
                        data = fh.read()
                    text = extract_clean_text(data)
                    for s in SEARCH_STRINGS:
                        findings[s] += text.count(s.lower())
                except KeyError:
                    if printKeyError:
                        if print_warnings: print(f"Warning: File not found in archive: {cf}")
                    continue
                except Exception as e:
                    if print_warnings: print(f"Warning: Error reading {cf}: {e}")
                    continue
    except Exception as e:
        if print_warnings: print(f"Warning: Error processing {epub_path}: {e}")
    return findings

def main(folder):
    p = Path(folder).expanduser().resolve()
    if not p.is_dir():
        print(f"Folder not found: {p}")
        return
    epub_paths = sorted(p.rglob('*.epub'))
    if not epub_paths:
        print("No EPUB files found")
        return
    for epub in epub_paths:
        results = analyze_epub_strings(str(epub))
        found = {s: c for s, c in results.items() if c > 0}
        if found:
            print(f"{epub.stem}:")
            for s, count in sorted(found.items(), key=lambda x: -x[1]):
                print(f"  \"{s}\" appears {count} times")

if __name__ == "__main__":
    default = last_folder_helper.get_last_folder()
    user_input = input(f'Input folder ({default}): ').strip()
    folder = user_input or default
    if not folder:
        folder = '.'
    last_folder_helper.save_last_folder(folder)
    main(folder)

