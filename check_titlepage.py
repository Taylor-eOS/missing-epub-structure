import sys
import zipfile
from pathlib import Path, PurePosixPath
from lxml import etree
import last_folder_helper
from get_covers import find_cover_path
from complex_scan import find_opf_path

problems_only = False

def resolve_href(opf_dir, href):
    return (PurePosixPath(opf_dir) / PurePosixPath(href)).as_posix()

def get_image_dimensions(z, image_path):
    try:
        with z.open(image_path) as f:
            data = f.read()
            if data[:2] == b'\xff\xd8':
                return get_jpeg_dimensions(data)
            elif data[:8] == b'\x89PNG\r\n\x1a\n':
                return get_png_dimensions(data)
    except Exception:
        pass
    return None, None

def get_jpeg_dimensions(data):
    try:
        i = 2
        while i < len(data):
            if data[i] != 0xFF:
                break
            marker = data[i + 1]
            if marker == 0xD8:
                i += 2
                continue
            if marker == 0xD9:
                break
            if 0xC0 <= marker <= 0xCF and marker not in [0xC4, 0xC8, 0xCC]:
                height = (data[i + 5] << 8) | data[i + 6]
                width = (data[i + 7] << 8) | data[i + 8]
                return width, height
            length = (data[i + 2] << 8) | data[i + 3]
            i += 2 + length
    except Exception:
        pass
    return None, None

def get_png_dimensions(data):
    try:
        if len(data) >= 24:
            width = (data[16] << 24) | (data[17] << 16) | (data[18] << 8) | data[19]
            height = (data[20] << 24) | (data[21] << 16) | (data[22] << 8) | data[23]
            return width, height
    except Exception:
        pass
    return None, None

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

def find_first_content_path(z, manifest, opf_dir, root, ns):
    spine = root.find('opf:spine', ns)
    if spine is None:
        return None, None
    for itemref_el in spine.findall('opf:itemref', ns):
        if itemref_el.get('linear', 'yes') != 'no':
            idref = itemref_el.get('idref')
            if idref and idref in manifest:
                item = manifest[idref]
                mt = item['media-type']
                if mt in ('application/xhtml+xml', 'text/html'):
                    href = item['href']
                    zip_path = resolve_href(opf_dir, href)
                    return zip_path, href
    return None, None

def analyze_content(z, first_zip_path, book_title, cover_width, cover_height):
    indicators = {
        'has_svg': False,
        'has_cover_class': False,
        'has_cover_id': False,
        'has_cover_image_name': False,
        'has_title_image_name': False,
        'contains_title': False,
        'has_single_image': False,
        'has_center_align': False,
        'text_length': 0,
        'image_count': 0,
        'has_ebookmaker_cover_class': False,
        'has_minimal_text': False,
        'has_body_image': False,
        'has_meta_cover': False,
        'has_fullsize_svg': False,
        'has_page_margin_zero': False,
        'title_is_cover': False,
        'has_single_svg_image': False,
        'body_direct_svg': False,
        'has_viewbox_svg': False,
        'css_text_align_center': False,
        'has_minimal_structure': False,
        'image_aspect_ratio_portrait': False,
        'no_navigation_text': False,
        'svg_aspect_mismatch': False
    }
    portrait_ratios_found = 0
    landscape_ratios_found = 0
    try:
        with z.open(first_zip_path) as f:
            content_tree = etree.parse(f, etree.XMLParser(recover=True))
            xhtml_ns = 'http://www.w3.org/1999/xhtml'
            svg_ns = 'http://www.w3.org/2000/svg'
            xlink_ns = 'http://www.w3.org/1999/xlink'
            head_els = content_tree.findall(f'.//{{{xhtml_ns}}}head')
            if head_els:
                for head in head_els:
                    meta_els = head.findall(f'.//{{{xhtml_ns}}}meta')
                    for meta in meta_els:
                        name_attr = meta.get('name', '')
                        content_attr = meta.get('content', '')
                        if 'cover' in name_attr.lower() or content_attr.lower() == 'true':
                            indicators['has_meta_cover'] = True
                    title_els = head.findall(f'.//{{{xhtml_ns}}}title')
                    for title_el in title_els:
                        title_text = (title_el.text or '').lower()
                        if 'cover' in title_text or 'title' in title_text:
                            indicators['title_is_cover'] = True
                    style_els = head.findall(f'.//{{{xhtml_ns}}}style')
                    for style_el in style_els:
                        style_text = (style_el.text or '').lower()
                        if 'text-align' in style_text and 'center' in style_text:
                            indicators['css_text_align_center'] = True
                        if ('margin' in style_text and '0' in style_text) or ('padding' in style_text and '0' in style_text):
                            indicators['has_page_margin_zero'] = True
            svg_els = content_tree.findall(f'.//{{{xhtml_ns}}}svg')
            if not svg_els:
                svg_els = content_tree.findall(f'.//{{{svg_ns}}}svg')
            indicators['has_svg'] = len(svg_els) > 0
            for svg_el in svg_els:
                width = svg_el.get('width', '')
                height = svg_el.get('height', '')
                preserve = svg_el.get('preserveAspectRatio', '')
                viewbox = svg_el.get('viewBox', '')
                if width == '100%' and height == '100%':
                    indicators['has_fullsize_svg'] = True
                if viewbox:
                    indicators['has_viewbox_svg'] = True
                    try:
                        parts = viewbox.split()
                        if len(parts) == 4:
                            vb_width = float(parts[2])
                            vb_height = float(parts[3])
                            if vb_height > vb_width:
                                portrait_ratios_found += 1
                            elif vb_width > vb_height:
                                landscape_ratios_found += 1
                            if cover_width and cover_height:
                                svg_ratio = vb_width / vb_height if vb_height > 0 else 0
                                cover_ratio = cover_width / cover_height if cover_height > 0 else 0
                                if svg_ratio > 0 and cover_ratio > 0:
                                    ratio_diff = abs(svg_ratio - cover_ratio) / cover_ratio
                                    if ratio_diff > 0.05:
                                        indicators['svg_aspect_mismatch'] = True
                    except (ValueError, IndexError, ZeroDivisionError):
                        pass
                svg_images = svg_el.findall(f'.//{{{svg_ns}}}image')
                if len(svg_images) == 1:
                    indicators['has_single_svg_image'] = True
            all_els = content_tree.findall(f'.//*')
            for el in all_els:
                class_attr = el.get('class', '')
                id_attr = el.get('id', '')
                style_attr = el.get('style', '')
                if 'cover' in class_attr.lower():
                    indicators['has_cover_class'] = True
                    if 'ebookmaker' in class_attr.lower() or 'x-ebookmaker' in class_attr.lower():
                        indicators['has_ebookmaker_cover_class'] = True
                if 'cover' in id_attr.lower():
                    indicators['has_cover_id'] = True
                if 'text-align' in style_attr and 'center' in style_attr:
                    indicators['has_center_align'] = True
                if ('margin' in style_attr and '0' in style_attr) or ('padding' in style_attr and '0' in style_attr):
                    indicators['has_page_margin_zero'] = True
            text_nodes = [t.strip() for t in content_tree.itertext() if t.strip()]
            full_text = ' '.join(text_nodes)
            indicators['text_length'] = len(full_text)
            indicators['has_minimal_text'] = len(full_text) < 100
            full_text_lower = full_text.lower()
            nav_words = ['next', 'previous', 'chapter', 'contents', 'table of contents', 'toc']
            has_nav = any(word in full_text_lower for word in nav_words)
            indicators['no_navigation_text'] = not has_nav
            if book_title and len(book_title) > 3:
                indicators['contains_title'] = book_title.lower() in full_text_lower
            img_els = content_tree.findall(f'.//{{{xhtml_ns}}}img')
            svg_img_els = content_tree.findall(f'.//{{{svg_ns}}}image')
            svg_img_els += content_tree.findall(f'.//{{{xhtml_ns}}}svg//{{{svg_ns}}}image')
            image_els = img_els + svg_img_els
            indicators['image_count'] = len(image_els)
            indicators['has_single_image'] = len(image_els) == 1
            body_els = content_tree.findall(f'.//{{{xhtml_ns}}}body')
            if not body_els:
                body_els = [content_tree.getroot()]
            for body in body_els:
                body_children = list(body)
                if len(body_children) == 1:
                    child = body_children[0]
                    if child.tag.endswith('svg'):
                        indicators['body_direct_svg'] = True
                    if child.tag.endswith('div') or child.tag.endswith('svg'):
                        grandchildren = list(child)
                        if len(grandchildren) == 1:
                            if grandchildren[0].tag.endswith('img') or grandchildren[0].tag.endswith('svg'):
                                indicators['has_body_image'] = True
                            if grandchildren[0].tag.endswith('svg'):
                                indicators['body_direct_svg'] = True
                if len(body_children) <= 2:
                    simple_structure = True
                    for child in body_children:
                        child_children = list(child)
                        if len(child_children) > 2:
                            simple_structure = False
                            break
                        for grandchild in child_children:
                            if len(list(grandchild)) > 1:
                                simple_structure = False
                                break
                    if simple_structure:
                        indicators['has_minimal_structure'] = True
            for el in image_els:
                src = el.get('src') or el.get(f'{{{xlink_ns}}}href')
                if src:
                    src_lower = src.lower()
                    if 'cover' in src_lower:
                        indicators['has_cover_image_name'] = True
                    if 'title' in src_lower:
                        indicators['has_title_image_name'] = True
                width = el.get('width', '')
                height = el.get('height', '')
                if width and height:
                    try:
                        w_val = float(width.rstrip('px%'))
                        h_val = float(height.rstrip('px%'))
                        if h_val > w_val:
                            portrait_ratios_found += 1
                        elif w_val > h_val:
                            landscape_ratios_found += 1
                    except (ValueError, TypeError):
                        pass
            if portrait_ratios_found > landscape_ratios_found and portrait_ratios_found > 0:
                indicators['image_aspect_ratio_portrait'] = True
    except Exception:
        pass
    return indicators

def classify_titlepage(basename_lower, indicators):
    reasons = []
    if 'titl' in basename_lower or 'cover' in basename_lower or 'wrap' in basename_lower:
        reasons.append('filename')
    if indicators['has_ebookmaker_cover_class']:
        reasons.append('ebookmaker_class')
    elif indicators['has_cover_class']:
        reasons.append('cover_class')
    if indicators['has_cover_id']:
        reasons.append('cover_id')
    if indicators['has_cover_image_name']:
        reasons.append('cover_img')
    if indicators['has_title_image_name']:
        reasons.append('title_img')
    if indicators['has_meta_cover']:
        reasons.append('meta_cover')
    if indicators['title_is_cover']:
        reasons.append('title_is_cover')
    if indicators['has_fullsize_svg']:
        reasons.append('fullsize_svg')
    if indicators['body_direct_svg']:
        reasons.append('body_direct_svg')
    if indicators['image_aspect_ratio_portrait']:
        reasons.append('portrait_ratio')
    if indicators['has_single_image'] and indicators['has_minimal_text']:
        reasons.append('single_img_minimal_text')
    elif indicators['has_single_image']:
        reasons.append('single_img')
    if indicators['has_body_image'] and not indicators['body_direct_svg']:
        reasons.append('body_img_structure')
    if indicators['has_center_align'] and indicators['image_count'] > 0:
        reasons.append('centered_img')
    if indicators['contains_title'] and indicators['text_length'] < 200:
        reasons.append('title_text_short')
    if indicators['text_length'] < 50 and indicators['image_count'] > 0:
        reasons.append('very_minimal_text')
    if indicators['has_page_margin_zero']:
        reasons.append('margin_zero')
    if indicators['css_text_align_center']:
        reasons.append('css_center')
    if indicators['has_minimal_structure']:
        reasons.append('minimal_structure')
    if indicators['no_navigation_text'] and indicators['text_length'] < 300:
        reasons.append('no_nav')
    if indicators['svg_aspect_mismatch']:
        reasons.append('svg_aspect_mismatch')
    return reasons

def main(epub_folder):
    p = Path(epub_folder).expanduser().resolve()
    if not p.is_dir():
        print(f"Folder not found: {p}")
        return
    epub_paths = sorted(p.rglob('*.epub'))
    if not epub_paths:
        print("No EPUB files found")
        return
    for epub_path in epub_paths:
        try:
            with zipfile.ZipFile(epub_path, 'r') as z:
                opf_path = find_opf_path(z)
                if opf_path is None:
                    print(f'{epub_path.name.removesuffix(".epub")[:30]:<30} SKIP: no OPF found')
                    continue
                manifest, opf_dir, root, ns = parse_opf(z, opf_path)
                first_zip_path, first_href = find_first_content_path(z, manifest, opf_dir, root, ns)
                if first_zip_path is None:
                    print(f'{epub_path.name.removesuffix(".epub")[:30]:<30} SKIP: no readable spine item')
                    continue
                basename = Path(first_href).name
                lower_basename = basename.lower()
                book_title = root.xpath('.//dc:title/text()', namespaces={'dc': 'http://purl.org/dc/elements/1.1/'})
                book_title = book_title[0].strip() if book_title else ""
                cover_zip_path, _ = find_cover_path(z, manifest, opf_dir, root, ns)
                cover_width, cover_height = None, None
                if cover_zip_path:
                    cover_width, cover_height = get_image_dimensions(z, cover_zip_path)
                indicators = analyze_content(z, first_zip_path, book_title, cover_width, cover_height)
                reasons = classify_titlepage(lower_basename, indicators)
                if problems_only and reasons:
                    continue
                reason_str = ', '.join(reasons) if reasons else 'none'
                print(f'{epub_path.name.removesuffix(".epub")[:30]:<30} {reason_str}')
        except Exception as e:
            print(f'{epub_path.name.removesuffix(".epub")[:30]:<30} SKIP: processing error')

if __name__ == "__main__":
    try:
        default = last_folder_helper.get_last_folder()
        user_input = input(f'Input folder ({default}): ').strip()
        folder = user_input or default
        if not folder:
            folder = '.'
        last_folder_helper.save_last_folder(folder)
    except ImportError:
        if len(sys.argv) > 1:
            folder = sys.argv[1]
        else:
            folder = input('Input folder: ').strip() or '.'
    print()
    main(folder)
