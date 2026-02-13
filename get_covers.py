import io
import zipfile
from pathlib import Path, PurePosixPath
from lxml import etree
from PIL import Image
import last_folder_helper
from complex_scan import find_opf_path

max_dimension = 1200
size_limit = 400
convert_to_jpg = False
quality = 90
min_dimension = 400
min_quality = 70
quality_step = 5
dimension_step = 0.95

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

def resize_image(img, max_dim):
    width, height = img.size
    if width <= max_dim and height <= max_dim:
        return img
    if width > height:
        new_width = max_dim
        new_height = int(height * max_dim / width)
    else:
        new_height = max_dim
        new_width = int(width * max_dim / height)
    return img.resize((new_width, new_height), Image.Resampling.LANCZOS)

def get_extension_from_path(path):
    return PurePosixPath(path).suffix.lower()

def save_resized_image(img, output_path, save_format, max_dimension, target_size_kb=size_limit):
    current_dimension = max_dimension
    current_quality = quality
    while current_dimension >= min_dimension and current_quality >= min_quality:
        resized_img = resize_image(img, current_dimension)
        buffer = io.BytesIO()
        if save_format == 'JPEG':
            if resized_img.mode not in ('RGB', 'L'):
                resized_img = resized_img.convert('RGB')
            resized_img.save(buffer, save_format, quality=current_quality, optimize=True)
        elif save_format == 'PNG':
            resized_img.save(buffer, save_format, optimize=True)
        elif save_format == 'GIF':
            resized_img.save(buffer, save_format)
        else:
            if resized_img.mode not in ('RGB', 'L'):
                resized_img = resized_img.convert('RGB')
            resized_img.save(buffer, save_format, quality=current_quality, optimize=True)
        size_kb = len(buffer.getvalue()) / 1024
        if size_kb <= target_size_kb:
            with open(output_path, 'wb') as f:
                f.write(buffer.getvalue())
            return True
        current_dimension = int(current_dimension * dimension_step)
        if save_format == 'JPEG':
            current_quality = max(min_quality, current_quality - quality_step)
    buffer = io.BytesIO()
    resized_img = resize_image(img, min_dimension)
    if save_format == 'JPEG':
        if resized_img.mode not in ('RGB', 'L'):
            resized_img = resized_img.convert('RGB')
        resized_img.save(buffer, save_format, quality=min_quality, optimize=True)
    elif save_format == 'PNG':
        resized_img.save(buffer, save_format, optimize=True)
    elif save_format == 'GIF':
        resized_img.save(buffer, save_format)
    else:
        if resized_img.mode not in ('RGB', 'L'):
            resized_img = resized_img.convert('RGB')
        resized_img.save(buffer, save_format, quality=min_quality, optimize=True)
    with open(output_path, 'wb') as f:
        f.write(buffer.getvalue())
    return True

def process_single_epub(epub_path, out_p, max_dimension, convert_to_jpg):
    try:
        with zipfile.ZipFile(epub_path) as z:
            opf_path = find_opf_path(z)
            if opf_path is None:
                return False
            manifest, opf_dir, root, ns = parse_opf(z, opf_path)
            cover_zip_path, _ = find_cover_path(z, manifest, opf_dir, root, ns)
            if cover_zip_path is None:
                return False
            with z.open(cover_zip_path) as cover_file:
                image_data = cover_file.read()
                img = Image.open(io.BytesIO(image_data))
                if convert_to_jpg:
                    output_filename = epub_path.stem + '.jpg'
                    output_path = out_p / output_filename
                    save_resized_image(img, output_path, 'JPEG', max_dimension)
                else:
                    original_ext = get_extension_from_path(cover_zip_path)
                    if not original_ext:
                        original_ext = '.jpg'
                    output_filename = epub_path.stem + original_ext
                    output_path = out_p / output_filename
                    if original_ext in ['.jpg', '.jpeg']:
                        save_format = 'JPEG'
                    elif original_ext == '.png':
                        save_format = 'PNG'
                    elif original_ext == '.gif':
                        save_format = 'GIF'
                    else:
                        save_format = 'JPEG'
                        output_filename = epub_path.stem + '.jpg'
                        output_path = out_p / output_filename
                    save_resized_image(img, output_path, save_format, max_dimension)
                print(f"Saved: {output_filename}")
                return True
    except Exception as e:
        return False

def main(folder, output_folder):
    p = Path(folder).expanduser().resolve()
    if not p.is_dir():
        print(f"Folder not found: {p}")
        return
    out_p = Path(output_folder).expanduser().resolve()
    out_p.mkdir(parents=True, exist_ok=True)
    epub_paths = sorted(p.rglob('*.epub'))
    if not epub_paths:
        print("No EPUB files found")
        return
    success_count = 0
    fail_count = 0
    for epub_path in epub_paths:
        if process_single_epub(epub_path, out_p, max_dimension, convert_to_jpg):
            success_count += 1
        else:
            fail_count += 1
    print(f"\nProcessed {success_count + fail_count} files: {success_count} succeeded, {fail_count} failed")

if __name__ == "__main__":
    print(f"Maximum dimension: {max_dimension}px. Change in file.")
    print(f"Convert to JPG: {convert_to_jpg}. Change in file.")
    default = last_folder_helper.get_last_folder()
    user_input = input(f'Input folder ({default}): ').strip().rstrip("/")
    folder = user_input or default
    if not folder:
        folder = '.'
    output_folder = str(Path(folder).resolve()) + '_covers'
    last_folder_helper.save_last_folder(folder)
    print()
    main(folder, output_folder)

