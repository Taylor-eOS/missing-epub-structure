import sys
from pathlib import Path
from zipfile import ZipFile
from lxml import etree
import last_folder_helper

def find_opf_path(z):
    try:
        with z.open('META-INF/container.xml') as f:
            parser = etree.XMLParser(recover=True)
            tree = etree.parse(f, parser)
            ns = '{urn:oasis:names:tc:opendocument:xmlns:container}'
            rootfile = tree.find(f'.//{ns}rootfile')
            if rootfile is not None:
                return rootfile.get('full-path')
    except Exception:
        pass
    for name in z.namelist():
        if name.lower().endswith('.opf'):
            return name
    return None

def get_package_version(z, opf_path):
    try:
        with z.open(opf_path) as f:
            parser = etree.XMLParser(recover=True)
            tree = etree.parse(f, parser)
            root = tree.getroot()
            tag_name = root.tag.split('}')[-1] if '}' in root.tag else root.tag
            if tag_name == 'package':
                return root.get('version')
    except Exception:
        pass
    return None

def classify_epub(path):
    try:
        with ZipFile(path, 'r') as z:
            opf_path = find_opf_path(z)
            if opf_path is None:
                return "weird (no OPF file found)"
            version = get_package_version(z, opf_path)
            if version:
                version = version.strip()
                if version.startswith("3."):
                    return f"EPUB 3 (version {version})"
                elif version in ("2.0", "2.0.1"):
                    return f"EPUB 2 (version {version})"
                else:
                    return f"weird (unusual version {version})"
            return "weird (missing version attribute or invalid package element)"
    except Exception:
        return "weird (cannot open ZIP or serious parsing error)"

def main(folder):
    p = Path(folder).expanduser().resolve()
    if not p.is_dir():
        print(f"Folder not found: {p}")
        sys.exit(1)
    epub_paths = list(p.rglob("*.epub"))
    if not epub_paths:
        print("No EPUB files found in the folder or its subfolders.")
        return
    epub_paths.sort(key=lambda x: x.name.lower())
    for epub in epub_paths:
        classification = classify_epub(str(epub))
        if not classification.startswith("EPUB 2"):
            print(f"{epub.stem}: {classification}")

if __name__ == "__main__":
    default = last_folder_helper.get_last_folder()
    user_input = input(f'Input folder ({default}): ').strip()
    folder = user_input or default
    if not folder:
        folder = '.'
    last_folder_helper.save_last_folder(folder)
    main(folder)

