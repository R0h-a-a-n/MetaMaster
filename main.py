import os
import time
from PIL import Image, ExifTags
import piexif
from multiprocessing import Pool
import functools
import hashlib

exif_cache = {}

def hash_key(file_path):
    return hashlib.md5(file_path.encode()).hexdigest()

def extract_metadata(file_path):
    try:
        cache_key = hash_key(file_path)
        if cache_key in exif_cache:
            return (file_path, exif_cache[cache_key])

        img = Image.open(file_path)
        exif_data = img._getexif()

        if exif_data:
            metadata = {}
            for tag_id, value in exif_data.items():
                tag = ExifTags.TAGS.get(tag_id, tag_id)
                metadata[tag] = value
            exif_cache[cache_key] = metadata
            return (file_path, metadata)
        else:
            return (file_path, None)
    except Exception as e:
        return (file_path, f"Error: {e}")

def modify_metadata(file_path, tag_name, new_value):
    try:
        img = Image.open(file_path)
        if 'exif' not in img.info:
            print(f"No EXIF data found in {file_path}. Cannot modify.")
            return

        exif_dict = piexif.load(img.info['exif'])
        exif_tags = {v: k for k, v in ExifTags.TAGS.items()}
        tag_id = exif_tags.get(tag_name)

        if tag_id is not None:
            if tag_id in piexif.ImageIFD.__dict__.values():
                ifd = "0th"
            elif tag_id in piexif.ExifIFD.__dict__.values():
                ifd = "Exif"
            elif tag_id in piexif.GPSIFD.__dict__.values():
                ifd = "GPS"
            elif tag_id in piexif.InteropIFD.__dict__.values():
                ifd = "Interop"
            elif tag_id in piexif.FirstIFD.__dict__.values():
                ifd = "1st"
            else:
                ifd = "0th"

            if isinstance(new_value, str):
                exif_dict[ifd][tag_id] = new_value.encode('utf-8')
            elif isinstance(new_value, int):
                exif_dict[ifd][tag_id] = new_value
            else:
                exif_dict[ifd][tag_id] = new_value

            exif_bytes = piexif.dump(exif_dict)
            img.save(file_path, exif=exif_bytes)
            print(f"Modified {tag_name} to '{new_value}' in {file_path}")
        else:
            print(f"Tag '{tag_name}' not found in EXIF metadata of {file_path}")
    except Exception as e:
        print(f"Error modifying {file_path}: {e}")

def delete_metadata(file_path):
    try:
        img = Image.open(file_path)
        img.save(file_path, exif=b'')
        print(f"Deleted EXIF metadata from {file_path}")
    except Exception as e:
        print(f"Error deleting metadata from {file_path}: {e}")

def process_images(file_list, operation, tag_name=None, new_value=None):
    if operation == 'extract':
        results = []
        for file_path in file_list:
            result = extract_metadata(file_path)
            results.append(result)
        return results
    elif operation == 'delete':
        for file_path in file_list:
            delete_metadata(file_path)
    elif operation == 'modify':
        if tag_name and new_value:
            for file_path in file_list:
                modify_metadata(file_path, tag_name, new_value)
        else:
            print("Missing tag name or value for modification operation.")
    else:
        print(f"Unknown operation: {operation}")

def batch_process_images(folder_path, operation, tag_name=None, new_value=None, batch_size=10):
    start_time = time.time()
    files = [f for f in os.listdir(folder_path) if f.lower().endswith(('.jpg', '.jpeg', '.png'))]
    total_files = len(files)
    if total_files == 0:
        print("No images found in the specified folder.")
        return

    print(f"Found {total_files} image(s) in '{folder_path}'. Starting '{operation}' operation...")

    for i in range(0, total_files, batch_size):
        batch = files[i:i + batch_size]
        batch_files = [os.path.join(folder_path, f) for f in batch]

        if operation == 'extract':
            with Pool() as pool:
                results = pool.map(extract_metadata, batch_files)
            for file_path, metadata in results:
                if isinstance(metadata, dict):
                    print(f"\nMetadata for {os.path.basename(file_path)}:")
                    for key, value in metadata.items():
                        print(f"  {key}: {value}")
                elif metadata is None:
                    print(f"No EXIF metadata found in {os.path.basename(file_path)}.")
                else:
                    print(f"Error processing {os.path.basename(file_path)}: {metadata}")
        elif operation == 'delete':
            with Pool() as pool:
                pool.map(delete_metadata, batch_files)
        elif operation == 'modify':
            if tag_name and new_value:
                with Pool() as pool:
                    func = functools.partial(modify_metadata, tag_name=tag_name, new_value=new_value)
                    pool.map(func, batch_files)
            else:
                print("Missing tag name or value for modification operation.")
        else:
            print(f"Unknown operation: {operation}")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"'{operation}' operation completed in {elapsed_time:.2f} seconds.")

def get_folder_from_user():
    folder_path = input("Enter the folder path containing images: ").strip('"').strip("'")
    
    if os.path.isdir(folder_path):
        return folder_path
    else:
        print(f"The folder '{folder_path}' does not exist.")
        return None

def main():
    folder_path = get_folder_from_user()
    if not folder_path:
        return

    operation = input("Enter operation (extract, modify, delete): ").strip().lower()
    
    if operation == 'modify':
        tag_name = input("Enter the EXIF tag name to modify (e.g., 'Artist'): ").strip()
        new_value = input(f"Enter the new value for '{tag_name}': ").strip()
        batch_process_images(folder_path, operation, tag_name, new_value)
    else:
        batch_process_images(folder_path, operation)

if __name__ == "__main__":
    main()
