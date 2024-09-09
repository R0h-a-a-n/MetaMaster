from PIL import Image
from PIL.ExifTags import TAGS
image_path="C:/Users/USER/Downloads/Nikon_D70.jpg"
image=Image.open(image_path)

exif_data = image._getexif()

if exif_data:
    for tag_id, value in exif_data.items():
        tag= TAGS.get(tag_id, tag_id)
        print(f"{tag}: {value}")
else:
    print("No EXIF metadata found.")
