from io import BytesIO
from PIL import Image, ImageFilter
import os

import numpy as np


def get_compressed_size(image: Image, quality: int=95):
    """Compress an image and determine its file size using JPEG compression.

    JPEG compression provides a decent estimate of the complexity of an image.

    :param image: Image to return compressed size for.
    :param quality: JPEG compression quality (1-100).
    """
    with BytesIO() as fh:
        image.save(fh, 'JPEG', quality=quality)
        size = len(fh.getvalue())
    return size


def score_image(path: str):
    """Use JPEG compression ratio as a proxy for image complexity.

    Histogram can be used to filter image for color saturation and variation.

    :param path: Path to the JPEG image to score.
    """
    img = Image.open(path)
    blurred = img.convert('L').filter(ImageFilter.GaussianBlur(3.5))
    size1 = get_compressed_size(img, 95)
    size2 = get_compressed_size(blurred, 95)
    hist = np.histogram(np.asarray(img), bins=6)[0]
    return (size1 / size2), hist


def score_images(path, pattern: str=None):
    """Return a score & histogram for all images in a path."""
    rv = []
    for filename in os.listdir(path):
        if not('png' in filename or 'jpg' in filename):
            continue
        if pattern and pattern not in filename:
            continue
        filename = os.path.join(path, filename)
        rv.append((filename, *score_image(filename)))
    return rv

