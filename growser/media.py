from io import BytesIO
from PIL import Image

import numpy as np
from skimage.filters import sobel_h, sobel_v


def get_compressed_size(image: Image, quality) -> float:
    """Find the size of an image after JPEG compression.

    :param image: Image to compress.
    :param quality: JPEG compression quality (1-100).
    """
    with BytesIO() as fh:
        image.save(fh, 'JPEG', quality=quality)
        size = len(fh.getvalue())
    return size


def get_compression_ratio(image: Image) -> float:
    """Return the lossless compression ratio for an image."""
    lossless = get_compressed_size(image, 100)
    compressed = get_compressed_size(image, 75)
    return compressed / lossless


def get_spatial_information_score(image: Image) -> float:
    """Use Sobel filters to find the edge energy of an image.

    .. math::
        SI_r = \sqrt{S_v^2 + S_h^2}

        SI_{mean} = \frac{1}{P}\sum{SI_r,}

    Where :math:`SI_r` is the spatial energy for each pixel and :math:`P` the
    number of pixels.

    .. seealso:: http://vintage.winklerbros.net/Publications/qomex2013si.pdf
    """
    img = np.asarray(image)
    num_pixels = img.shape[0] * img.shape[1]
    energy = np.sum(np.sqrt(sobel_v(img) ** 2 + sobel_h(img) ** 2))
    return energy / num_pixels


def score_image(image: Image):
    """Return the JPEG compression ratio and spatial complexity of an image.

    Gray-scaling images prevents color from impacting compression performance.

    :param image: Pillow Image instance.
    """
    if image.mode != 'L':
        image = image.convert('L')
    cr = get_compression_ratio(image)
    si = get_spatial_information_score(image)
    return cr, si
