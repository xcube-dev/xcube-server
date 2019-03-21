# The MIT License (MIT)
# Copyright (c) 2018 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import functools
import io
import time
import uuid
from abc import ABCMeta, abstractmethod
from typing import Tuple, Sequence, Union, Any, Callable, Optional

import matplotlib.cm as cm
import numpy as np
from PIL import Image

from .cmaps import ensure_cmaps_loaded
from .geoextent import GeoExtent
from .tilegrid import TileGrid
from .utils import downsample_ndarray, aggregate_ndarray_first
from ..cache import Cache

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

X = int
Y = int
Width = int
Height = int
Size2D = Tuple[Width, Height]
Rectangle2D = Tuple[X, Y, Width, Height]
Number = Union[int, float]
Tile = Any
TileQuad = Tuple[Tile, Tile, Tile, Tile]
TiledImageCollection = Sequence['TiledImage']
LevelTransformer = Callable[['TiledImage', 'TiledImage', int, Optional[Any]], 'TiledImage']
LevelMapper = Callable[['TiledImage', int, Optional[Any]], 'TiledImage']
TileAggregator = Callable[[Tile, Tile, Tile, Tile], Tile]
LevelImageIdFactory = Callable[[int], str]


class TiledImage(metaclass=ABCMeta):
    """
    The interface for tiled images.
    """

    @property
    @abstractmethod
    def id(self) -> str:
        """
        Return a unique image identifier.
        :return: A unique (string) object
        """

    @property
    @abstractmethod
    def format(self) -> str:
        """
        Return a format string such as 'PNG', 'JPG', 'RAW', etc, or None according to PIL.
        :return: A string indicating the image (file) format.
        """

    @property
    @abstractmethod
    def mode(self) -> str:
        """
        Return the image mode string such as 'RGBA', 'RGB', 'L', etc, or None according to PIL.
        See http://pillow.readthedocs.org/en/3.0.x/handbook/concepts.html#modes
        :return: A string indicating the image mode
        """

    @property
    @abstractmethod
    def size(self) -> Size2D:
        """
        :return: The size of the image as a (width, height) tuple
        """

    @property
    @abstractmethod
    def tile_size(self) -> Size2D:
        """
        :return: The size of the image as a (tile_width, tile_height) tuple
        """

    @property
    @abstractmethod
    def num_tiles(self) -> Size2D:
        """
        :return: The number of tiles as a (num_tiles_x, num_tiles_y) tuple
        """

    @abstractmethod
    def get_tile(self, tile_x, tile_y) -> Tile:
        """
        Get the tile at tile indices *tile_x*, *tile_y*.

        :param tile_x: the tile index in X direction
        :param tile_y: the tile index in Y direction
        :return: The image's tile data at tile_x, tile_y.
        """

    @abstractmethod
    def dispose(self) -> None:
        """
        Dispose resources allocated by this image.
        """


class AbstractTiledImage(TiledImage, metaclass=ABCMeta):
    """
    An abstract base class for tiled images.
    Derived classes must implement the get_tile(tile_x, tile_y) method.
    It is strongly advised to also override the dispose() method in order to release any allocated resources.

    :param size: the image size as (width, height)
    :param tile_size: tile size as (tile_width, tile_height)
    :param num_tiles: number of tiles as (num_tiles_x, num_tiles_y)
    :param mode: optional mode string
    :param format: optional format string
    :param image_id: optional unique image identifier
    """

    def __init__(self, size: Size2D, tile_size: Size2D, num_tiles: Size2D,
                 mode: str = None, format: str = None, image_id: str = None):
        self._width = size[0]
        self._height = size[1]
        self._tile_width = tile_size[0]
        self._tile_height = tile_size[1]
        self._num_tiles_x = num_tiles[0]
        self._num_tiles_y = num_tiles[1]
        self._id = image_id or str(uuid.uuid4())
        self._mode = mode
        self._format = format

    @property
    def id(self) -> str:
        return self._id

    @property
    def format(self) -> str:
        return self._format

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def size(self) -> Size2D:
        return self._width, self._height

    @property
    def tile_size(self) -> Size2D:
        return self._tile_width, self._tile_height

    @property
    def num_tiles(self) -> Size2D:
        return self._num_tiles_x, self._num_tiles_y

    def dispose(self) -> None:
        """
        Does nothing.
        """

    def get_tile_id(self, tile_x, tile_y):
        return '%s/%d/%d' % (self.id, tile_x, tile_y)


class OpImage(AbstractTiledImage, metaclass=ABCMeta):
    """
    An abstract base class for images that compute their tiles.
    Derived classes must implement the compute_tile(tile_x, tile_y, rectangle) method only.

    :param size: the image size as (width, height)
    :param tile_size: optional tile size as (tile_width, tile_height)
    :param num_tiles: optional number of tiles as (num_tiles_x, num_tiles_y)
    :param mode: optional mode string
    :param format: optional format string
    :param image_id: optional unique image identifier
    :param tile_cache: optional tile cache
    :param log_perf: whether to log runtime performance information
    """

    def __init__(self, size: Size2D, tile_size: Size2D, num_tiles: Size2D,
                 mode: str = None, format: str = None, image_id: str = None, tile_cache: Cache = None, log_perf=False):
        super().__init__(size, tile_size, num_tiles, mode=mode, format=format, image_id=image_id)
        self._tile_cache = tile_cache
        self._log_perf = log_perf

    @property
    def tile_cache(self) -> Cache:
        return self._tile_cache

    def get_tile(self, tile_x: int, tile_y: int) -> Tile:

        tile_cache = self._tile_cache
        log_perf_cm = self.log_perf_cm(tile_x, tile_y)

        if tile_cache is not None:
            tile_id = self.get_tile_id(tile_x, tile_y)
            with log_perf_cm("queried cache"):
                tile = tile_cache.get_value(tile_id)
            if tile is not None:
                if self._log_perf:
                    print(f"tile {tile_id!r}:", 'restored from cache')
                return tile

        tw, th = self.tile_size
        with log_perf_cm("compute tile"):
            tile = self.compute_tile(tile_x, tile_y, (tw * tile_x, th * tile_y, tw, th))

        if tile_cache is not None:
            with log_perf_cm("stored in cache"):
                # noinspection PyUnboundLocalVariable
                tile_cache.put_value(tile_id, tile)

        return tile

    @abstractmethod
    def compute_tile(self, tile_x: int, tile_y: int, rectangle: Rectangle2D) -> Tile:
        """
        Compute a tile at tile indices *tile_x*, *tile_y*.
        The tile's boundaries are provided in *rectangle* given in image pixel coordinates.

        :param tile_x: the tile index in X direction
        :param tile_y: the tile index in Y direction
        :param rectangle: tile rectangle is given in image pixel coordinates.
        :return: a new tile
        """

    def dispose(self) -> None:
        cache = self._tile_cache
        if cache:
            num_tiles_x, num_tiles_y = self.num_tiles
            for tile_y in range(num_tiles_y):
                for tile_x in range(num_tiles_x):
                    cache.remove_value(self.get_tile_id(tile_x, tile_y))

    def log_perf_cm(self, tile_x, tile_y):
        if self._log_perf:
            tile_id = self.get_tile_id(tile_x, tile_y)
            tile_tag = f"tile {tile_id!r}"
            return functools.partial(_LogPerfCm, tile_tag)
        return _LogPerfCmNoOp


class DecoratorImage(OpImage, metaclass=ABCMeta):
    """
    Abstract tiled image class allowing behavior to be added to a given tiled source image.
    The decorator image will have the same image layout as the source image.
    Derived classes must implement the compute_tile_from_source_tile() method only.

    :param source_image: the source image
    :param image_id: optional unique image identifier
    :param format: optional format string
    :param mode: optional mode string
    :param tile_cache: optional tile cache
    :param log_perf: whether to log runtime performance information
    """

    def __init__(self,
                 source_image: TiledImage,
                 image_id: str = None,
                 format: str = None,
                 mode: str = None,
                 tile_cache: Cache = None,
                 log_perf=False):
        super().__init__(source_image.size,
                         source_image.tile_size,
                         source_image.num_tiles,
                         mode=mode if mode else source_image.mode,
                         format=format if format else source_image.format,
                         image_id=image_id,
                         tile_cache=tile_cache,
                         log_perf=log_perf)
        self._source_image = source_image

    @property
    def source_image(self):
        return self._source_image

    def compute_tile(self, tile_x: int, tile_y: int, rectangle: Rectangle2D) -> Tile:
        source_tile = self._source_image.get_tile(tile_x, tile_y)
        target_tile = None
        if source_tile is not None:
            target_tile = self.compute_tile_from_source_tile(tile_x, tile_y, rectangle, source_tile)
        return target_tile

    @abstractmethod
    def compute_tile_from_source_tile(self,
                                      tile_x: int, tile_y: int,
                                      rectangle: Rectangle2D,
                                      source_tile: Tile) -> Tile:
        """
        Compute a tile from the given *source_tile*.

        :param tile_x: the tile index in X direction
        :param tile_y: the tile index in Y direction
        :param rectangle: tile rectangle is given in image pixel coordinates.
        :param source_tile: the source tile
        :return: a new tile computed from the source tile
        """


class TransformArrayImage(DecoratorImage):
    """
    Performs basic (numpy) array tile transformations. Currently available: force_masked, flip_y.
    Expects the source image to provide (numpy) arrays.

    :param source_image: the source image
    :param image_id: optional unique image identifier
    :param flip_y: weather to flip pixels in y-direction
    :param force_masked: weather to force creation of masked arrays
    :param no_data_value: optional no-data value for mask creation
    :param tile_cache: optional tile cache
    :param log_perf: whether to log runtime performance information
    """

    def __init__(self,
                 source_image: TiledImage,
                 image_id: str = None,
                 flip_y: bool = False,
                 force_masked: bool = True,
                 force_2d: bool = False,
                 no_data_value: Number = None,
                 valid_range: Tuple[Number] = None,
                 tile_cache: Cache = None,
                 log_perf=False):
        super().__init__(source_image, image_id=image_id, tile_cache=tile_cache, log_perf=log_perf)
        self._force_masked = force_masked
        self._force_2d = force_2d
        self._flip_y = flip_y
        self._no_data_value = no_data_value
        self._valid_range = valid_range

    @property
    def no_data_value(self) -> Optional[Number]:
        return self._no_data_value

    @property
    def valid_range(self) -> Optional[Tuple[Number]]:
        return self._valid_range

    def compute_tile(self, tile_x: int, tile_y: int, rectangle: Rectangle2D) -> Tile:
        if self._flip_y:
            num_tiles_y = self.num_tiles[1]
            tile_size_y = self.tile_size[1]
            tile_y = num_tiles_y - 1 - tile_y
            x, y, w, h = rectangle
            rectangle = x, tile_y * tile_size_y, w, h
        source_tile = self._source_image.get_tile(tile_x, tile_y)
        target_tile = None
        if source_tile is not None:
            # noinspection PyTypeChecker
            target_tile = self.compute_tile_from_source_tile(tile_x, tile_y, rectangle, source_tile)
        return target_tile

    def compute_tile_from_source_tile(self, tile_x: int, tile_y: int, rectangle: Rectangle2D, tile: Tile) -> Tile:
        if self._force_2d and tile.ndim > 2:
            # Create 2D subset using basic indexing
            # noinspection PyTypeChecker
            index = (tile.ndim - 2) * [0] + [slice(None), slice(None)]
            tile = tile[index]
        if self._flip_y:
            # Flip tile using fancy indexing
            tile = tile[..., ::-1, :]
        if self._force_masked and not np.ma.is_masked(tile):
            # if tile is not masked
            if self._no_data_value is not None:
                # and we have a fill value, return a masked tile
                tile = np.ma.masked_equal(tile, self._no_data_value)
            elif self._valid_range is not None:
                valid_min, valid_max = self._valid_range
                # and we have a valid min or max, return a masked tile
                if valid_min is not None:
                    tile = np.ma.masked_less(tile, valid_min)
                if valid_max is not None:
                    tile = np.ma.masked_greater(tile, valid_max)
            elif np.issubdtype(tile.dtype, np.floating) or np.issubdtype(tile.dtype, np.complexfloating):
                # and it is of float type, return a masked tile with a mask from invalids, i.e. NaN, -Inf, +Inf
                tile = np.ma.masked_invalid(tile)
        return tile


class ColorMappedRgbaImage(DecoratorImage):
    """
    Creates a color-mapped image from a source image that provide tiles as numpy-like image arrays.

    :param source_image: the source image
    :param image_id: optional unique image identifier
    :param no_data_value: optional no-data value for mask creation
    :param value_range: The display value range.
    :param cmap_name: A Matplotlib color map name
    :param num_colors: Number of colors
    :param no_data_value: No-data value
    :param encode: Whether to create tiles that are encoded image bytes according to *format*.
    :param format: Image format, e.g. "JPEG", "PNG"
    :param tile_cache: optional tile cache
    :param log_perf: whether to log runtime performance information
    """

    def __init__(self,
                 source_image: TiledImage,
                 image_id: str = None,
                 value_range: Tuple[float, float] = (0.0, 1.0),
                 cmap_name: str = None,
                 num_colors: int = 256,
                 no_data_value: Union[int, float] = None,
                 encode: bool = False,
                 format: str = None,
                 tile_cache=None,
                 log_perf=False):
        super().__init__(source_image, image_id=image_id, format=format, mode='RGBA',
                         tile_cache=tile_cache, log_perf=log_perf)
        self._value_range = value_range
        self._cmap_name = cmap_name if cmap_name else 'jet'
        ensure_cmaps_loaded()
        self._cmap = cm.get_cmap(self._cmap_name, num_colors)
        self._cmap.set_bad('k', 0)
        self._no_data_value = no_data_value
        self._encode = encode

    def compute_tile_from_source_tile(self,
                                      tile_x: int, tile_y: int,
                                      rectangle: Rectangle2D, source_tile: Tile) -> Tile:
        log_perf_cm = self.log_perf_cm(tile_x, tile_y)
        with log_perf_cm("clip"):
            value_min, value_max = self._value_range
            if not np.ma.is_masked(source_tile):
                if self._no_data_value is not None:
                    array = np.ma.masked_equal(source_tile, self._no_data_value)
                    array = array.clip(value_min, value_max, out=array)
                elif np.issubdtype(source_tile.dtype, np.floating):
                    array = np.ma.masked_invalid(source_tile)
                    array = array.clip(value_min, value_max, out=array)
                else:
                    array = source_tile.clip(value_min, value_max)
            else:
                array = source_tile.clip(value_min, value_max)

        with log_perf_cm("reshape"):
            old_shape = array.shape
            height = old_shape[-2]
            width = old_shape[-1]
            if width * height == array.size:
                array = np.reshape(array, (height, width))
            else:
                # noinspection PyTypeChecker
                index = [0] * (array.ndim - 2) + [slice(None), slice(None)]
                array = array[index]

        with log_perf_cm("cmap"):
            # check if we can optimize the following calls by using Numexpr
            # see https://github.com/pydata/numexpr/wiki/Numexpr-Users-Guide
            array -= value_min
            array *= 1.0 / (value_max - value_min)
            array = self._cmap(array, bytes=True)
            image = Image.fromarray(array, mode=self.mode)

        with log_perf_cm("save PNG"):
            if self._encode and self.format:
                # Saving a PNG file is slow: https://github.com/python-pillow/Pillow/issues/1211
                ostream = io.BytesIO()
                image.save(ostream, format=self.format, compress_level=1)
                encoded_image = ostream.getvalue()
                ostream.close()
                return encoded_image
            else:
                return image

    def create_pyramid(self, **kwargs) -> 'ImagePyramid':
        if self._encode:
            raise TypeError("can't create pyramid from encoded hi-res tiles")
        return ImagePyramid.create_from_image(self, create_pil_downsampling_image, **kwargs)


class DownsamplingImage(OpImage):
    """
    Abstract base class for images that downsample a tiled source image.
    Derived classes must implement the aggregate_and_stitch_source_tiles() method only.

    :param source_image: a tiled source image (type TiledImage) whose source tiles must be PIL Images
    :param image_id: optional, unique image identifier
    :param tile_cache: an optional tile cache of type Cache
    """

    def __init__(self,
                 source_image: TiledImage,
                 image_id: str = None,
                 tile_cache: Cache = None):
        w, h = source_image.size
        nx, ny = source_image.num_tiles
        super().__init__((w // 2, h // 2),
                         tile_size=source_image.tile_size,
                         num_tiles=(nx // 2, ny // 2),
                         image_id=image_id,
                         mode=source_image.mode,
                         format=source_image.format,
                         tile_cache=tile_cache)
        self._source_image = source_image

    @property
    def source_image(self) -> TiledImage:
        return self._source_image

    def compute_tile(self, tile_x: int, tile_y: int, rectangle: Rectangle2D) -> Tile:
        source_tile_x = 2 * tile_x
        source_tile_y = 2 * tile_y

        source_image = self._source_image
        source_tiles = (source_image.get_tile(source_tile_x, source_tile_y),
                        source_image.get_tile(source_tile_x, source_tile_y + 1),
                        source_image.get_tile(source_tile_x + 1, source_tile_y),
                        source_image.get_tile(source_tile_x + 1, source_tile_y + 1))

        target_width, target_height = self.tile_size
        target_width_h, target_height_h = target_width // 2, target_height // 2

        target_positions = ((0, 0),
                            (0, target_height_h),
                            (target_width_h, 0),
                            (target_width_h, target_height_h))

        return self.aggregate_and_stitch_source_tiles(source_tiles,
                                                      (target_width, target_height),
                                                      target_positions)

    @abstractmethod
    def aggregate_and_stitch_source_tiles(self, source_tiles: TileQuad, target_size: Size2D, target_positions) -> Tile:
        pass


class PilDownsamplingImage(DownsamplingImage):
    """
    A tile image which downsamples a tiled source image whose tiles are PIL images (see http://pillow.readthedocs.org).

    :param source_image: a tiled source image (type TiledImage) whose source tiles must be PIL Images
    :param image_id: optional unique image identifier
    :param tile_cache: an optional tile cache
    :param resampling: the PIL Image resampling filter, default is PIL.Image.ANTIALIAS.
           See http://pillow.readthedocs.org/en/3.0.x/handbook/concepts.html#filters
           See http://pillow.readthedocs.org/en/3.0.x/reference/Image.html#PIL.Image.Image.resize
    """

    def __init__(self,
                 source_image: TiledImage,
                 image_id: str = None,
                 tile_cache: Cache = None,
                 resampling=Image.ANTIALIAS):
        super().__init__(source_image, image_id=image_id, tile_cache=tile_cache)
        self._resampling = resampling

    @property
    def resampling(self):
        return self._resampling

    def aggregate_and_stitch_source_tiles(self, source_tiles: TileQuad, target_size: Size2D, target_positions) -> Tile:
        target_shape = (target_size[0] // 2, target_size[1] // 2)
        agg_tiles = [source_tile.resize(target_shape, self._resampling) for source_tile in source_tiles]
        target_tile = Image.new(self._source_image.mode, target_size)
        for i in range(len(agg_tiles)):
            target_tile.paste(agg_tiles[i], target_positions[i])
        return target_tile


class NdarrayDownsamplingImage(DownsamplingImage):
    """
    A tiled image which downsamples a source image whose tiles are numpy ndarray-like arrays.

    :param source_image: a tiled source image (type TiledImage) whose source tiles must be PIL Images
    :param image_id: optional unique image identifier
    :param tile_cache: an optional tile cache
    :param aggregator: an aggregator function which will be called like so:
            aggregator(downsampled_tile_00, downsampled_tile_01, downsampled_tile_10, downsampled_tile_11).
            see utils.downsample_ndarray() function
    """

    def __init__(self,
                 source_image: TiledImage,
                 image_id: str = None,
                 tile_cache: Cache = None,
                 aggregator=aggregate_ndarray_first):
        super().__init__(source_image, image_id=image_id, tile_cache=tile_cache)
        self._aggregator = aggregator

    def aggregate_and_stitch_source_tiles(self, source_tiles: TileQuad, target_size: Size2D, target_positions) -> Tile:
        prototype_tile = source_tiles[0]
        agg_tiles = [downsample_ndarray(source_tile, aggregator=self._aggregator) for source_tile in source_tiles]
        target_shape = list(prototype_tile.shape)
        target_shape[-1] = target_size[0]
        target_shape[-2] = target_size[1]
        if np.ma.is_masked(prototype_tile):
            target_tile = np.ma.empty_like(prototype_tile)
        else:
            target_tile = np.empty_like(prototype_tile)
        for i in range(len(agg_tiles)):
            agg_x = target_positions[i][0]
            agg_y = target_positions[i][1]
            agg_tile = agg_tiles[i]
            agg_h, agg_w = agg_tile.shape[-2], agg_tile.shape[-1]
            # print('agg_tile h, w: ', agg_h, agg_w)
            target_tile[..., agg_y:agg_y + agg_h, agg_x:agg_x + agg_w] = agg_tile
        return target_tile


class FastNdarrayDownsamplingImage(OpImage):
    """
    A tiled image created from down-sampling a numpy ndarray-like array.

    :param array: a numpy ndarray-like array
    :param tile_size: the tile size
    :param step_exp: used to compute the step size / image resolution reduction factor: ``step_size = 2 ** step_exp``
    :param image_id: optional unique image identifier
    :param tile_cache: an optional tile cache
    """

    def __init__(self,
                 array,
                 tile_size: Size2D,
                 step_exp: int,
                 image_id: str = None,
                 tile_cache: Cache = None):
        step_size = 1 << step_exp
        source_width, source_height = array.shape[-1], array.shape[-2]
        width, height = source_width // step_size, source_height // step_size
        tile_width, tile_height = tile_size
        num_tiles = (width + tile_width - 1) // tile_width, (height + tile_height - 1) // tile_height
        super().__init__((width, height),
                         tile_size=tile_size,
                         num_tiles=num_tiles,
                         mode=str(array.dtype),
                         format=None,
                         image_id=image_id,
                         tile_cache=tile_cache)
        self._array = array
        self._step_size = step_size
        self._empty_tile = None

    def compute_tile(self, tile_x: int, tile_y: int, rectangle: Rectangle2D) -> Tile:
        x, y, w, h = rectangle
        s = self._step_size
        x *= s
        y *= s
        w *= s
        h *= s

        # Note by forman: check why this is 10x slower than without it
        #
        # num_tiles_x, num_tiles_y = self.num_tiles
        # if tile_x < 0 or tile_x > num_tiles_x - 1 or tile_y < 0 or tile_y > num_tiles_y - 1:
        #     print("Empty: ", tile_y, tile_x)
        #     if self._empty_tile is not None \
        #             and self._empty_tile.shape[-2] == h \
        #             and self._empty_tile.shape[-1] == w:
        #         return self._empty_tile
        #     shape = list(self._array.shape)
        #     shape[-2] = h
        #     shape[-1] = w
        #     tile = np.empty(shape, dtype=self._array.dtype)
        #     tile.fill(np.nan)
        #     self._empty_tile = tile
        #     return tile

        # For performance, we first read the non-resampled tile data.
        # We could use slices with 'zoom' as step size, but this is incredibly slow when using xarray with dask!
        # 0.4 vs. 0.025 secs for 220x220 pixel tiles for chunked, compressed SST data.
        # tile = self._array[..., y:y + h:s, x:x + w:s]
        tile = self._array[..., y:y + h, x:x + w]

        # Let's see if it has the xarray.DataArray.load() method.
        # Pre-loading of tile data makes it easier to find bottlenecks in the image processing chain.
        if hasattr(tile, 'load'):
            tile.load()

        # We do the resampling to lower resolution after loading the data, which is MUCH faster, see note above.
        tile = tile[..., ::s, ::s]

        # ensure that our tile size is w x h: resize and fill in background value.
        return trim_tile(tile, self.tile_size)


class NdarrayImage(OpImage):
    """
    A tiled image created an numpy ndarray-like data array.

    :param array: a numpy ndarray-like data array
    :param tile_size: the tile size
    :param image_id: optional unique image identifier
    :param tile_cache: an optional tile cache
    """

    def __init__(self,
                 array,
                 tile_size: Size2D,
                 image_id: str = None,
                 tile_cache: Cache = None,
                 log_perf=False):
        width, height = array.shape[-1], array.shape[-2]
        tile_width, tile_height = tile_size
        num_tiles = (width + tile_width - 1) // tile_width, (height + tile_height - 1) // tile_height
        super().__init__((width, height),
                         tile_size=tile_size,
                         num_tiles=num_tiles,
                         mode=str(array.dtype),
                         format=None,
                         image_id=image_id,
                         tile_cache=tile_cache,
                         log_perf=log_perf)
        self._array = array
        self._empty_tile = None

    def compute_tile(self, tile_x: int, tile_y: int, rectangle: Rectangle2D) -> Tile:
        x, y, w, h = rectangle
        tile = self._array[..., y:y + h, x:x + w]
        # ensure that our tile size is w x h
        return trim_tile(tile, self.tile_size)


LC_STANDARD_NAMES = {'land_cover_lccs algorithmic_confidence', 'land_cover_lccs status_flag', 'land_cover_lccs',
                     'land_cover_lccs number_of_observations', 'land_cover_lccs status_flag'}


class ImagePyramid:
    """
    A stack of tiled images (see TileImage) that form a quadtree image pyramid with increasing levels of detail.
    Level 0 represents the lowest resolution.
    The level of detail (image resolution) increases by a factor of two between any two, subsequent levels.
    The tile sizes for each level are the same.
    """

    @classmethod
    def create_from_image(cls,
                          source_image: TiledImage,
                          level_transformer: LevelTransformer,
                          geo_extent: GeoExtent = None,
                          **kwargs) -> 'ImagePyramid':

        """
        Create an image pyramid build from a single, max-resolution source image of type TiledImage.
        The given source image will be returned for highest resolution level in the pyramid.
        Other level images are created from the given level_image_factory function.

        :param source_image: the high-resolution source image, see TiledImage interface
        :param level_transformer: transforms level z+1 into level z. Called like:
               level_images[z_index] = level_transformer(source_image, level_images[z_index+1], z_index, **kwargs)
        :param geo_extent: the geographical extent.
        :param kwargs: keyword arguments passed to the level_image_factory function
        :return: a new ImagePyramid instance
        """
        if geo_extent is None:
            geo_extent = GeoExtent()
        tile_grid = TileGrid.create(source_image.size[0], source_image.size[1],
                                    source_image.tile_size[0], source_image.tile_size[1],
                                    geo_extent)
        level_images = [None] * tile_grid.num_levels
        z_index_max = tile_grid.num_levels - 1
        level_images[z_index_max] = source_image
        level_image = source_image
        for i in range(1, tile_grid.num_levels):
            z_index = z_index_max - i
            image_id = '%s/%d' % (source_image.id, z_index)
            level_images[z_index] = level_image = level_transformer(source_image, level_image, i,
                                                                    image_id=image_id, **kwargs)
        return ImagePyramid(tile_grid, level_images)

    @classmethod
    def create_from_array(cls,
                          array: np.ndarray,
                          tile_grid: TileGrid,
                          level_image_id_factory: LevelImageIdFactory = None,
                          **kwargs) -> 'ImagePyramid':

        """
        Create an image pyramid build from a numpy-like array using nearest neighbor resampling.
        This is a fast pyramid exploiting the array's underlying slicing capabilities.
        For example, if array is a H5Py dataset object, the created pyramid will take advantage of
        the HDF-5 libraries's slicing.

        :param array: numpy-like array that supports stepping in it's subscript operator, e.g.
                      array[..., y::step, x:step]
        :param tile_grid: the tile grid
        :param level_image_id_factory: a factory function for unique image identifiers
        :param kwargs: keyword arguments passed to FastNdarrayDownsamplingImage constructor
        :return: a new ImagePyramid instance
        """
        tile_size = tile_grid.tile_size
        num_levels = tile_grid.num_levels
        level_images = [None] * num_levels
        z_index_max = num_levels - 1
        for i in range(0, num_levels):
            z_index = z_index_max - i
            image_id = level_image_id_factory(z_index) if level_image_id_factory else None
            level_images[z_index] = FastNdarrayDownsamplingImage(array,
                                                                 tile_size,
                                                                 i,
                                                                 image_id=image_id, **kwargs)
        return ImagePyramid(tile_grid, level_images)

    def __init__(self,
                 tile_grid: TileGrid,
                 level_images: TiledImageCollection):
        if tile_grid.num_levels != len(level_images):
            raise ValueError('level_images do not match tile_grid')
        self._tile_grid = tile_grid
        self._level_images = list(level_images)

    @property
    def tile_grid(self) -> TileGrid:
        return self._tile_grid

    @property
    def num_level_zero_tiles(self) -> Size2D:
        return self._tile_grid.num_level_zero_tiles_x, self._tile_grid.num_level_zero_tiles_y

    @property
    def tile_size(self) -> Size2D:
        return self._tile_grid.tile_width, self._tile_grid.tile_height

    @property
    def num_levels(self) -> int:
        return self._tile_grid.num_levels

    def get_level_image(self, z_index: int) -> TiledImage:
        return self._level_images[z_index]

    def get_tile(self, tile_x: int, tile_y: int, z_index: int):
        level_image = self._level_images[z_index]
        return level_image.get_tile(tile_x, tile_y)

    def dispose(self) -> None:
        for level_image in self._level_images:
            level_image.dispose()

    def apply(self, level_mapper: LevelMapper, *args, **kwargs):
        level_images = self._level_images
        return ImagePyramid(self._tile_grid,
                            [level_mapper(level_images[level], level, *args, **kwargs)
                             for level in range(len(level_images))])


# noinspection PyUnusedLocal
def create_pil_downsampling_image(source_image: TiledImage,
                                  higher_level_image: TiledImage,
                                  step_exp: int,
                                  **kwargs) -> TiledImage:
    return PilDownsamplingImage(higher_level_image, **kwargs)


# noinspection PyUnusedLocal
def create_ndarray_downsampling_image(source_image: TiledImage,
                                      higher_level_image: TiledImage,
                                      step_exp: int,
                                      **kwargs) -> TiledImage:
    return NdarrayDownsamplingImage(higher_level_image, **kwargs)


def trim_tile(tile: Tile, expected_tile_size: Size2D, fill_value: float = np.nan) -> Tile:
    """
    Trim a tile.

    If to small, expand and pad with background value. If to large, crop.

    :param tile: The tile
    :param expected_tile_size: expected tile size
    :param fill_value: fill value for padding
    :return: the trimmed tile
    """
    expected_width, expected_height = expected_tile_size
    actual_width, actual_height = tile.shape[-1], tile.shape[-2]
    if expected_width > actual_width:
        # expand in width and pad with fill_value
        h_pad = np.empty((actual_height, expected_width - actual_width))
        h_pad.fill(fill_value)
        tile = np.hstack((tile, h_pad))
    if expected_height > actual_height:
        # expand in height and pad with fill_value
        v_pad = np.empty((expected_height - actual_height, expected_width))
        v_pad.fill(fill_value)
        tile = np.vstack((tile, v_pad))
    if expected_width < actual_width or expected_height < actual_height:
        # crop
        tile = tile[..., 0:expected_height, 0:expected_width]
    return tile


class _LogPerfCm:
    def __init__(self, tag: str, message: str):
        self.tag = tag
        self.message = message
        self.start_time = None
        self.delta = None

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.delta = time.perf_counter() - self.start_time
        print(f"{self.tag}: {self.message}:", "took %.2fms" % (self.delta * 1000.))


class _LogPerfCmNoOp:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None
