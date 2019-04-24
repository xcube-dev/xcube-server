import os
import threading
from abc import abstractmethod, ABCMeta
from typing import Sequence, Callable, Any, Dict, Optional

import xarray as xr

from xcube_server.im import TileGrid, GeoExtent


# TODO (forman): issue #46: write unit level tests for concrete classes in here


class MultiLevelDataset(metaclass=ABCMeta):
    """
    A multi-level dataset of decreasing spatial resolutions (a multi-resolution pyramid).

    The pyramid level at index zero provides the original spatial dimensions.
    The size of the spatial dimensions in subsequent levels
    is computed by the formula ``size[index + 1] = (size[index] + 1) // 2``
    with ``size[index]`` being the maximum size of the spatial dimensions at level zero.

    Any dataset chunks are assumed to be the same in all levels. Usually, the number of chunks is one
    in one of the spatial dimensions of the highest level.
    """

    @property
    @abstractmethod
    def tile_grid(self) -> TileGrid:
        """
        :return: the tile grid.
        """

    @property
    def num_levels(self) -> int:
        """
        :return: the number of pyramid levels.
        """
        return self.tile_grid.num_levels

    @property
    def base_dataset(self) -> xr.Dataset:
        """
        :return: the base dataset for lowest level at index 0.
        """
        return self.get_dataset(0)

    @property
    def datasets(self) -> Sequence[xr.Dataset]:
        """
        Get datasets for all levels.

        Calling this method will trigger any lazy dataset instantiation.

        :return: the datasets for all levels.
        """
        return [self.get_dataset(index) for index in range(self.num_levels)]

    @abstractmethod
    def get_dataset(self, index: int) -> xr.Dataset:
        """
        :param index: the level index
        :return: the dataset for the level at *index*.
        """

    def close(self):
        """ Close all datasets. Default implementation does nothing. """


class LazyMultiLevelDataset(MultiLevelDataset, metaclass=ABCMeta):
    """
    A multi-level dataset where each level dataset is lazily retrieved, i.e. read or computed by the abstract method
    ``get_dataset_lazily(index, **kwargs)``.

    If no *tile_grid* is passed it will be retrieved lazily by using the ``get_tile_grid_lazily()`` method,
    which may be overridden.The default implementation computes a new tile grid based on the dataset at level zero.

    :param tile_grid: The tile grid. If None, a new tile grid will be computed based on the dataset at level zero.
    :param kwargs: Extra keyword arguments that will be passed to the ``get_dataset_lazily`` method.
    """

    def __init__(self, tile_grid: TileGrid = None, kwargs: Dict[str, Any] = None):
        self._tile_grid = tile_grid
        self._level_datasets = {}
        self._kwargs = kwargs
        self._lock = threading.RLock()

    @property
    def tile_grid(self) -> TileGrid:
        if self._tile_grid is None:
            with self._lock:
                self._tile_grid = self.get_tile_grid_lazily()
        return self._tile_grid

    def get_dataset(self, index: int) -> xr.Dataset:
        """
        Get or compute the dataset for the level at given *index*.

        :param index: the level index
        :return: the dataset for the level at *index*.
        """
        if index not in self._level_datasets:
            kwargs = self._kwargs if self._kwargs is not None else {}
            with self._lock:
                # noinspection PyTypeChecker
                self._level_datasets[index] = self.get_dataset_lazily(index, **kwargs)
        # noinspection PyTypeChecker
        return self._level_datasets[index]

    @abstractmethod
    def get_dataset_lazily(self, index: int, **kwargs) -> xr.Dataset:
        """
        Retrieve, i.e. read or compute, the dataset for the level at given *index*.

        :param index: the level index
        :param kwargs: Extra keyword arguments passed to constructor.
        :return: the dataset for the level at *index*.
        """

    def get_tile_grid_lazily(self):
        """
        Retrieve, i.e. read or compute, the tile grid used by the multi-level dataset.

        :return: the dataset for the level at *index*.
        """
        return _get_dataset_tile_grid(self.get_dataset(0))

    def close(self):
        with self._lock:
            for dataset in self._level_datasets.values():
                if dataset is not None:
                    dataset.close()


class StoredMultiLevelDataset(LazyMultiLevelDataset):
    """
    A stored multi-level dataset whose level datasets are lazily read from storage location.

    :param num_levels: The number of levels.
    :param zarr_kwargs: Keyword arguments accepted by the ``xarray.open_zarr()`` function.
    """

    def __init__(self, dir_path: str, zarr_kwargs: Dict[str, Any] = None):
        file_paths = os.listdir(dir_path)
        level_paths = {}
        num_levels = -1
        for filename in file_paths:
            file_path = os.path.join(dir_path, filename)
            basename, ext = os.path.splitext(filename)
            if basename.isdigit():
                index = int(basename)
                num_levels = max(num_levels, index + 1)
                if os.path.isfile(file_path) and ext == ".link":
                    level_paths[index] = (ext, file_path)
                elif os.path.isdir(file_path) and ext == ".zarr":
                    level_paths[index] = (ext, file_path)

        if num_levels != len(level_paths):
            raise ValueError(f"Inconsistent levels directory:"
                             f" expected {num_levels} but found {len(level_paths)} entries:"
                             f" {dir_path}")

        super().__init__(kwargs=zarr_kwargs)
        self._dir_path = dir_path
        self._level_paths = level_paths

    def get_dataset_lazily(self, index: int, **zarr_kwargs) -> xr.Dataset:
        """
        Read the dataset for the level at given *index*.

        :param index: the level index
        :return: the dataset for the level at *index*.
        """
        ext, file_path = self._level_paths[index]
        if ext == ".link":
            with open(file_path, "r") as fp:
                file_path = fp.read()
        return xr.open_zarr(file_path, **zarr_kwargs)


class BaseMultiLevelDataset(LazyMultiLevelDataset):
    """
    A multi-level dataset whose level datasets are a created by down-sampling a base dataset.

    :param base_dataset: The base dataset for the level at index zero.
    """

    def __init__(self, base_dataset: xr.Dataset, tile_grid: TileGrid = None):
        super().__init__(tile_grid=tile_grid)
        if base_dataset is None:
            raise ValueError("base_dataset must be given")
        self._base_dataset = base_dataset

    def get_dataset_lazily(self, index: int, **kwargs) -> xr.Dataset:
        """
        Compute the dataset at level *index*: If *index* is zero, return the base image passed to constructor,
        otherwise down-sample the dataset for the level at given *index*.

        :param index: the level index
        :param kwargs: currently unused
        :return: the dataset for the level at *index*.
        """
        if index == 0:
            level_dataset = self._base_dataset
        else:
            base_dataset = self._base_dataset
            step = 2 ** index
            data_vars = {}
            for var_name in base_dataset.data_vars:
                var = base_dataset[var_name]
                var = var[..., ::step, ::step]
                data_vars[var_name] = var
            level_dataset = xr.Dataset(data_vars, attrs=base_dataset.attrs)
        return level_dataset


class ComputedMultiLevelDataset(LazyMultiLevelDataset):
    """
    A multi-level dataset whose level datasets are a computed from the levels of a source multi-level dataset.

    :param source: The source multi-level dataset.
    :param tile_grid: The source multi-level dataset.
    """

    def __init__(self,
                 function: Callable[[Optional[xr.Dataset], int], xr.Dataset],
                 source: MultiLevelDataset = None,
                 tile_grid: TileGrid = None,
                 kwargs: Dict[str, Any] = None):
        if source is None and tile_grid is None:
            raise ValueError("either source or tile_grid must be given")
        super().__init__(tile_grid=tile_grid, kwargs=kwargs)
        self._source = source
        self._function = function

    def get_tile_grid_lazily(self) -> TileGrid:
        if self._source is not None:
            return self._source.tile_grid
        return super().get_tile_grid_lazily()

    def get_dataset_lazily(self, index: int, **kwargs) -> xr.Dataset:
        level_source = self._source.get_dataset(index) if self._source is not None else None
        return self._function(level_source, index, **kwargs)


def _get_dataset_tile_grid(dataset: xr.Dataset):
    geo_extent = GeoExtent.from_coord_arrays(dataset.lon.values, dataset.lat.values)
    width, height, tile_width, tile_height = _get_cube_spatial_sizes(dataset)
    try:
        tile_grid = TileGrid.create(width, height, tile_width, tile_height, geo_extent)
    except ValueError:
        tile_grid = TileGrid(1, 1, 1, width, height, geo_extent)
    return tile_grid


def _get_cube_spatial_sizes(dataset: xr.Dataset):
    first_var_name = None
    spatial_shape = None
    spatial_chunks = None
    for var_name in dataset.data_vars:
        var = dataset[var_name]

        if var.ndim < 2 or var.dims[-2:] != ("lat", "lon"):
            continue

        if first_var_name is None:
            first_var_name = var_name

        if spatial_shape is None:
            spatial_shape = var.shape[-2:]
        elif spatial_shape != var.shape[-2:]:
            raise ValueError(f"variables in dataset have different spatial shapes:"
                             f" variable {first_var_name!r} has {spatial_shape}"
                             f" while {var_name!r} has {var.shape}")

        if var.chunks is not None:
            if spatial_chunks is None:
                spatial_chunks = var.chunks[-2:]
            elif spatial_chunks != var.chunks[-2:]:
                raise ValueError(f"variables in dataset have different spatial chunks:"
                                 f" variable {first_var_name!r} has {spatial_chunks}"
                                 f" while {var_name!r} has {var.chunks}")

    if spatial_shape is None:
        raise ValueError("no variables with spatial dimensions found")

    width, height = spatial_shape[-1], spatial_shape[-2]
    tile_width, tile_height = None, None

    if spatial_chunks is not None:
        def to_int(v):
            return v if isinstance(v, int) else v[0]

        spatial_chunks = tuple(map(to_int, spatial_chunks))
        tile_width, tile_height = spatial_chunks[-1], spatial_chunks[-2]

    return width, height, tile_width, tile_height
