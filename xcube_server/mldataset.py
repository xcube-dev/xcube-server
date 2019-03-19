import threading
from abc import abstractmethod, ABCMeta
from typing import Dict, Union, Sequence

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

    Any dataset chunking is assumed to be the same in all levels. Usually, the number of chunks is one
    in one of the spatial dimensions of the highest level.
    """

    @property
    @abstractmethod
    def tile_grid(self) -> TileGrid:
        """
        :return: the tile grid.
        """

    @property
    @abstractmethod
    def num_levels(self) -> int:
        """
        :return: the number of pyramid levels.
        """

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
        """ Close all datasets. """


class SimpleMultiLevelDataset(MultiLevelDataset):
    """
    A multi-level dataset created from a sequence of datasets.

    :param level_datasets: A dataset for each level.
    """

    def __init__(self, level_datasets: Sequence[xr.Dataset]):
        # TODO (forman): issue #46: perform validation of levels_datasets here
        # 0. must be sequence
        # 1. all items must be instanceof(xr.Dataset)
        # 2. all items variables must have same dims + shapes + chunks
        # 3. for all items, i > 0: size[i] = (size[i - 1] + 1) // 2
        self._level_datasets = list(level_datasets)
        self._num_levels = len(self._level_datasets)
        self._tile_grid = _get_dataset_tile_grid(self.get_dataset(0))

    @property
    def num_levels(self) -> int:
        return self._num_levels

    @property
    def tile_grid(self) -> TileGrid:
        return self._tile_grid

    def get_dataset(self, index: int) -> xr.Dataset:
        return self._level_datasets[index]

    def close(self):
        for dataset in self._level_datasets:
            dataset.close()


class LazyMultiLevelDataset(MultiLevelDataset, metaclass=ABCMeta):
    """
    A multi-level dataset where each level is lazily retrieved, i.e. read or computed.

    :param num_levels: The number of levels.
    :param args: Extra arguments that will be passed to the ``retrieve_dataset`` method.
    :param kwargs: Extra keyword arguments that will be passed to the ``retrieve_dataset`` method.
    """

    def __init__(self, num_levels: int, *args, **kwargs):
        if num_levels < 1:
            raise ValueError("num_levels must be a positive integer")
        self._num_levels = num_levels
        self._level_datasets = [None] * num_levels
        self._args = args
        self._kwargs = kwargs
        self._lock = threading.RLock()

    @property
    def num_levels(self) -> int:
        """
        :return: the number of pyramid levels.
        """
        return self._num_levels

    def get_dataset(self, index: int) -> xr.Dataset:
        """
        Get or compute the dataset for the level at given *index*.

        :param index: the level index
        :return: the dataset for the level at *index*.
        """
        if self._level_datasets[index] is None:
            with self._lock:
                # noinspection PyTypeChecker
                self._level_datasets[index] = self.get_dataset_lazily(index, *self._args, **self._kwargs)
        # noinspection PyTypeChecker
        return self._level_datasets[index]

    @abstractmethod
    def get_dataset_lazily(self, index: int, *args, **kwargs) -> xr.Dataset:
        """
        Retrieve, i.e. read or compute, the dataset for the level at given *index*.

        :param index: the level index
        :param args: Extra arguments passed to constructor.
        :param kwargs: Extra keyword arguments passed to constructor.
        :return: the dataset for the level at *index*.
        """

    def close(self):
        for dataset in self._level_datasets:
            if dataset is not None:
                dataset.close()


class StoredMultiLevelDataset(LazyMultiLevelDataset):
    """
    A stored multi-level dataset whose level datasets are lazily read from storage location.

    :param num_levels: The number of levels.
    :param zarr_kwargs: Keyword arguments accepted by the ``xarray.open_zarr()`` function.
    """

    def __init__(self, dir_path: str, **zarr_kwargs):
        import os

        self._dir_path = dir_path
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

        super().__init__(num_levels, **zarr_kwargs)
        self._level_paths = level_paths
        self._tile_grid = _get_dataset_tile_grid(self.get_dataset(0))

    @property
    def tile_grid(self) -> TileGrid:
        return self._tile_grid

    def get_dataset_lazily(self, index: int, *args, **kwargs) -> xr.Dataset:
        """
        Read the dataset for the level at given *index*.

        :param index: the level index
        :return: the dataset for the level at *index*.
        """
        ext, file_path = self._level_paths[index]
        if ext == ".link":
            with open(file_path, "r") as fp:
                file_path = fp.read()
        return xr.open_zarr(file_path, **kwargs)


class BaseMultiLevelDataset(LazyMultiLevelDataset):
    """
    A multi-level dataset whose level datasets are a created by down-sampling a base dataset.

    :param base_dataset: The base dataset for the level at index zero.
    :param num_levels: The number of levels.
    :param chunks: The chunks for each dimension.
    """

    def __init__(self,
                 base_dataset: xr.Dataset,
                 num_levels: int = None,
                 chunks: Union[int, Dict[str, int]] = None):
        if base_dataset is None:
            raise ValueError("base_dataset must be given")

        tile_grid = _get_dataset_tile_grid(base_dataset)

        chunks = {}
        for var_name in base_dataset.data_vars:
            chunks.update({dim: 1 for dim in base_dataset[var_name].dims})
        chunks.update({"lon": tile_grid.tile_width, "lat": tile_grid.tile_height})

        self._tile_grid = tile_grid
        self._chunks = chunks

        super().__init__(tile_grid.num_levels)
        self._base_dataset = base_dataset

    @property
    def tile_grid(self) -> TileGrid:
        return self._tile_grid

    def get_dataset_lazily(self, index: int, *args, **kwargs) -> xr.Dataset:
        """
        Compute the dataset at level *index*: If *index* is zero, return the base image passed to constructor,
        otherwise down-sample the dataset for the level at given *index*.

        :param index: the level index
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
        # TODO (forman): PERF: check if following line can improve performance
        #                for tiling != chunking and tiling == chunking.
        #                So far, I couldn't see a performance benefit when uncommenting line.
        # level_dataset = level_dataset.chunk(chunks=self._chunks)
        return level_dataset


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


def _get_dataset_tile_grid(dataset: xr.Dataset):
    geo_extent = GeoExtent.from_coord_arrays(dataset.lon.values, dataset.lat.values)
    width, height, tile_width, tile_height = _get_cube_spatial_sizes(dataset)
    try:
        tile_grid = TileGrid.create(width, height, tile_width, tile_height, geo_extent)
    except ValueError:
        tile_grid = TileGrid(1, 1, 1, width, height, geo_extent)

    return tile_grid
