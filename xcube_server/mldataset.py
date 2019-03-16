import threading
from abc import abstractmethod, ABCMeta
from typing import Dict, Union, Sequence

import xarray as xr


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

    @property
    def num_levels(self) -> int:
        """
        :return: the number of pyramid levels.
        """
        return self._num_levels

    def get_dataset(self, index: int) -> xr.Dataset:
        """
        :param index: the level index
        :return: the dataset for the level at *index*.
        """
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
            raise ValueError(f"Inconsistent pyramid directory:"
                             f" expected {num_levels} but found {len(level_paths)} entries:"
                             f" {dir_path}")

        super().__init__(num_levels, **zarr_kwargs)
        self._level_paths = level_paths

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

        var_names = list(base_dataset.data_vars)
        if not var_names:
            raise ValueError("base_dataset must contain at least one data variable")

        var = base_dataset[var_names[0]]

        if isinstance(chunks, int):
            chunks_dict = {dim: 1 for dim in var.dims}
            chunks_dict[var.dims[-2]] = chunks_dict[var.dims[-1]] = chunks
            chunks = chunks_dict
        elif isinstance(chunks, dict):
            chunks_dict = dict(chunks)
            for dim in var.dims:
                if dim not in chunks_dict:
                    chunks_dict[dim] = 1
            chunks = chunks_dict
        elif chunks is not None:
            raise TypeError("chunks must be an int, dict, or None")

        var_chunks = var.chunks
        if var_chunks is not None:
            # var.chunks is a tuple whose items are either a common block size or a tuple of varying block sizes
            var_chunks = {var.dims[i]: (var_chunks[i] if isinstance(var_chunks[i], int) else max(var_chunks[i]))
                          for i in range(var.ndim)}

        rechunk = chunks != var_chunks
        chunks = chunks or var_chunks

        if num_levels is None and chunks is None:
            raise ValueError("one of num_levels or chunks must be given")

        if num_levels is None:
            num_levels = 1
            height, width = var.shape[-2:]
            tile_height, tile_width = chunks[var.dims[-2]], chunks[var.dims[-1]]
            while width > tile_width and height > tile_height:
                width = (width + 1) // 2
                height = (height + 1) // 2
                num_levels += 1

        super().__init__(num_levels)
        self._base_dataset = base_dataset
        self._chunks = chunks
        self._rechunk = rechunk

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
        if self._rechunk:
            level_dataset = level_dataset.chunk(chunks=self._chunks)
        return level_dataset
