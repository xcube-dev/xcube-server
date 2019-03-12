from abc import abstractmethod, ABCMeta

import xarray as xr


class DatasetPyramid(metaclass=ABCMeta):
    """
    A multi-level dataset pyramid of decreasing spatial resolutions.

    The pyramid level at index zero provides the original spatial dimensions.
    The size of the spatial dimensions in subsequent levels
    is computed by the formula ``size[index + 1] = (size[index] + 1) // 2``
    with ``size[index]`` being the maximum size of the spatial dimensions at level zero.

    The chunking is assumed to be the same in all pyramid levels. Usually, the number of chunks is one
    in one of the spatial dimensions of the highest level.
    """

    @property
    @abstractmethod
    def num_levels(self) -> int:
        """
        :return: the number of pyramid levels.
        """

    @abstractmethod
    def get_level_dataset(self, index: int) -> xr.Dataset:
        """
        :param index: the level index
        :return: the dataset for the level at *index*.
        """

    # @classmethod
    # def read_from_fs(input_path) -> "DatasetPyramid":
    #     file_paths = os.listdir(dir_path)
    #     level_paths = {}
    #     num_levels = -1
    #     for filename in file_paths:
    #         file_path = os.path.join(dir_path, filename)
    #         basename, ext = os.path.splitext(filename)
    #         if basename.isdigit():
    #             index = int(basename)
    #             num_levels = max(num_levels, index + 1)
    #             if os.path.isfile(file_path) and ext == ".link":
    #                 level_paths[index] = (ext, file_path)
    #             elif os.path.isdir(file_path) and ext == ".zarr":
    #                 level_paths[index] = (ext, file_path)
    #
    #     if num_levels != len(level_paths):
    #         raise ValueError(f"Inconsistent pyramid directory:"
    #                          f" expected {num_levels} but found {len(level_paths)} entries:"
    #                          f" {dir_path}")
    #
    #     levels = []
    #     for index in range(num_levels):
    #         ext, file_path = level_paths[index]
    #         if ext == ".link":
    #             with open(file_path, "r") as fp:
    #                 link_file_path = fp.read()
    #             dataset = xr.open_zarr(link_file_path)
    #         else:
    #             dataset = xr.open_zarr(file_path)
    #         if progress_monitor is not None:
    #             progress_monitor(dataset, index, num_levels)
    #         levels.append(dataset)
    #     return levels
