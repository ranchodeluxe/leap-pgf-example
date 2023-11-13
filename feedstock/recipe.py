import apache_beam as beam
import os
import s3fs
import functools
import pangeo_forge_recipes
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
)
from pangeo_forge_recipes.storage import (
    FSSpecTarget,
    CacheFSSpecTarget,
)
from pangeo_forge_recipes.patterns import FilePattern, ConcatDim, MergeDim
from pangeo_forge_recipes.transforms import Indexed, T


# Github url to meta.yml:
meta_yaml_url = (
    "https://github.com/carbonplan/leap-pgf-example/blob/main/feedstock/meta.yaml"
)

# Filename Pattern Inputs
target_chunks = {"time": 40}

# Time Range
years = list(range(1971, 1972))  # 2020

# Variable List
variables = ["precip", "tmax"]  # "tmin", "vapourpres_h09", "vapourpres_h15"


def make_filename(variable, time):
    if variable == "precip":
        fpath = f"https://dapds00.nci.org.au/thredds/fileServer/zv2/agcd/v1/{variable}/total/r005/01day/agcd_v1_{variable}_total_r005_daily_{time}.nc"  # noqa: E501
    else:
        fpath = f"https://dapds00.nci.org.au/thredds/fileServer/zv2/agcd/v1/{variable}/mean/r005/01day/agcd_v1_{variable}_mean_r005_daily_{time}.nc"  # noqa: E501
    return fpath


pattern = FilePattern(
    make_filename,
    ConcatDim(name="time", keys=years),
    MergeDim(name="variable", keys=variables),
)


def my_get_injection_specs():
    return {
        "StoreToZarr": {
            "target_root": "TARGET_STORAGE",
        },
        "DropVars": {
            "target_root": "TARGET_STORAGE",
        },
        "WriteCombinedReference": {
            "target_root": "TARGET_STORAGE",
        },
        "OpenURLWithFSSpec": {"cache": "INPUT_CACHE_STORAGE"},
    }

# monkey patch this sucker
pangeo_forge_recipes.injections.get_injection_specs =  my_get_injection_specs


class DropVars(beam.PTransform):

    def __init__(self, label=None, target_root=None):
        # type: (Optional[str]) -> None
        super().__init__()
	self.target_root = target_root
        import logging
        logger = logging.getLogger('pangeo_forge_recipes')
        logger.error("#######################################################################")
        logger.error(self.target_root)
        logger.error("#######################################################################")
        self.label = label  # type: ignore # https://github.com/python/mypy/issues/3004

    @staticmethod
    def _drop_vars(item: Indexed[T]) -> Indexed[T]:
        index, ds = item
        ds = ds.drop_vars(["crs", "lat_bnds", "lon_bnds", "time_bnds"])
        return index, ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._drop_vars)


AGCD = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(file_type=pattern.file_type)
    | DropVars()
    | StoreToZarr(
        store_name="AGCD.zarr",
        combine_dims=pattern.combine_dim_keys,
        target_chunks=target_chunks,
        attrs={"meta_yaml_url": meta_yaml_url},
    )
)
