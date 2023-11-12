import apache_beam as beam
import os
import s3fs
import functools
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


class DropVars(beam.PTransform):
    """
    Custom Beam tranform to drop unused vars
    """

    @staticmethod
    def _drop_vars(item: Indexed[T]) -> Indexed[T]:
        index, ds = item
        ds = ds.drop_vars(["crs", "lat_bnds", "lon_bnds", "time_bnds"])
        return index, ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._drop_vars)


fss3 = s3fs.S3FileSystem(
    anon=False,
    key=os.environ["AWS_ACCESS_KEY"],
    secret=os.environ["AWS_SECRET_ACCESS_KEY"],
    client_kwargs={"region_name":"us-west-2"}
)
fs = FSSpecTarget(fs=fss3, root_path="s3://gcorradini-forge-runner-test/agcd/output")
cfs = CacheFSSpecTarget(fs=fss3, root_path="s3://gcorradini-forge-runner-test/agcd/cache")

#StoreToZarrWithTargetRoot = functools.partial(StoreToZarr, target_root=fs)

AGCD = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec(cache=cfs)
    | OpenWithXarray(file_type=pattern.file_type)
    | DropVars()
    | StoreToZarr(
        target_root=fs,
        store_name="AGCD.zarr",
        combine_dims=pattern.combine_dim_keys,
        target_chunks=target_chunks,
        attrs={"meta_yaml_url": meta_yaml_url},
    )
)
