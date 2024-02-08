# This recipe can be run with `pangeo-forge-runner` with the CLI command:
# pangeo-forge-runner bake --repo=~/Documents/carbonplan/leap-pgf-example/ -f ~/Documents/carbonplan/leap-pgf-example/feedstock/config.json --Bake.recipe_id=AGCD --Bake.job_name=agcd

import apache_beam as beam
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
    StoreToPyramid,
)
from pangeo_forge_recipes.patterns import FilePattern, ConcatDim, MergeDim
from pangeo_forge_recipes.transforms import Indexed, T


# -----------------------------------------------------------------------


# Time Range
years = list(range(1971, 1973))  # 2020

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

pattern = pattern.prune()


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


pyramid_test_recipe = (
    process = (beam.Create(pattern.items()) 
        | OpenURLWithFSSpec()
        | OpenWithXarray(file_type=pattern.file_type)
        | DropVars())

    zarr_store = process | "Write Base Level" >> StoreToZarr(
        store_name="store", combine_dims=pattern.combine_dim_keys
    )
    pyramid_store = process | "Write Pyramid Levels" >> StoreToPyramid(
        store_name="pyramid",
        n_levels=4,
        combine_dims=pattern.combine_dim_keys,
    )
    )

# recipe = (
#     beam.Create(pattern.items())
#     | OpenWithKerchunk(
#         remote_protocol=earthdata_protocol,
#         file_type=pattern.file_type,
#         # lat/lon are around 5k, this is the best option for forcing kerchunk to inline them
#         inline_threshold=6000,
#         storage_options=auth_args,
#     )
#     | WriteCombinedReference(
#         concat_dims=CONCAT_DIMS,
#         identical_dims=IDENTICAL_DIMS,
#         store_name=SHORT_NAME,
#         # for running without a runner, use this target_root
#         # target_root=fs_target,
#         # mzz_kwargs={'coo_map': {"time": "cf:time"}, 'inline_threshold': 0}
#     )
#     #| ValidateDatasetDimensions(expected_dims={'time': None, 'lat': (-90, 90), 'lon': (-180, 180)})
)