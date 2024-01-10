# This recipe can be run with `pangeo-forge-runner` with the CLI command:
# pangeo-forge-runner bake --repo=~/Documents/carbonplan/LEAP/leap-pgf-example/ -f ~/Documents/carbonplan/LEAP/ # noqa: E501
# leap-pgf-example/feedstock/config.json --Bake.recipe_id=AGCD --Bake.job_name=agcd # noqa: E501


import apache_beam as beam
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
)
from pangeo_forge_recipes.patterns import FilePattern, ConcatDim, MergeDim
from pangeo_forge_recipes.transforms import Indexed, T
from data_management_utils import RegisterDatasetToCatalog


# --------------- METADATA AND CATALOGING -------------------------------
# Github url to meta.yml:
meta_yaml_url = (
    "https://github.com/carbonplan/leap-pgf-example/blob/main/feedstock/meta.yaml"
)
dataset_id = "AGCD"
table_id = "carbonplan.leap.test_dataset_catalog"
# -----------------------------------------------------------------------


# Filename Pattern Inputs
target_chunks = {"time": 40}

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
    | "Log to carbonplan BQ Catalog Table"
    >> RegisterDatasetToCatalog(table_id=table_id, dataset_id=dataset_id)
)
