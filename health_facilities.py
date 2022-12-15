import logging
from geopandas import read_file
from glob import glob
from os.path import join
from pandas import concat, DataFrame, merge, read_csv
from zipfile import ZipFile, BadZipFile

from hdx.data.dataset import Dataset
from hdx.utilities.downloader import DownloadError
from hdx.utilities.uuid import get_uuid

logger = logging.getLogger()


class HealthFacilities:
    def __init__(self, configuration, subnational_json, temp_folder):
        self.boundaries = subnational_json
        self.temp_folder = temp_folder
        self.exceptions = {"dataset": configuration["inputs"].get("dataset_exceptions", {}),
                           "resource": configuration["inputs"].get("resource_exceptions", {})}

    def find_read_resource(self, iso, dataset_name):
        dataset = Dataset.read_from_hdx(dataset_name)
        if not dataset:
            logger.error(f"{iso}: Could not find dataset")
            return None
        health_resource = [r for r in dataset.get_resources() if r.get_file_type() == "shp" and
                           "points" in r["name"]]
        if len(health_resource) == 0:
            logger.error(f"{iso}: Could not find resource")
            return None

        try:
            _, resource_file = health_resource[0].download(folder=self.temp_folder)
        except DownloadError:
            logger.error(f"{iso}: Could not download resource")
            return None

        temp_dir = join(self.temp_folder, get_uuid())
        try:
            with ZipFile(resource_file, "r") as z:
                z.extractall(temp_dir)
        except BadZipFile:
            logger.error(f"{iso}: Could not unzip file")
            return None

        out_files = glob(join(temp_dir, "**", "*.shp"), recursive=True)
        if len(out_files) == 0:
            logger.error(f"{iso}: Did not find a shapefile in the zip")
            return None

        lyr = read_file(out_files[0])

        return lyr

    def summarize_data(self, countries):
        summarized_data = DataFrame()
        updated_countries = dict()
        for iso in countries:

            dataset_name = self.exceptions["dataset"].get(iso, f"hotosm_{iso.lower()}_health_facilities")
            health_shp_lyr = self.find_read_resource(iso, dataset_name)

            if isinstance(health_shp_lyr, type(None)):
                continue

            levels = list(set(self.boundaries["ADM_LEVEL"].loc[(self.boundaries["alpha_3"] == iso)]))
            for level in levels:
                if level not in updated_countries:
                    updated_countries[level] = list()
                logger.info(f"{iso}: Processing health facilities at adm{level}")

                join_lyr = health_shp_lyr.sjoin(
                    self.boundaries.loc[(self.boundaries["alpha_3"] == iso) &
                                        (self.boundaries["ADM_LEVEL"] == level)]
                )
                join_lyr = join_lyr.groupby("ADM_PCODE").size()
                join_lyr = join_lyr.to_frame(name="Health_Facilities").reset_index()
                summarized_data = concat([summarized_data, join_lyr])

                if iso not in updated_countries[level]:
                    updated_countries[level].append(iso)

            return summarized_data, updated_countries

    def update_hdx_resource(self, dataset_name, summarized_data, updated_countries):
        dataset = Dataset.read_from_hdx(dataset_name)
        if not dataset:
            logger.error("Could not find overall health facility dataset")
            return None, None

        resource = dataset.get_resources()[0]
        try:
            _, health_data = resource.download(folder=self.temp_folder)
        except DownloadError:
            logger.error(f"Could not download population csv")
            return None, None
        health_data = read_csv(health_data)

        updated_data = self.boundaries.drop(columns="geometry")
        updated_data = merge(updated_data, summarized_data, on="ADM_PCODE")
        updated_data.loc[updated_data["Health_Facilities"].isna()] = 0
        for level in updated_countries:
            health_data.drop(health_data[(health_data["alpha_3"].isin(updated_countries[level])) &
                                         (health_data["ADM_LEVEL"] == level)].index, inplace=True)
            health_data = concat([health_data,
                                  updated_data.loc[(updated_data["alpha_3"].isin(updated_countries[level])) &
                                                   (updated_data["ADM_LEVEL"] == level)]])

        health_data.sort_values(by=["alpha_3", "ADM_LEVEL", "ADM_PCODE"], inplace=True)
        return health_data, resource
