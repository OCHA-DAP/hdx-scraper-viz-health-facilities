import logging
from pandas import DataFrame

from scrapers.utilities.hdx_functions import (
    download_unzip_read_data,
    find_resource,
    update_csv_resource,
    upload_updated_resource,
)

logger = logging.getLogger()


class HealthFacilities:
    def __init__(self, configuration, downloader, subnational_jsons, temp_folder):
        self.downloader = downloader
        self.boundaries = {level: subnational_jsons[level].copy(deep=True) for level in subnational_jsons}
        self.temp_folder = temp_folder
        self.exceptions = {"dataset": configuration["health_facilities"].get("dataset_exceptions", {}),
                           "resource": configuration["health_facilities"].get("resource_exceptions", {})}

    def run(self, countries):
        for level in countries:
            self.boundaries[level]["Health_Facilities"] = None
            updated_countries = dict()
            for iso in countries[level]:
                logger.info(f"Processing health facilities for {iso}")

                dataset_name = self.exceptions["dataset"].get(iso, f"hotosm_{iso.lower()}_health_facilities")
                health_resource = find_resource(dataset_name, "shp", kw="point")
                if not health_resource:
                    continue

                health_shp_lyr = download_unzip_read_data(
                    health_resource[0], "shp", unzip=True, read=True, folder=self.temp_folder
                )
                if isinstance(health_shp_lyr, type(None)):
                    continue

                join_lyr = health_shp_lyr.sjoin(
                    self.boundaries[level].loc[self.boundaries[level]["alpha_3"] == iso]
                )
                join_lyr = DataFrame(join_lyr)
                join_lyr = join_lyr.groupby(f"ADM{level[-1]}_PCODE").size()
                for pcode in join_lyr.index:
                    hfs = join_lyr[pcode]
                    self.boundaries[level].loc[
                        self.boundaries[level][f"ADM{level[-1]}_PCODE"] == pcode, "Health_Facilities"
                    ] = hfs

                if level in updated_countries:
                    updated_countries[level].append(iso)
                else:
                    updated_countries[level] = [iso]

            self.boundaries[level].drop(columns="geometry", inplace=True)
            self.boundaries[level] = self.boundaries[level][
                self.boundaries[level]["alpha_3"].isin(updated_countries[level])
            ]
            self.boundaries[level].loc[
                self.boundaries[level]["Health_Facilities"].isna(), "Health_Facilities"
            ] = 0

    def update_hdx_resource(self, dataset):
        subnational_data, resource = update_csv_resource(
            dataset, self.boundaries, folder=self.temp_folder
        )
        upload_updated_resource(
            resource, "subnational_health_facilities.csv", self.temp_folder, subnational_data
        )
